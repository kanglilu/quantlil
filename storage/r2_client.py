from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

from config import Settings
from utils.retry import retry_async


class R2Store:
    def __init__(self, settings: Settings) -> None:
        if not settings.r2_enabled:
            raise ValueError("R2 credentials are incomplete")

        self.settings = settings
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint,
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            region_name=settings.r2_region,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    async def verify_bucket(self) -> dict[str, Any]:
        await self._call(
            lambda: self.client.head_bucket(Bucket=self.settings.r2_bucket),
            "r2_head_bucket",
        )
        return {
            "status": "ok",
            "bucket": self.settings.r2_bucket,
        }

    async def upload_bytes(
        self,
        object_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = await self._call(
            lambda: self.client.put_object(
                Bucket=self.settings.r2_bucket,
                Key=object_key,
                Body=data,
                ContentType=content_type,
                Metadata=metadata or {},
            ),
            "r2_put_object",
        )
        return {
            "object_key": object_key,
            "size_bytes": len(data),
            "etag": str(response.get("ETag", "")).strip('"'),
        }

    async def upload_parquet(
        self,
        object_key: str,
        frame: pd.DataFrame,
        *,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        buffer = io.BytesIO()
        frame.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            compression="zstd",
        )
        result = await self.upload_bytes(
            object_key,
            buffer.getvalue(),
            content_type="application/vnd.apache.parquet",
            metadata=metadata,
        )
        result["row_count"] = len(frame)
        return result

    async def head_object(self, object_key: str) -> dict[str, Any]:
        response = await self._call(
            lambda: self.client.head_object(
                Bucket=self.settings.r2_bucket,
                Key=object_key,
            ),
            "r2_head_object",
        )
        return {
            "object_key": object_key,
            "size_bytes": int(response["ContentLength"]),
            "content_type": response.get("ContentType"),
            "etag": str(response.get("ETag", "")).strip('"'),
            "metadata": response.get("Metadata", {}),
        }

    async def download_bytes(self, object_key: str) -> bytes:
        response = await self._call(
            lambda: self.client.get_object(
                Bucket=self.settings.r2_bucket,
                Key=object_key,
            ),
            "r2_get_object",
        )
        return await asyncio.to_thread(response["Body"].read)

    async def download_parquet(
        self, object_key: str
    ) -> pd.DataFrame | None:
        try:
            response = await asyncio.to_thread(
                self.client.get_object,
                Bucket=self.settings.r2_bucket,
                Key=object_key,
            )
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

        data = await asyncio.to_thread(response["Body"].read)
        return await asyncio.to_thread(
            pd.read_parquet,
            io.BytesIO(data),
        )

    async def list_parquet_objects(self, prefix: str) -> list[str]:
        def _list() -> list[str]:
            keys: list[str] = []
            continuation_token: str | None = None

            while True:
                params: dict[str, Any] = {
                    "Bucket": self.settings.r2_bucket,
                    "Prefix": prefix,
                }
                if continuation_token:
                    params["ContinuationToken"] = continuation_token

                response = self.client.list_objects_v2(**params)
                keys.extend(
                    str(item["Key"])
                    for item in response.get("Contents", [])
                    if str(item["Key"]).endswith(".parquet")
                )
                if not response.get("IsTruncated"):
                    break
                continuation_token = response.get("NextContinuationToken")
                if not continuation_token:
                    break

            return sorted(keys)

        return await self._call(_list, "r2_list_parquet_objects")

    async def delete_object(self, object_key: str) -> None:
        await self._call(
            lambda: self.client.delete_object(
                Bucket=self.settings.r2_bucket,
                Key=object_key,
            ),
            "r2_delete_object",
        )

    async def smoke_test(self) -> dict[str, Any]:
        timestamp = datetime.now(timezone.utc)
        object_key = (
            "_smoke/"
            f"quant-pipeline-{timestamp.strftime('%Y%m%dT%H%M%SZ')}.parquet"
        )
        frame = pd.DataFrame(
            [
                {
                    "timestamp": timestamp,
                    "symbol": "BTCUSDT",
                    "interval": "1m",
                    "close": 1.0,
                }
            ]
        )

        await self.verify_bucket()
        uploaded = await self.upload_parquet(
            object_key,
            frame,
            metadata={"purpose": "quant-pipeline-smoke-test"},
        )
        try:
            remote = await self.head_object(object_key)
            downloaded = await self.download_bytes(object_key)
            restored = pd.read_parquet(io.BytesIO(downloaded))
            if len(restored) != 1:
                raise RuntimeError("R2 smoke-test Parquet row count mismatch")
            return {
                "status": "ok",
                "bucket": self.settings.r2_bucket,
                "object_key": object_key,
                "size_bytes": remote["size_bytes"],
                "row_count": len(restored),
                "etag": uploaded["etag"],
            }
        finally:
            await self.delete_object(object_key)

    async def _call(self, operation: Any, operation_name: str) -> Any:
        return await retry_async(
            lambda: asyncio.to_thread(operation),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name=operation_name,
        )
