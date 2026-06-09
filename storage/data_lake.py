from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import pandas as pd

from storage.r2_client import R2Store
from storage.supabase_client import SupabaseStore


class DataLakeWriter:
    def __init__(self, r2: R2Store, catalog: SupabaseStore) -> None:
        self.r2 = r2
        self.catalog = catalog
        self.logger = logging.getLogger(self.__class__.__name__)
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(
            asyncio.Lock
        )

    async def archive_rows(
        self,
        *,
        namespace: str,
        dataset: str,
        rows: list[dict[str, Any]],
        unique_keys: tuple[str, ...],
        symbol: str | None = None,
        interval: str | None = None,
        partitions: dict[str, str] | None = None,
        partition_granularity: str = "day",
    ) -> dict[str, Any]:
        if not rows:
            return {"status": "skip", "files": 0, "rows": 0}

        frame = pd.DataFrame(rows)
        if "timestamp" not in frame.columns:
            raise ValueError(f"{dataset} rows require a timestamp column")

        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"], utc=True, errors="coerce"
        )
        frame = frame.dropna(subset=["timestamp"]).copy()
        if frame.empty:
            raise ValueError(f"{dataset} has no valid timestamps")

        frame["fetched_at"] = datetime.now(timezone.utc)
        frame["_partition_key"] = self._partition_key(
            frame["timestamp"],
            partition_granularity,
        )

        results = []
        for partition_key, partition_frame in frame.groupby(
            "_partition_key", sort=True
        ):
            partition_frame = partition_frame.drop(
                columns=["_partition_key"]
            )
            partition_date = pd.Timestamp(partition_key)
            object_key = self._object_key(
                namespace=namespace,
                dataset=dataset,
                date=pd.Timestamp(partition_date),
                symbol=symbol,
                interval=interval,
                partitions=partitions,
                partition_granularity=partition_granularity,
            )
            results.append(
                await self._merge_daily_file(
                    object_key=object_key,
                    dataset=dataset,
                    frame=partition_frame,
                    unique_keys=unique_keys,
                    symbol=symbol,
                    interval=interval,
                )
            )

        return {
            "status": "ok",
            "files": len(results),
            "rows": sum(item["row_count"] for item in results),
            "objects": results,
        }

    async def _merge_daily_file(
        self,
        *,
        object_key: str,
        dataset: str,
        frame: pd.DataFrame,
        unique_keys: tuple[str, ...],
        symbol: str | None,
        interval: str | None,
    ) -> dict[str, Any]:
        async with self._locks[object_key]:
            existing = await self.r2.download_parquet(object_key)
            merged = self._merge_frames(existing, frame, unique_keys)
            uploaded = await self.r2.upload_parquet(
                object_key,
                merged,
                metadata={
                    "dataset": dataset,
                    "format": "parquet",
                    "compression": "zstd",
                },
            )

            min_timestamp = pd.Timestamp(merged["timestamp"].min())
            max_timestamp = pd.Timestamp(merged["timestamp"].max())
            now = datetime.now(timezone.utc).isoformat()
            catalog_row = {
                "dataset": dataset,
                "symbol": symbol,
                "interval": interval,
                "object_key": object_key,
                "min_timestamp": min_timestamp.isoformat(),
                "max_timestamp": max_timestamp.isoformat(),
                "row_count": len(merged),
                "size_bytes": uploaded["size_bytes"],
                "etag": uploaded["etag"],
                "updated_at": now,
            }
            await self.catalog.upsert_r2_object(catalog_row)
            self.logger.info(
                "Archived %s rows to r2://%s/%s",
                len(merged),
                self.r2.settings.r2_bucket,
                object_key,
            )
            return catalog_row

    @staticmethod
    def _merge_frames(
        existing: pd.DataFrame | None,
        incoming: pd.DataFrame,
        unique_keys: tuple[str, ...],
    ) -> pd.DataFrame:
        frames = [incoming] if existing is None else [existing, incoming]
        merged = pd.concat(frames, ignore_index=True, sort=False)
        merged["timestamp"] = pd.to_datetime(
            merged["timestamp"], utc=True, errors="coerce"
        )
        merged["fetched_at"] = pd.to_datetime(
            merged["fetched_at"], utc=True, errors="coerce"
        )
        merged = merged.dropna(subset=["timestamp"])
        missing = [key for key in unique_keys if key not in merged.columns]
        if missing:
            raise ValueError(f"Missing data-lake unique keys: {missing}")
        return (
            merged.sort_values(["timestamp", "fetched_at"])
            .drop_duplicates(subset=list(unique_keys), keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    @staticmethod
    def _object_key(
        *,
        namespace: str,
        dataset: str,
        date: pd.Timestamp,
        symbol: str | None,
        interval: str | None,
        partitions: dict[str, str] | None,
        partition_granularity: str,
    ) -> str:
        parts = ["raw", quote(namespace, safe=""), quote(dataset, safe="")]
        if symbol:
            parts.append(f"symbol={quote(symbol, safe='')}")
        if interval:
            parts.append(f"interval={quote(interval, safe='')}")
        for key, value in sorted((partitions or {}).items()):
            parts.append(
                f"{quote(str(key), safe='')}={quote(str(value), safe='')}"
            )
        parts.append(f"year={date.year:04d}")
        if partition_granularity in {"month", "day"}:
            parts.append(f"month={date.month:02d}")
        if partition_granularity == "day":
            parts.append(f"day={date.day:02d}")
        parts.append("data.parquet")
        return "/".join(parts)

    @staticmethod
    def _partition_key(
        timestamps: pd.Series,
        granularity: str,
    ) -> pd.Series:
        if granularity == "day":
            return timestamps.dt.strftime("%Y-%m-%d")
        if granularity == "month":
            return timestamps.dt.strftime("%Y-%m-01")
        if granularity == "year":
            return timestamps.dt.strftime("%Y-01-01")
        raise ValueError(
            "partition_granularity must be day, month, or year"
        )
