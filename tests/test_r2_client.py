from __future__ import annotations

import asyncio
import io
import logging
from types import SimpleNamespace

import pandas as pd
from botocore.exceptions import ClientError

from storage.data_lake import DataLakeWriter
from storage.data_lake_reader import DataLakeReader
from storage.r2_client import R2Store


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object]] = {}

    def head_bucket(self, **kwargs):
        return {}

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = {
            "Body": kwargs["Body"],
            "ContentType": kwargs["ContentType"],
            "Metadata": kwargs["Metadata"],
        }
        return {"ETag": '"test-etag"'}

    def head_object(self, **kwargs):
        item = self.objects[kwargs["Key"]]
        return {
            "ContentLength": len(item["Body"]),
            "ContentType": item["ContentType"],
            "Metadata": item["Metadata"],
            "ETag": '"test-etag"',
        }

    def get_object(self, **kwargs):
        if kwargs["Key"] not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            )
        item = self.objects[kwargs["Key"]]
        return {"Body": io.BytesIO(item["Body"])}

    def delete_object(self, **kwargs):
        self.objects.pop(kwargs["Key"], None)
        return {}

    def list_objects_v2(self, **kwargs):
        prefix = kwargs.get("Prefix", "")
        contents = [
            {"Key": key}
            for key in sorted(self.objects)
            if key.startswith(prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}


def test_r2_smoke_test_uploads_reads_and_deletes_parquet() -> None:
    store = object.__new__(R2Store)
    store.settings = SimpleNamespace(
        r2_bucket="quant",
        retry_attempts=1,
        retry_base_delay=0,
    )
    store.logger = logging.getLogger("test-r2")
    store.client = FakeS3Client()

    result = asyncio.run(store.smoke_test())

    assert result["status"] == "ok"
    assert result["bucket"] == "quant"
    assert result["row_count"] == 1
    assert result["size_bytes"] > 0
    assert store.client.objects == {}


def test_upload_parquet_can_be_read_back() -> None:
    store = object.__new__(R2Store)
    store.settings = SimpleNamespace(
        r2_bucket="quant",
        retry_attempts=1,
        retry_base_delay=0,
    )
    store.logger = logging.getLogger("test-r2")
    store.client = FakeS3Client()
    frame = pd.DataFrame([{"timestamp": "2026-06-09", "close": 100.5}])

    result = asyncio.run(store.upload_parquet("data/test.parquet", frame))
    payload = store.client.objects["data/test.parquet"]["Body"]
    restored = pd.read_parquet(io.BytesIO(payload))

    assert result["row_count"] == 1
    assert restored.to_dict("records") == frame.to_dict("records")


def test_list_parquet_objects_filters_prefix_and_extension() -> None:
    store = object.__new__(R2Store)
    store.settings = SimpleNamespace(
        r2_bucket="quant",
        retry_attempts=1,
        retry_base_delay=0,
    )
    store.logger = logging.getLogger("test-r2")
    store.client = FakeS3Client()
    store.client.objects = {
        "raw/alternative/dxy/a.parquet": {},
        "raw/alternative/dxy/notes.txt": {},
        "raw/alternative/fear_greed/b.parquet": {},
    }

    keys = asyncio.run(
        store.list_parquet_objects("raw/alternative/dxy/")
    )

    assert keys == ["raw/alternative/dxy/a.parquet"]


class FakeCatalog:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}

    async def upsert_r2_object(self, row):
        self.rows[row["object_key"]] = row
        return 1

    async def fetch_r2_objects_since(
        self,
        since,
        *,
        dataset=None,
        interval=None,
        object_prefix=None,
    ):
        rows = list(self.rows.values())
        if dataset:
            rows = [row for row in rows if row["dataset"] == dataset]
        if interval:
            rows = [row for row in rows if row["interval"] == interval]
        if object_prefix:
            rows = [
                row
                for row in rows
                if row["object_key"].startswith(object_prefix)
            ]
        return rows


def test_data_lake_merges_daily_file_without_duplicates() -> None:
    r2 = object.__new__(R2Store)
    r2.settings = SimpleNamespace(
        r2_bucket="quant",
        retry_attempts=1,
        retry_base_delay=0,
    )
    r2.logger = logging.getLogger("test-r2")
    r2.client = FakeS3Client()
    catalog = FakeCatalog()
    lake = DataLakeWriter(r2, catalog)  # type: ignore[arg-type]

    first = [
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "timestamp": "2026-06-09T10:00:00+00:00",
            "close": "100",
        },
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "timestamp": "2026-06-09T10:01:00+00:00",
            "close": "101",
        },
    ]
    second = [
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "timestamp": "2026-06-09T10:01:00+00:00",
            "close": "102",
        },
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "timestamp": "2026-06-09T10:02:00+00:00",
            "close": "103",
        },
    ]

    asyncio.run(
        lake.archive_rows(
            namespace="market",
            dataset="btc_ohlcv",
            rows=first,
            unique_keys=("symbol", "interval", "timestamp"),
            symbol="BTCUSDT",
            interval="1m",
        )
    )
    result = asyncio.run(
        lake.archive_rows(
            namespace="market",
            dataset="btc_ohlcv",
            rows=second,
            unique_keys=("symbol", "interval", "timestamp"),
            symbol="BTCUSDT",
            interval="1m",
        )
    )

    object_key = (
        "raw/market/btc_ohlcv/symbol=BTCUSDT/interval=1m/"
        "year=2026/month=06/day=09/data.parquet"
    )
    restored = pd.read_parquet(
        io.BytesIO(r2.client.objects[object_key]["Body"])
    )
    assert result["rows"] == 3
    assert len(restored) == 3
    assert (
        restored.loc[
            restored["timestamp"]
            == pd.Timestamp("2026-06-09T10:01:00Z"),
            "close",
        ].iloc[0]
        == "102"
    )
    assert catalog.rows[object_key]["row_count"] == 3


def test_data_lake_normalizes_mixed_values_before_parquet_upload() -> None:
    r2 = object.__new__(R2Store)
    r2.settings = SimpleNamespace(
        r2_bucket="quant",
        retry_attempts=1,
        retry_base_delay=0,
    )
    r2.logger = logging.getLogger("test-r2")
    r2.client = FakeS3Client()
    catalog = FakeCatalog()
    lake = DataLakeWriter(r2, catalog)  # type: ignore[arg-type]
    object_key = (
        "raw/alternative/polymarket/metric=total_volume_24h/"
        "year=2026/month=06/day=09/data.parquet"
    )

    asyncio.run(
        lake.archive_rows(
            namespace="alternative",
            dataset="polymarket",
            rows=[
                {
                    "source": "polymarket",
                    "metric_name": "total_volume_24h",
                    "timestamp": "2026-06-09T12:00:00+00:00",
                    "value": 138677529.125854,
                    "label": None,
                }
            ],
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": "total_volume_24h"},
        )
    )
    asyncio.run(
        lake.archive_rows(
            namespace="alternative",
            dataset="polymarket",
            rows=[
                {
                    "source": "polymarket",
                    "metric_name": "total_volume_24h",
                    "timestamp": "2026-06-09T18:00:00+00:00",
                    "value": "155627413.3720149719851917",
                    "label": None,
                }
            ],
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": "total_volume_24h"},
        )
    )

    restored = pd.read_parquet(
        io.BytesIO(r2.client.objects[object_key]["Body"])
    )
    assert restored["value"].tolist() == [
        "138677529.125854",
        "155627413.3720149719851917",
    ]


def test_data_lake_reader_loads_btc_and_features() -> None:
    r2 = object.__new__(R2Store)
    r2.settings = SimpleNamespace(
        r2_bucket="quant",
        retry_attempts=1,
        retry_base_delay=0,
    )
    r2.logger = logging.getLogger("test-r2")
    r2.client = FakeS3Client()
    catalog = FakeCatalog()
    writer = DataLakeWriter(r2, catalog)  # type: ignore[arg-type]

    asyncio.run(
        writer.archive_rows(
            namespace="market",
            dataset="btc_ohlcv",
            rows=[
                {
                    "symbol": "BTCUSDT",
                    "interval": "15m",
                    "timestamp": "2026-06-09T10:00:00+00:00",
                    "close": "100",
                }
            ],
            unique_keys=("symbol", "interval", "timestamp"),
            symbol="BTCUSDT",
            interval="15m",
        )
    )
    asyncio.run(
        writer.archive_rows(
            namespace="alternative",
            dataset="fear_greed",
            rows=[
                {
                    "source": "fear_greed",
                    "metric_name": "index",
                    "timestamp": "2026-06-09T00:00:00+00:00",
                    "value": "10",
                    "label": "Extreme Fear",
                }
            ],
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": "index"},
        )
    )

    reader = DataLakeReader(r2, catalog)  # type: ignore[arg-type]
    btc = asyncio.run(
        reader.fetch_btc_closes_since(
            "2026-06-01T00:00:00+00:00",
            interval="15m",
        )
    )
    features = asyncio.run(
        reader.fetch_features_since("2026-06-01T00:00:00+00:00")
    )

    assert len(btc) == 1
    assert btc[0]["close"] == "100"
    assert len(features) == 1
    assert features[0]["source"] == "fear_greed"
