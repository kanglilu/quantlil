from __future__ import annotations

import asyncio
import logging
from typing import Any

import pandas as pd

from storage.r2_client import R2Store
from storage.supabase_client import SupabaseStore


class DataLakeReader:
    def __init__(
        self,
        r2: R2Store,
        catalog: SupabaseStore,
        *,
        max_concurrent_downloads: int = 8,
    ) -> None:
        self.r2 = r2
        self.catalog = catalog
        self.max_concurrent_downloads = max(1, max_concurrent_downloads)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_btc_closes_since(
        self,
        since: str,
        *,
        interval: str,
    ) -> list[dict[str, Any]]:
        objects = await self.catalog.fetch_r2_objects_since(
            since,
            dataset="btc_ohlcv",
            interval=interval,
        )
        frame = await self._load_objects(objects)
        if frame.empty:
            return []
        frame = self._filter_since(frame, since)
        columns = ["symbol", "interval", "timestamp", "close"]
        return frame[columns].to_dict("records")

    async def fetch_features_since(
        self, since: str
    ) -> list[dict[str, Any]]:
        alternative, macro = await asyncio.gather(
            self.catalog.fetch_r2_objects_since(
                since,
                object_prefix="raw/alternative/",
            ),
            self.catalog.fetch_r2_objects_since(
                since,
                object_prefix="raw/macro/",
            ),
        )
        frame = await self._load_objects([*alternative, *macro])
        if frame.empty:
            return []
        frame = self._filter_since(frame, since)
        if "label" not in frame.columns:
            frame["label"] = None
        columns = ["source", "metric_name", "timestamp", "value", "label"]
        return frame[columns].to_dict("records")

    async def _load_objects(
        self, objects: list[dict[str, Any]]
    ) -> pd.DataFrame:
        if not objects:
            return pd.DataFrame()

        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

        async def download(item: dict[str, Any]) -> pd.DataFrame | None:
            async with semaphore:
                return await self.r2.download_parquet(item["object_key"])

        frames = await asyncio.gather(
            *[download(item) for item in objects]
        )
        available = [frame for frame in frames if frame is not None]
        if not available:
            return pd.DataFrame()
        return pd.concat(available, ignore_index=True, sort=False)

    @staticmethod
    def _filter_since(frame: pd.DataFrame, since: str) -> pd.DataFrame:
        frame = frame.copy()
        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"], utc=True, errors="coerce"
        )
        cutoff = pd.Timestamp(since)
        if cutoff.tzinfo is None:
            cutoff = cutoff.tz_localize("UTC")
        else:
            cutoff = cutoff.tz_convert("UTC")
        return frame.loc[frame["timestamp"] >= cutoff].copy()
