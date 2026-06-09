from __future__ import annotations

import asyncio
import logging
from typing import Any

from supabase import Client, create_client

from config import Settings
from utils.retry import retry_async


class SupabaseStore:
    PAGE_SIZE = 1000

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client: Client | None = None

        if settings.supabase_enabled:
            self.client = create_client(
                settings.supabase_url,
                settings.supabase_service_key,
            )
        else:
            self.logger.warning("Supabase env is empty; database writes are disabled")

    async def upsert_btc_ohlcv(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self.client is None:
            self.logger.info("Dry-run btc_ohlcv upsert skipped for %s rows", len(rows))
            return 0

        def _upsert() -> int:
            response = (
                self.client.table("btc_ohlcv")
                .upsert(
                    rows,
                    on_conflict="symbol,interval,timestamp",
                    ignore_duplicates=False,
                )
                .execute()
            )
            return len(response.data or [])

        return await retry_async(
            lambda: asyncio.to_thread(_upsert),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_upsert_btc_ohlcv",
        )

    async def latest_btc_ohlcv(self, limit: int = 5) -> list[dict[str, Any]]:
        if self.client is None:
            return []

        def _query() -> list[dict[str, Any]]:
            response = (
                self.client.table("btc_ohlcv")
                .select("*")
                .order("timestamp", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data or []

        return await retry_async(
            lambda: asyncio.to_thread(_query),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_latest_btc_ohlcv",
        )

    async def upsert_alt_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self.client is None:
            self.logger.info("Dry-run alt_data upsert skipped for %s rows", len(rows))
            return 0

        def _upsert() -> int:
            response = (
                self.client.table("alt_data")
                .upsert(
                    rows,
                    on_conflict="source,metric_name,timestamp",
                    ignore_duplicates=False,
                )
                .execute()
            )
            return len(response.data or [])

        return await retry_async(
            lambda: asyncio.to_thread(_upsert),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_upsert_alt_data",
        )

    async def fetch_alt_data_since(self, since: str) -> list[dict[str, Any]]:
        if self.client is None:
            return []

        def _page(offset: int) -> list[dict[str, Any]]:
            response = (
                self.client.table("alt_data")
                .select("source,metric_name,timestamp,value,label")
                .gte("timestamp", since)
                .order("timestamp")
                .range(offset, offset + self.PAGE_SIZE - 1)
                .execute()
            )
            return response.data or []

        return await self._fetch_pages(_page, "supabase_fetch_alt_data")

    async def fetch_macro_data_since(
        self, since: str
    ) -> list[dict[str, Any]]:
        if self.client is None:
            return []

        def _page(offset: int) -> list[dict[str, Any]]:
            response = (
                self.client.table("macro_data")
                .select("source,metric_name,timestamp,value")
                .gte("timestamp", since)
                .order("timestamp")
                .range(offset, offset + self.PAGE_SIZE - 1)
                .execute()
            )
            return response.data or []

        return await self._fetch_pages(_page, "supabase_fetch_macro_data")

    async def fetch_features_since(
        self, since: str
    ) -> list[dict[str, Any]]:
        alt_rows, macro_rows = await asyncio.gather(
            self.fetch_alt_data_since(since),
            self.fetch_macro_data_since(since),
        )
        for row in alt_rows:
            row["dataset"] = "alt_data"
        for row in macro_rows:
            row["dataset"] = "macro_data"
            row["label"] = None
        return [*alt_rows, *macro_rows]

    async def upsert_macro_data(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        if self.client is None:
            self.logger.info(
                "Dry-run macro_data upsert skipped for %s rows", len(rows)
            )
            return 0

        def _upsert() -> int:
            response = (
                self.client.table("macro_data")
                .upsert(
                    rows,
                    on_conflict="source,metric_name,timestamp",
                    ignore_duplicates=False,
                )
                .execute()
            )
            return len(response.data or [])

        return await retry_async(
            lambda: asyncio.to_thread(_upsert),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_upsert_macro_data",
        )

    async def upsert_r2_object(self, row: dict[str, Any]) -> int:
        if self.client is None:
            self.logger.info(
                "Dry-run r2_objects upsert skipped for %s",
                row.get("object_key"),
            )
            return 0

        def _upsert() -> int:
            response = (
                self.client.table("r2_objects")
                .upsert(
                    row,
                    on_conflict="object_key",
                    ignore_duplicates=False,
                )
                .execute()
            )
            return len(response.data or [])

        return await retry_async(
            lambda: asyncio.to_thread(_upsert),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_upsert_r2_object",
        )

    async def fetch_r2_objects_since(
        self,
        since: str,
        *,
        dataset: str | None = None,
        interval: str | None = None,
        object_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.client is None:
            return []

        def _query() -> list[dict[str, Any]]:
            query = (
                self.client.table("r2_objects")
                .select(
                    "dataset,symbol,interval,object_key,"
                    "min_timestamp,max_timestamp,row_count,size_bytes"
                )
                .gte("max_timestamp", since)
                .order("min_timestamp")
            )
            if dataset:
                query = query.eq("dataset", dataset)
            if interval:
                query = query.eq("interval", interval)
            if object_prefix:
                query = query.like("object_key", f"{object_prefix}%")
            return query.execute().data or []

        return await retry_async(
            lambda: asyncio.to_thread(_query),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_fetch_r2_objects",
        )

    async def fetch_btc_closes_since(
        self,
        since: str,
        *,
        interval: str,
    ) -> list[dict[str, Any]]:
        if self.client is None:
            return []

        def _page(offset: int) -> list[dict[str, Any]]:
            response = (
                self.client.table("btc_ohlcv")
                .select("symbol,interval,timestamp,close")
                .eq("symbol", "BTCUSDT")
                .eq("interval", interval)
                .gte("timestamp", since)
                .order("timestamp")
                .range(offset, offset + self.PAGE_SIZE - 1)
                .execute()
            )
            return response.data or []

        return await self._fetch_pages(_page, "supabase_fetch_btc_closes")

    async def sample_counts(self) -> dict[str, dict[str, int]]:
        alt_queries = {
            "fear_greed": ("fear_greed", "index"),
            "dxy": ("dxy", "close"),
            "polymarket": ("polymarket", "total_volume_24h"),
        }
        macro_queries = {
            "treasury_10y": ("fred", "treasury_10y"),
            "google_trends_btc": ("google_trends", "bitcoin"),
            "google_trends_crypto": ("google_trends", "crypto"),
            "bdi": ("bdi", "close"),
        }

        async def count_group(
            table: str,
            queries: dict[str, tuple[str, str]],
        ) -> dict[str, int]:
            names = list(queries)
            counts = await asyncio.gather(
                *[
                    self._count_rows(
                        table,
                        source=queries[name][0],
                        metric_name=queries[name][1],
                    )
                    for name in names
                ]
            )
            return dict(zip(names, counts))

        alt, macro, btc = await asyncio.gather(
            count_group("alt_data", alt_queries),
            count_group("macro_data", macro_queries),
            self._btc_sample_counts(),
        )
        return {"alt_data": alt, "macro_data": macro, "btc_ohlcv": btc}

    async def _btc_sample_counts(self) -> dict[str, int]:
        intervals = ("1m", "5m", "15m")
        catalog_rows = await self._fetch_r2_btc_catalog()
        catalog_counts = {interval: 0 for interval in intervals}
        for row in catalog_rows:
            interval = str(row.get("interval", ""))
            if interval in catalog_counts:
                catalog_counts[interval] += int(row.get("row_count") or 0)

        async def with_fallback(interval: str) -> int:
            if catalog_counts[interval] > 0:
                return catalog_counts[interval]
            return await self._count_rows(
                "btc_ohlcv",
                symbol="BTCUSDT",
                interval=interval,
            )

        counts = await asyncio.gather(
            *[with_fallback(interval) for interval in intervals]
        )
        return dict(zip(intervals, counts))

    async def _fetch_r2_btc_catalog(self) -> list[dict[str, Any]]:
        if self.client is None:
            return []

        def _page(offset: int) -> list[dict[str, Any]]:
            response = (
                self.client.table("r2_objects")
                .select("interval,row_count")
                .eq("dataset", "btc_ohlcv")
                .range(offset, offset + self.PAGE_SIZE - 1)
                .execute()
            )
            return response.data or []

        return await self._fetch_pages(
            _page,
            "supabase_fetch_r2_btc_catalog",
        )

    async def _count_rows(self, table: str, **filters: str) -> int:
        if self.client is None:
            return 0

        def _query() -> int:
            query = self.client.table(table).select(
                "id",
                count="exact",
            )
            for column, value in filters.items():
                query = query.eq(column, value)
            response = query.limit(1).execute()
            return int(response.count or 0)

        filter_text = "_".join(f"{key}_{value}" for key, value in filters.items())
        return await retry_async(
            lambda: asyncio.to_thread(_query),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name=f"supabase_count_{table}_{filter_text}",
        )

    async def upsert_correlation_results(
        self, rows: list[dict[str, Any]]
    ) -> int:
        if not rows:
            return 0
        if self.client is None:
            self.logger.info(
                "Dry-run correlation_results upsert skipped for %s rows",
                len(rows),
            )
            return 0

        def _upsert() -> int:
            response = (
                self.client.table("correlation_results")
                .upsert(
                    rows,
                    on_conflict=(
                        "dataset_a,dataset_b,lag_hours,date_calculated"
                    ),
                    ignore_duplicates=False,
                )
                .execute()
            )
            return len(response.data or [])

        return await retry_async(
            lambda: asyncio.to_thread(_upsert),
            attempts=self.settings.retry_attempts,
            base_delay=self.settings.retry_base_delay,
            logger=self.logger,
            operation_name="supabase_upsert_correlation_results",
        )

    async def _fetch_pages(
        self,
        page_loader: Any,
        operation_name: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0

        while True:
            page = await retry_async(
                lambda current_offset=offset: asyncio.to_thread(
                    page_loader, current_offset
                ),
                attempts=self.settings.retry_attempts,
                base_delay=self.settings.retry_base_delay,
                logger=self.logger,
                operation_name=f"{operation_name}_offset_{offset}",
            )
            rows.extend(page)
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

        return rows
