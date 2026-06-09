from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from binance.client import Client

from config import PRICE_FETCH_LIMIT, PRICE_INTERVALS, SYMBOL
from storage.data_lake import DataLakeWriter
from storage.supabase_client import SupabaseStore
from utils.retry import retry_async


class PriceAgent:
    def __init__(
        self,
        store: SupabaseStore,
        data_lake: DataLakeWriter | None = None,
    ) -> None:
        self.store = store
        self.data_lake = data_lake
        self.client = Client(api_key=None, api_secret=None)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_all(self) -> dict[str, Any]:
        results: dict[str, Any] = {}
        for interval in PRICE_INTERVALS:
            try:
                results[interval] = await self.fetch_interval(interval)
            except Exception as exc:
                self.logger.exception("Price fetch failed for %s", interval)
                results[interval] = {
                    "symbol": SYMBOL,
                    "interval": interval,
                    "status": "error",
                    "error": str(exc),
                    "fetched": 0,
                    "upserted": 0,
                }
        return results

    async def fetch_interval(self, interval: str) -> dict[str, Any]:
        klines = await retry_async(
            lambda: asyncio.to_thread(
                self.client.get_klines,
                symbol=SYMBOL,
                interval=interval,
                limit=PRICE_FETCH_LIMIT,
            ),
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name=f"binance_get_klines_{interval}",
        )
        rows = [self._map_kline(interval, kline) for kline in klines]
        inserted = await self.store.upsert_btc_ohlcv(rows)
        archive = await self._archive(interval, rows)
        return {
            "status": "ok",
            "symbol": SYMBOL,
            "interval": interval,
            "fetched": len(rows),
            "upserted": inserted,
            "r2_archive": archive,
        }

    async def _archive(
        self, interval: str, rows: list[dict[str, Any]]
    ) -> dict[str, Any]:
        if self.data_lake is None:
            return {"status": "disabled"}
        try:
            return await self.data_lake.archive_rows(
                namespace="market",
                dataset="btc_ohlcv",
                rows=rows,
                unique_keys=("symbol", "interval", "timestamp"),
                symbol=SYMBOL,
                interval=interval,
            )
        except Exception as exc:
            self.logger.exception(
                "R2 archive failed for BTC %s", interval
            )
            return {"status": "error", "error": str(exc)}

    @staticmethod
    def _map_kline(interval: str, kline: list[Any]) -> dict[str, Any]:
        timestamp = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
        return {
            "symbol": SYMBOL,
            "interval": interval,
            "timestamp": timestamp.isoformat(),
            "open": str(Decimal(kline[1])),
            "high": str(Decimal(kline[2])),
            "low": str(Decimal(kline[3])),
            "close": str(Decimal(kline[4])),
            "volume": str(Decimal(kline[5])),
        }
