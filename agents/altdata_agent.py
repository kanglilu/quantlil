from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
import requests
import yfinance as yf

from storage.supabase_client import SupabaseStore
from storage.data_lake import DataLakeWriter
from utils.retry import retry_async


FEAR_GREED_URL = "https://api.alternative.me/fng/"
POLYMARKET_MARKETS_URL = "https://gamma-api.polymarket.com/markets/keyset"
DXY_TICKER = "DX-Y.NYB"
POLYMARKET_PAGE_SIZE = 100


class AltDataAgent:
    def __init__(
        self,
        store: SupabaseStore,
        data_lake: DataLakeWriter | None = None,
    ) -> None:
        self.store = store
        self.data_lake = data_lake
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_all(self) -> dict[str, Any]:
        fetchers = {
            "fear_greed": self.fetch_fear_greed,
            "dxy": self.fetch_dxy,
            "polymarket": self.fetch_polymarket_volume,
        }
        results: dict[str, Any] = {}
        rows: list[dict[str, Any]] = []

        for source, fetcher in fetchers.items():
            try:
                row = await fetcher()
                rows.append(row)
                results[source] = {"status": "ok", "row": row}
            except Exception as exc:
                self.logger.exception("Alt-data fetch failed for %s", source)
                results[source] = {"status": "error", "error": str(exc)}

        upserted = await self.store.upsert_alt_data(rows) if rows else 0
        results["upserted"] = upserted
        results["r2_archive"] = await self._archive(rows)
        return results

    async def _archive(
        self, rows: list[dict[str, Any]]
    ) -> dict[str, Any]:
        if self.data_lake is None:
            return {"status": "disabled"}

        archived: dict[str, Any] = {}
        for row in rows:
            dataset = str(row["source"])
            try:
                archived[dataset] = await self.data_lake.archive_rows(
                    namespace="alternative",
                    dataset=dataset,
                    rows=[row],
                    unique_keys=("source", "metric_name", "timestamp"),
                    partitions={"metric": str(row["metric_name"])},
                )
            except Exception as exc:
                self.logger.exception(
                    "R2 archive failed for alt source %s", dataset
                )
                archived[dataset] = {
                    "status": "error",
                    "error": str(exc),
                }
        return {"status": "ok", "datasets": archived}

    async def fetch_fear_greed(self) -> dict[str, Any]:
        async def _request() -> dict[str, Any]:
            response = await asyncio.to_thread(
                requests.get,
                FEAR_GREED_URL,
                params={"limit": 1, "format": "json"},
                timeout=20,
            )
            response.raise_for_status()
            return response.json()

        payload = await retry_async(
            _request,
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="alternative_me_fear_greed",
        )
        return self._parse_fear_greed(payload)

    async def fetch_dxy(self) -> dict[str, Any]:
        async def _download() -> pd.DataFrame:
            return await asyncio.to_thread(
                yf.download,
                DXY_TICKER,
                period="5d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
                timeout=20,
            )

        frame = await retry_async(
            _download,
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="yfinance_dxy",
        )
        return self._parse_dxy(frame)

    async def fetch_polymarket_volume(self) -> dict[str, Any]:
        cursor: str | None = None
        total = Decimal("0")
        market_count = 0

        while True:
            page, next_cursor = await self._fetch_polymarket_page(cursor)
            active_markets = [
                market
                for market in page
                if market.get("active") is True
                and market.get("closed") is not True
            ]
            total += self._sum_market_volume(active_markets)
            market_count += len(active_markets)
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

        timestamp = self._current_six_hour_bucket()
        self.logger.info(
            "Aggregated Polymarket 24h volume from %s active markets",
            market_count,
        )
        return {
            "source": "polymarket",
            "metric_name": "total_volume_24h",
            "timestamp": timestamp.isoformat(),
            "value": str(total),
            "label": None,
        }

    async def _fetch_polymarket_page(
        self, cursor: str | None
    ) -> tuple[list[dict[str, Any]], str | None]:
        async def _request() -> tuple[list[dict[str, Any]], str | None]:
            params: dict[str, Any] = {
                "closed": "false",
                "limit": POLYMARKET_PAGE_SIZE,
            }
            if cursor:
                params["after_cursor"] = cursor
            response = await asyncio.to_thread(
                requests.get,
                POLYMARKET_MARKETS_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Unexpected Polymarket markets response")
            markets = payload.get("markets")
            if not isinstance(markets, list):
                raise ValueError("Polymarket response has no markets list")
            next_cursor = payload.get("next_cursor")
            return markets, str(next_cursor) if next_cursor else None

        return await retry_async(
            _request,
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="polymarket_markets_keyset",
        )

    @staticmethod
    def _parse_fear_greed(payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise ValueError("Fear & Greed response has no data")

        item = data[0]
        timestamp = datetime.fromtimestamp(
            int(item["timestamp"]), tz=timezone.utc
        )
        return {
            "source": "fear_greed",
            "metric_name": "index",
            "timestamp": timestamp.isoformat(),
            "value": str(Decimal(str(item["value"]))),
            "label": str(item.get("value_classification") or ""),
        }

    @staticmethod
    def _parse_dxy(frame: pd.DataFrame) -> dict[str, Any]:
        if frame.empty:
            raise ValueError(f"No DXY data returned for {DXY_TICKER}")

        close = frame["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = pd.to_numeric(close, errors="coerce").dropna()
        if close.empty:
            raise ValueError("DXY response has no valid close price")

        latest_timestamp = pd.Timestamp(close.index[-1])
        timestamp = datetime.combine(
            latest_timestamp.date(),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        return {
            "source": "dxy",
            "metric_name": "close",
            "timestamp": timestamp.isoformat(),
            "value": str(Decimal(str(close.iloc[-1]))),
            "label": None,
        }

    @staticmethod
    def _sum_market_volume(markets: list[dict[str, Any]]) -> Decimal:
        total = Decimal("0")
        for market in markets:
            raw_value = market.get("volume24hr")
            if raw_value in (None, ""):
                continue
            try:
                total += Decimal(str(raw_value))
            except InvalidOperation:
                continue
        return total

    @staticmethod
    def _current_six_hour_bucket() -> datetime:
        now = datetime.now(timezone.utc)
        return now.replace(
            hour=(now.hour // 6) * 6,
            minute=0,
            second=0,
            microsecond=0,
        )
