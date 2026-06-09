from __future__ import annotations

import argparse
import asyncio
import io
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from binance.client import Client
from pytrends.request import TrendReq

from config import PRICE_INTERVALS, SYMBOL, get_settings
from storage.data_lake import DataLakeWriter
from storage.r2_client import R2Store
from storage.supabase_client import SupabaseStore
from utils.logging_config import configure_logging
from utils.retry import retry_async


FEAR_GREED_URL = "https://api.alternative.me/fng/"
FRED_DGS10_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
DXY_TICKER = "DX-Y.NYB"
GOOGLE_TRENDS_KEYWORDS = ("bitcoin", "crypto")
GOOGLE_TRENDS_TIMEFRAME = "today 5-y"
BINANCE_PAGE_SIZE = 1000
DEFAULT_BACKFILL_DAYS = 730
INTERVAL_MILLISECONDS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
}


@dataclass
class BackfillResult:
    source: str
    status: str
    fetched: int = 0
    archived: int = 0
    error: str = ""


class BackfillRunner:
    def __init__(
        self,
        store: SupabaseStore,
        data_lake: DataLakeWriter,
        *,
        days: int = DEFAULT_BACKFILL_DAYS,
        google_delay: float = 30.0,
    ) -> None:
        self.store = store
        self.data_lake = data_lake
        self.days = days
        self.google_delay = google_delay
        self.client = Client(api_key=None, api_secret=None)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self, sources: list[str]) -> list[BackfillResult]:
        results: list[BackfillResult] = []

        if "btc" in sources:
            for interval in PRICE_INTERVALS:
                results.append(
                    await self._run_isolated(
                        f"btc_{interval}",
                        lambda current_interval=interval: self.backfill_btc(
                            current_interval
                        ),
                    )
                )

        source_jobs: list[
            tuple[str, Callable[[], Awaitable[tuple[int, int]]]]
        ] = [
            ("fear_greed", self.backfill_fear_greed),
            ("dxy", self.backfill_dxy),
            ("treasury_10y", self.backfill_treasury_10y),
        ]
        for source, job in source_jobs:
            if source in sources:
                results.append(await self._run_isolated(source, job))

        if "google_trends" in sources:
            for index, keyword in enumerate(GOOGLE_TRENDS_KEYWORDS):
                if index > 0:
                    print(
                        f"[google_trends:{keyword}] waiting "
                        f"{self.google_delay:.0f}s between requests"
                    )
                    await asyncio.sleep(self.google_delay)
                results.append(
                    await self._run_isolated(
                        f"google_trends_{keyword}",
                        lambda current_keyword=keyword: (
                            self.backfill_google_trends(current_keyword)
                        ),
                    )
                )

        return results

    async def _run_isolated(
        self,
        source: str,
        job: Callable[[], Awaitable[tuple[int, int]]],
    ) -> BackfillResult:
        print(f"\n[{source}] starting")
        try:
            fetched, archived = await job()
            print(
                f"[{source}] done: fetched={fetched:,}, "
                f"archived={archived:,}"
            )
            return BackfillResult(
                source=source,
                status="ok",
                fetched=fetched,
                archived=archived,
            )
        except Exception as exc:
            self.logger.exception("Backfill failed for %s", source)
            print(f"[{source}] FAILED: {type(exc).__name__}: {exc}")
            return BackfillResult(
                source=source,
                status="error",
                error=f"{type(exc).__name__}: {exc}",
            )

    async def backfill_btc(self, interval: str) -> tuple[int, int]:
        interval_ms = INTERVAL_MILLISECONDS[interval]
        end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start = end - timedelta(days=self.days)
        estimated = max(
            1,
            (int(end.timestamp() * 1000) - int(start.timestamp() * 1000))
            // interval_ms
            + 1,
        )
        fetched = 0
        archived = 0
        current_day = start.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        final_day = end.replace(hour=0, minute=0, second=0, microsecond=0)
        day_number = 0
        total_days = (final_day.date() - current_day.date()).days + 1

        while current_day <= final_day:
            day_end = min(
                current_day + timedelta(days=1) - timedelta(milliseconds=1),
                end,
            )
            range_start = max(current_day, start)
            rows = await self._fetch_binance_range(
                interval,
                range_start,
                day_end,
            )
            if rows:
                result = await self.data_lake.archive_rows(
                    namespace="market",
                    dataset="btc_ohlcv",
                    rows=rows,
                    unique_keys=("symbol", "interval", "timestamp"),
                    symbol=SYMBOL,
                    interval=interval,
                    partition_granularity="day",
                )
                fetched += len(rows)
                archived += int(result["rows"])

            day_number += 1
            if day_number == 1 or day_number % 10 == 0:
                percent = min(100.0, day_number / total_days * 100)
                print(
                    f"[btc:{interval}] days={day_number}/{total_days} "
                    f"candles={fetched:,}/{estimated:,} ({percent:.1f}%)"
                )
            current_day += timedelta(days=1)

        print(f"[btc:{interval}] progress 100%")
        return fetched, archived

    async def _fetch_binance_range(
        self,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        interval_ms = INTERVAL_MILLISECONDS[interval]
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        rows: list[dict[str, Any]] = []

        while start_ms <= end_ms:
            klines = await retry_async(
                lambda current_start=start_ms: asyncio.to_thread(
                    self.client.get_klines,
                    symbol=SYMBOL,
                    interval=interval,
                    startTime=current_start,
                    endTime=end_ms,
                    limit=BINANCE_PAGE_SIZE,
                ),
                attempts=self.store.settings.retry_attempts,
                base_delay=self.store.settings.retry_base_delay,
                logger=self.logger,
                operation_name=f"backfill_binance_{interval}",
            )
            if not klines:
                break
            rows.extend(
                self._map_binance_kline(interval, item) for item in klines
            )
            next_start_ms = int(klines[-1][0]) + interval_ms
            if next_start_ms <= start_ms:
                raise RuntimeError("Binance pagination did not advance")
            start_ms = next_start_ms
            if len(klines) < BINANCE_PAGE_SIZE:
                break
            await asyncio.sleep(0.05)
        return rows

    async def backfill_fear_greed(self) -> tuple[int, int]:
        async def _request() -> dict[str, Any]:
            response = await asyncio.to_thread(
                requests.get,
                FEAR_GREED_URL,
                params={"limit": 365, "format": "json"},
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        payload = await retry_async(
            _request,
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="backfill_fear_greed",
        )
        rows = self._parse_fear_greed_history(payload)
        result = await self.data_lake.archive_rows(
            namespace="alternative",
            dataset="fear_greed",
            rows=rows,
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": "index"},
            partition_granularity="year",
        )
        return len(rows), int(result["rows"])

    async def backfill_dxy(self) -> tuple[int, int]:
        frame = await retry_async(
            lambda: asyncio.to_thread(
                yf.download,
                DXY_TICKER,
                period="2y",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
                timeout=30,
            ),
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="backfill_dxy",
        )
        rows = self._parse_market_history(
            frame,
            source="dxy",
            metric_name="close",
            include_label=True,
        )
        result = await self.data_lake.archive_rows(
            namespace="alternative",
            dataset="dxy",
            rows=rows,
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": "close"},
            partition_granularity="year",
        )
        return len(rows), int(result["rows"])

    async def backfill_treasury_10y(self) -> tuple[int, int]:
        start_date = (
            datetime.now(timezone.utc).date() - timedelta(days=self.days)
        ).isoformat()

        async def _request() -> str:
            response = await asyncio.to_thread(
                requests.get,
                FRED_DGS10_URL,
                params={"id": "DGS10", "cosd": start_date},
                timeout=60,
            )
            response.raise_for_status()
            return response.text

        csv_text = await retry_async(
            _request,
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="backfill_fred_dgs10",
        )
        rows = self._parse_fred_history(csv_text)
        result = await self.data_lake.archive_rows(
            namespace="macro",
            dataset="fred",
            rows=rows,
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": "treasury_10y"},
            partition_granularity="year",
        )
        return len(rows), int(result["rows"])

    async def backfill_google_trends(
        self, keyword: str
    ) -> tuple[int, int]:
        frame = await self._request_google_trends_history(keyword)
        rows = self._parse_google_trends_history(frame, keyword)
        result = await self.data_lake.archive_rows(
            namespace="macro",
            dataset="google_trends",
            rows=rows,
            unique_keys=("source", "metric_name", "timestamp"),
            partitions={"metric": keyword},
            partition_granularity="year",
        )
        return len(rows), int(result["rows"])

    async def _request_google_trends_history(
        self, keyword: str
    ) -> pd.DataFrame:
        delays = (30, 60, 120)
        last_exc: Exception | None = None

        for attempt in range(4):
            if attempt > 0:
                delay = delays[attempt - 1]
                print(
                    f"[google_trends:{keyword}] retry {attempt}/3 "
                    f"in {delay}s"
                )
                await asyncio.sleep(delay)
            try:
                return await asyncio.to_thread(
                    self._google_trends_request_sync,
                    keyword,
                )
            except Exception as exc:
                last_exc = exc
                self.logger.warning(
                    "Google Trends history %s attempt %s/4 failed: %s",
                    keyword,
                    attempt + 1,
                    exc,
                )

        assert last_exc is not None
        raise last_exc

    @staticmethod
    def _google_trends_request_sync(keyword: str) -> pd.DataFrame:
        client = TrendReq(hl="en-US", tz=0, timeout=(10, 60))
        client.build_payload([keyword], timeframe=GOOGLE_TRENDS_TIMEFRAME)
        return client.interest_over_time()

    @staticmethod
    def _map_binance_kline(
        interval: str, kline: list[Any]
    ) -> dict[str, Any]:
        timestamp = datetime.fromtimestamp(
            int(kline[0]) / 1000,
            tz=timezone.utc,
        )
        return {
            "symbol": SYMBOL,
            "interval": interval,
            "timestamp": timestamp.isoformat(),
            "open": str(Decimal(str(kline[1]))),
            "high": str(Decimal(str(kline[2]))),
            "low": str(Decimal(str(kline[3]))),
            "close": str(Decimal(str(kline[4]))),
            "volume": str(Decimal(str(kline[5]))),
        }

    @staticmethod
    def _parse_fear_greed_history(
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise ValueError("Fear & Greed response has no data")

        rows = []
        for item in data:
            timestamp = datetime.fromtimestamp(
                int(item["timestamp"]),
                tz=timezone.utc,
            )
            rows.append(
                {
                    "source": "fear_greed",
                    "metric_name": "index",
                    "timestamp": timestamp.isoformat(),
                    "value": str(Decimal(str(item["value"]))),
                    "label": str(
                        item.get("value_classification") or ""
                    ),
                }
            )
        return rows

    @staticmethod
    def _parse_market_history(
        frame: pd.DataFrame,
        *,
        source: str,
        metric_name: str,
        include_label: bool,
    ) -> list[dict[str, Any]]:
        if frame.empty:
            raise ValueError(f"No historical data returned for {source}")

        close = frame["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = pd.to_numeric(close, errors="coerce").dropna()
        rows = []
        for index, value in close.items():
            timestamp = datetime.combine(
                pd.Timestamp(index).date(),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            row = {
                "source": source,
                "metric_name": metric_name,
                "timestamp": timestamp.isoformat(),
                "value": str(Decimal(str(value))),
            }
            if include_label:
                row["label"] = None
            rows.append(row)
        return rows

    @staticmethod
    def _parse_fred_history(csv_text: str) -> list[dict[str, Any]]:
        frame = pd.read_csv(io.StringIO(csv_text))
        date_column = (
            "observation_date" if "observation_date" in frame else "DATE"
        )
        if date_column not in frame or "DGS10" not in frame:
            raise ValueError("Unexpected FRED DGS10 CSV columns")

        frame[date_column] = pd.to_datetime(
            frame[date_column], errors="coerce"
        )
        frame["DGS10"] = pd.to_numeric(frame["DGS10"], errors="coerce")
        frame = frame.dropna(subset=[date_column, "DGS10"])
        rows = []
        for _, item in frame.iterrows():
            timestamp = datetime.combine(
                pd.Timestamp(item[date_column]).date(),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            rows.append(
                {
                    "source": "fred",
                    "metric_name": "treasury_10y",
                    "timestamp": timestamp.isoformat(),
                    "value": str(Decimal(str(item["DGS10"]))),
                }
            )
        return rows

    @staticmethod
    def _parse_google_trends_history(
        frame: pd.DataFrame,
        keyword: str,
    ) -> list[dict[str, Any]]:
        if frame.empty or keyword not in frame.columns:
            raise ValueError(
                f"Google Trends returned no historical data for {keyword}"
            )

        values = pd.to_numeric(frame[keyword], errors="coerce").dropna()
        rows = []
        for index, value in values.items():
            timestamp = pd.Timestamp(index)
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize("UTC")
            else:
                timestamp = timestamp.tz_convert("UTC")
            rows.append(
                {
                    "source": "google_trends",
                    "metric_name": keyword,
                    "timestamp": timestamp.isoformat(),
                    "value": str(Decimal(str(value))),
                }
            )
        return rows


def print_summary(results: list[BackfillResult]) -> None:
    print("\n" + "=" * 72)
    print("BACKFILL SUMMARY")
    print("=" * 72)
    print(
        f"{'source':28} {'status':8} "
        f"{'fetched':>12} {'archived':>12}"
    )
    for result in results:
        print(
            f"{result.source:28} {result.status:8} "
            f"{result.fetched:12,} {result.archived:12,}"
        )
        if result.error:
            print(f"  error: {result.error}")
    print("=" * 72)
    print(
        f"TOTAL{'':23} "
        f"{sum(item.fetched for item in results):12,} "
        f"{sum(item.archived for item in results):12,}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-time historical backfill into R2 Parquet; "
            "Supabase stores catalog metadata only."
        )
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=[
            "btc",
            "fear_greed",
            "dxy",
            "treasury_10y",
            "google_trends",
        ],
        default=[
            "btc",
            "fear_greed",
            "dxy",
            "treasury_10y",
            "google_trends",
        ],
        help="Sources to run. Default: all.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_BACKFILL_DAYS,
        help="BTC/FRED lookback days. Default: 730.",
    )
    parser.add_argument(
        "--google-delay",
        type=float,
        default=30.0,
        help="Delay between Google Trends keywords. Default: 30 seconds.",
    )
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level, "logs/backfill.log")

    if not settings.supabase_enabled:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY are required.")
        return 1

    print("Quant Pipeline historical backfill")
    print(f"Sources: {', '.join(args.sources)}")
    print(f"Lookback: {args.days} days")
    if "btc" in args.sources:
        estimated_rows = int(
            args.days
            * 24
            * (60 + 12 + 4)
        )
        print(
            f"BTC estimate: about {estimated_rows:,} rows for "
            f"{args.days} days across 1m/5m/15m."
        )
    print("Raw historical rows go to R2 only.")
    print("Supabase receives only r2_objects catalog metadata.")
    print("Rerunning is deduplicated but repeats API and R2 work.")

    store = SupabaseStore(settings)
    if not settings.r2_enabled:
        print("ERROR: complete R2 credentials are required.")
        return 1
    data_lake = DataLakeWriter(R2Store(settings), store)
    runner = BackfillRunner(
        store,
        data_lake,
        days=args.days,
        google_delay=args.google_delay,
    )
    results = await runner.run(args.sources)
    print_summary(results)
    return 0 if all(item.status == "ok" for item in results) else 2


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(async_main()))
    except KeyboardInterrupt:
        print("\nBackfill interrupted. Existing upserts are preserved.")
        raise SystemExit(130)
