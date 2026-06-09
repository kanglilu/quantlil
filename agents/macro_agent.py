from __future__ import annotations

import asyncio
import io
import logging
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from pytrends.request import TrendReq

from storage.supabase_client import SupabaseStore
from storage.data_lake import DataLakeWriter
from utils.retry import retry_async


FRED_DGS10_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
GOOGLE_TRENDS_KEYWORDS = ("bitcoin", "crypto")
GOOGLE_TRENDS_TIMEFRAME = "today 3-m"
GOOGLE_TRENDS_RETRY_DELAYS = (30, 60, 120)
GOOGLE_TRENDS_KEYWORD_DELAY = 30
BDI_TICKERS = ("^BDI", "BDI")


class MacroAgent:
    def __init__(
        self,
        store: SupabaseStore,
        data_lake: DataLakeWriter | None = None,
    ) -> None:
        self.store = store
        self.data_lake = data_lake
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_all(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        results: dict[str, Any] = {}

        try:
            fred_row = await self.fetch_fred_treasury_10y()
            rows.append(fred_row)
            results["treasury_10y"] = {"status": "ok", "row": fred_row}
        except Exception as exc:
            self.logger.exception("FRED DGS10 fetch failed")
            results["treasury_10y"] = {
                "status": "error",
                "error": str(exc),
            }

        trend_rows, trend_metrics = await self.fetch_google_trends()
        rows.extend(trend_rows)
        trend_statuses = {
            metric["status"] for metric in trend_metrics.values()
        }
        trends_status = (
            "ok"
            if trend_statuses == {"ok"}
            else "skip"
            if trend_statuses == {"skip"}
            else "partial"
        )
        results["google_trends"] = {
            "status": trends_status,
            "metrics": trend_metrics,
        }

        bdi_result = await self.fetch_bdi()
        results["bdi"] = bdi_result
        if bdi_result["status"] == "ok":
            rows.append(bdi_result["row"])

        results["upserted"] = (
            await self.store.upsert_macro_data(rows) if rows else 0
        )
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
            metric = str(row["metric_name"])
            archive_key = f"{dataset}.{metric}"
            try:
                archived[archive_key] = (
                    await self.data_lake.archive_rows(
                        namespace="macro",
                        dataset=dataset,
                        rows=[row],
                        unique_keys=(
                            "source",
                            "metric_name",
                            "timestamp",
                        ),
                        partitions={"metric": metric},
                    )
                )
            except Exception as exc:
                self.logger.exception(
                    "R2 archive failed for macro source %s", archive_key
                )
                archived[archive_key] = {
                    "status": "error",
                    "error": str(exc),
                }
        return {"status": "ok", "datasets": archived}

    async def fetch_fred_treasury_10y(self) -> dict[str, Any]:
        async def _request() -> str:
            response = await asyncio.to_thread(
                requests.get,
                FRED_DGS10_URL,
                params={
                    "cosd": (
                        datetime.now(timezone.utc).date()
                        - timedelta(days=30)
                    ).isoformat()
                },
                timeout=30,
            )
            response.raise_for_status()
            return response.text

        csv_text = await retry_async(
            _request,
            attempts=self.store.settings.retry_attempts,
            base_delay=self.store.settings.retry_base_delay,
            logger=self.logger,
            operation_name="fred_dgs10",
        )
        return self._parse_fred_csv(csv_text)

    async def fetch_google_trends(
        self,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        delay = random.uniform(8, 15)
        self.logger.info(
            "Waiting %.1fs before Google Trends requests", delay
        )
        await asyncio.sleep(delay)

        rows: list[dict[str, Any]] = []
        metrics: dict[str, dict[str, Any]] = {}

        for index, keyword in enumerate(GOOGLE_TRENDS_KEYWORDS):
            if index > 0:
                self.logger.info(
                    "Waiting %ss before Google Trends keyword %s",
                    GOOGLE_TRENDS_KEYWORD_DELAY,
                    keyword,
                )
                await asyncio.sleep(GOOGLE_TRENDS_KEYWORD_DELAY)

            row, metric_result = await self._fetch_google_trend_keyword(
                keyword
            )
            rows.append(row)
            metrics[keyword] = metric_result

        return rows, metrics

    async def _fetch_google_trend_keyword(
        self, keyword: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        last_exc: Exception | None = None

        for attempt in range(len(GOOGLE_TRENDS_RETRY_DELAYS) + 1):
            if attempt > 0:
                delay = GOOGLE_TRENDS_RETRY_DELAYS[attempt - 1]
                self.logger.warning(
                    "Google Trends %s retry %s/3 in %ss",
                    keyword,
                    attempt,
                    delay,
                )
                await asyncio.sleep(delay)

            try:
                frame = await self._request_google_trend(keyword)
                row = self._parse_google_trend_keyword(frame, keyword)
                return row, {"status": "ok", "row": row}
            except Exception as exc:
                last_exc = exc
                self.logger.warning(
                    "Google Trends %s attempt %s/4 failed: %s",
                    keyword,
                    attempt + 1,
                    exc,
                )

        error = str(last_exc) if last_exc else "unknown error"
        self.logger.error(
            "Google Trends %s rate limited, skip: %s",
            keyword,
            error,
        )
        row = self._google_trends_skip_row(keyword)
        return row, {"status": "skip", "error": error, "row": row}

    @staticmethod
    async def _request_google_trend(keyword: str) -> pd.DataFrame:
        def _fetch() -> pd.DataFrame:
            client = TrendReq(hl="en-US", tz=0, timeout=(10, 30))
            client.build_payload(
                [keyword],
                timeframe=GOOGLE_TRENDS_TIMEFRAME,
            )
            return client.interest_over_time()

        return await asyncio.to_thread(_fetch)

    async def fetch_bdi(self) -> dict[str, Any]:
        errors: list[str] = []

        for ticker in BDI_TICKERS:
            try:
                frame = await asyncio.to_thread(
                    yf.download,
                    ticker,
                    period="5d",
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                    timeout=20,
                )
                row = self._parse_bdi(frame)
                self.logger.info("BDI fetched with ticker %s", ticker)
                return {"status": "ok", "ticker": ticker, "row": row}
            except Exception as exc:
                errors.append(f"{ticker}: {exc}")
                self.logger.warning("BDI ticker %s failed: %s", ticker, exc)

        error = "; ".join(errors) or "No BDI data returned"
        self.logger.error("BDI skipped: %s", error)
        return {"status": "skip", "error": error}

    @staticmethod
    def _parse_fred_csv(csv_text: str) -> dict[str, Any]:
        frame = pd.read_csv(io.StringIO(csv_text))
        date_column = "observation_date" if "observation_date" in frame else "DATE"
        if date_column not in frame or "DGS10" not in frame:
            raise ValueError("Unexpected FRED DGS10 CSV columns")

        frame[date_column] = pd.to_datetime(
            frame[date_column], errors="coerce"
        )
        frame["DGS10"] = pd.to_numeric(frame["DGS10"], errors="coerce")
        frame = frame.dropna(subset=[date_column, "DGS10"])
        if frame.empty:
            raise ValueError("FRED DGS10 has no valid observations")

        latest = frame.iloc[-1]
        timestamp = datetime.combine(
            pd.Timestamp(latest[date_column]).date(),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        return {
            "source": "fred",
            "metric_name": "treasury_10y",
            "timestamp": timestamp.isoformat(),
            "value": str(Decimal(str(latest["DGS10"]))),
        }

    @staticmethod
    def _parse_google_trend_keyword(
        frame: pd.DataFrame,
        keyword: str,
    ) -> dict[str, Any]:
        if frame.empty:
            raise ValueError("Google Trends returned no data")
        if keyword not in frame.columns:
            raise ValueError(
                f"Google Trends response is missing keyword {keyword}"
            )

        valid = pd.to_numeric(frame[keyword], errors="coerce").dropna()
        if valid.empty:
            raise ValueError("Google Trends has no valid observations")

        timestamp = pd.Timestamp(valid.index[-1])
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")

        return {
            "source": "google_trends",
            "metric_name": keyword,
            "timestamp": timestamp.isoformat(),
            "value": str(Decimal(str(valid.iloc[-1]))),
        }

    @staticmethod
    def _google_trends_skip_row(keyword: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        timestamp = now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        return {
            "source": "google_trends",
            "metric_name": keyword,
            "timestamp": timestamp.isoformat(),
            "value": None,
        }

    @staticmethod
    def _parse_bdi(frame: pd.DataFrame) -> dict[str, Any]:
        if frame.empty:
            raise ValueError("No BDI data returned")

        close = frame["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = pd.to_numeric(close, errors="coerce").dropna()
        if close.empty:
            raise ValueError("BDI response has no valid close")

        latest_timestamp = pd.Timestamp(close.index[-1])
        timestamp = datetime.combine(
            latest_timestamp.date(),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        return {
            "source": "bdi",
            "metric_name": "close",
            "timestamp": timestamp.isoformat(),
            "value": str(Decimal(str(close.iloc[-1]))),
        }
