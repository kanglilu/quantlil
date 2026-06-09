from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from scipy.stats import pearsonr

from config import (
    CORRELATION_MAX_P_VALUE,
    CORRELATION_MIN_ABS_R,
    CORRELATION_MIN_SAMPLE_SIZE,
    LAG_HOURS,
    Settings,
)
from storage.supabase_client import SupabaseStore
from storage.data_lake_reader import DataLakeReader


EXPECTED_FEATURE_SOURCES = (
    "fear_greed",
    "dxy",
    "polymarket",
    "fred",
    "google_trends",
)
MACRO_SOURCES = {"fred", "google_trends", "bdi"}
BASE_WINDOW_DAYS = 120
MAX_WINDOW_DAYS = 365
SKIP_UNTIL_READY = {"polymarket"}


class CorrelationEngine:
    def __init__(
        self,
        settings: Settings,
        store: SupabaseStore,
        data_lake: DataLakeReader | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.data_lake = data_lake
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        base_window_days = min(
            self.settings.correlation_lookback_days,
            BASE_WINDOW_DAYS,
        )
        since = now - timedelta(days=MAX_WINDOW_DAYS)
        since_iso = since.isoformat()

        btc_rows, feature_rows, input_source = await self._load_inputs(
            since_iso
        )

        btc = self._prepare_btc(btc_rows)
        all_features = self._prepare_alt(feature_rows)
        features, window_details = self._select_adaptive_windows(
            all_features,
            now=now,
            base_window_days=base_window_days,
            max_window_days=MAX_WINDOW_DAYS,
        )
        source_samples = self._count_adaptive_samples(window_details)
        dataset_b = (
            "btc_ohlcv.BTCUSDT."
            f"{self.settings.correlation_btc_interval}.close_1h"
        )

        evaluated = 0
        insufficient = 0
        significant_rows: list[dict[str, Any]] = []

        for (source, metric_name), series in features.items():
            table = "macro_data" if source in MACRO_SOURCES else "alt_data"
            dataset_a = f"{table}.{source}.{metric_name}"
            for lag_hours in LAG_HOURS:
                evaluated += 1
                sample = self._align_with_lag(series, btc, lag_hours)
                if (
                    len(sample) < CORRELATION_MIN_SAMPLE_SIZE
                    or sample["alt_value"].nunique() < 2
                    or sample["btc_close"].nunique() < 2
                ):
                    insufficient += 1
                    continue

                pearson_r, p_value = pearsonr(
                    sample["alt_value"],
                    sample["btc_close"],
                )
                if pd.isna(pearson_r) or pd.isna(p_value):
                    continue
                if (
                    abs(float(pearson_r)) <= CORRELATION_MIN_ABS_R
                    or float(p_value) >= CORRELATION_MAX_P_VALUE
                ):
                    continue

                significant_rows.append(
                    {
                        "dataset_a": dataset_a,
                        "dataset_b": dataset_b,
                        "lag_hours": lag_hours,
                        "pearson_r": float(pearson_r),
                        "p_value": float(p_value),
                        "sample_size": len(sample),
                        "date_calculated": now.date().isoformat(),
                    }
                )

        upserted = await self.store.upsert_correlation_results(significant_rows)
        skipped_sources = [
            source
            for source in SKIP_UNTIL_READY
            if source_samples.get(source, 0) < CORRELATION_MIN_SAMPLE_SIZE
        ]
        insufficient_sources = [
            source
            for source, sample_count in source_samples.items()
            if sample_count < CORRELATION_MIN_SAMPLE_SIZE
            and source not in SKIP_UNTIL_READY
        ]
        status = "insufficient_data" if insufficient_sources else "ok"
        message = self._build_message(
            status=status,
            source_samples=source_samples,
            significant=len(significant_rows),
            base_window_days=base_window_days,
            max_window_days=MAX_WINDOW_DAYS,
            window_details=window_details,
            skipped_sources=skipped_sources,
        )
        result = {
            "status": status,
            "message": message,
            "lookback_days": base_window_days,
            "max_lookback_days": MAX_WINDOW_DAYS,
            "btc_interval": self.settings.correlation_btc_interval,
            "input_source": input_source,
            "btc_hourly_samples": len(btc),
            "feature_series": len(features),
            "source_samples": source_samples,
            "window_details": window_details,
            "insufficient_sources": insufficient_sources,
            "skipped_sources": skipped_sources,
            "evaluated": evaluated,
            "insufficient": insufficient,
            "significant": len(significant_rows),
            "upserted": upserted,
            "results": significant_rows,
        }
        self.logger.info("Correlation analysis completed: %s", result)
        return result

    @staticmethod
    def _select_adaptive_windows(
        features: dict[tuple[str, str], pd.Series],
        *,
        now: datetime,
        base_window_days: int,
        max_window_days: int,
    ) -> tuple[
        dict[tuple[str, str], pd.Series],
        dict[str, dict[str, Any]],
    ]:
        base_cutoff = pd.Timestamp(now - timedelta(days=base_window_days))
        max_cutoff = pd.Timestamp(now - timedelta(days=max_window_days))
        selected: dict[tuple[str, str], pd.Series] = {}
        details: dict[str, dict[str, Any]] = {}

        for (source, metric_name), series in features.items():
            base_series = series.loc[series.index >= base_cutoff]
            max_series = series.loc[series.index >= max_cutoff]
            use_expanded = (
                len(base_series) < CORRELATION_MIN_SAMPLE_SIZE
                and source not in SKIP_UNTIL_READY
            )
            chosen = max_series if use_expanded else base_series

            if (
                source in SKIP_UNTIL_READY
                and len(base_series) < CORRELATION_MIN_SAMPLE_SIZE
            ):
                chosen = base_series.iloc[0:0]

            detail_key = f"{source}.{metric_name}"
            skipped = (
                source in SKIP_UNTIL_READY
                and len(base_series) < CORRELATION_MIN_SAMPLE_SIZE
            )
            details[detail_key] = {
                "source": source,
                "metric_name": metric_name,
                "base_samples": len(base_series),
                "final_samples": (
                    len(base_series) if skipped else len(chosen)
                ),
                "window_days": (
                    max_window_days if use_expanded else base_window_days
                ),
                "expanded": use_expanded,
                "skipped": skipped,
            }
            if not skipped:
                selected[(source, metric_name)] = chosen

        return selected, details

    async def _load_inputs(
        self, since_iso: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        if self.data_lake is not None:
            btc_rows, feature_rows = await asyncio.gather(
                self.data_lake.fetch_btc_closes_since(
                    since_iso,
                    interval=self.settings.correlation_btc_interval,
                ),
                self.store.fetch_features_since(since_iso),
            )
            if btc_rows:
                return (
                    btc_rows,
                    feature_rows,
                    "r2_btc+supabase_features",
                )
            self.logger.warning(
                "R2 BTC history incomplete; using Supabase fallback"
            )

        btc_rows, feature_rows = await asyncio.gather(
            self.store.fetch_btc_closes_since(
                since_iso,
                interval=self.settings.correlation_btc_interval,
            ),
            self.store.fetch_features_since(since_iso),
        )
        return btc_rows, feature_rows, "supabase"

    @staticmethod
    def _prepare_btc(rows: list[dict[str, Any]]) -> pd.Series:
        if not rows:
            return pd.Series(dtype="float64", name="btc_close")

        frame = pd.DataFrame(rows)
        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"], utc=True, errors="coerce"
        )
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.dropna(subset=["timestamp", "close"])
        frame = frame.sort_values("timestamp").drop_duplicates(
            subset=["timestamp"], keep="last"
        )
        hourly = (
            frame.set_index("timestamp")["close"]
            .resample("1h", closed="left", label="right")
            .last()
            .dropna()
        )
        hourly.name = "btc_close"
        return hourly

    @staticmethod
    def _prepare_alt(
        rows: list[dict[str, Any]],
    ) -> dict[tuple[str, str], pd.Series]:
        if not rows:
            return {}

        frame = pd.DataFrame(rows)
        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"], utc=True, errors="coerce"
        ).dt.floor("h")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(
            subset=["source", "metric_name", "timestamp", "value"]
        )

        series_by_metric: dict[tuple[str, str], pd.Series] = {}
        for keys, group in frame.groupby(["source", "metric_name"]):
            series = (
                group.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"], keep="last")
                .set_index("timestamp")["value"]
            )
            series.name = "alt_value"
            series_by_metric[(str(keys[0]), str(keys[1]))] = series
        return series_by_metric

    @staticmethod
    def _align_with_lag(
        alt: pd.Series,
        btc: pd.Series,
        lag_hours: int,
    ) -> pd.DataFrame:
        shifted_btc = btc.copy()
        shifted_btc.index = shifted_btc.index - pd.Timedelta(hours=lag_hours)
        return pd.concat([alt, shifted_btc], axis=1, join="inner").dropna()

    @staticmethod
    def _count_source_samples(
        alt: dict[tuple[str, str], pd.Series],
    ) -> dict[str, int]:
        counts = {source: 0 for source in EXPECTED_FEATURE_SOURCES}
        for (source, _metric_name), series in alt.items():
            counts[source] = max(counts.get(source, 0), len(series))
        return counts

    @staticmethod
    def _count_adaptive_samples(
        window_details: dict[str, dict[str, Any]],
    ) -> dict[str, int]:
        counts = {source: 0 for source in EXPECTED_FEATURE_SOURCES}
        for detail in window_details.values():
            source = str(detail["source"])
            counts[source] = max(
                counts.get(source, 0),
                int(detail["final_samples"]),
            )
        return counts

    @staticmethod
    def _build_message(
        *,
        status: str,
        source_samples: dict[str, int],
        significant: int,
        base_window_days: int,
        max_window_days: int,
        window_details: dict[str, dict[str, Any]],
        skipped_sources: list[str],
    ) -> str:
        lines = [
            "Correlation adaptive window:",
            f"Minimum: {CORRELATION_MIN_SAMPLE_SIZE} samples per metric",
        ]
        for detail in window_details.values():
            name = detail["source"]
            if detail["source"] == "google_trends":
                name = f"google_trends_{detail['metric_name']}"
            if detail["skipped"]:
                lines.append(
                    f"⏳ {name}: {detail['base_samples']} samples dalam "
                    f"{base_window_days} hari → skip sampai ≥30"
                )
            elif detail["expanded"]:
                ready = (
                    "✅"
                    if detail["final_samples"] >= CORRELATION_MIN_SAMPLE_SIZE
                    else "⏳"
                )
                lines.append(
                    f"⏳ {name}: {detail['base_samples']} samples dalam "
                    f"{base_window_days} hari\n"
                    f"→ expanding window ke {max_window_days} hari...\n"
                    f"→ dapat {detail['final_samples']} samples {ready}"
                )

        if status == "insufficient_data":
            insufficient = ", ".join(
                f"{source}={count}"
                for source, count in source_samples.items()
                if count < CORRELATION_MIN_SAMPLE_SIZE
                and source not in skipped_sources
            )
            lines.append(f"Masih insufficient: {insufficient}")
        else:
            lines.append(f"Selesai: {significant} hasil signifikan.")
        return "\n".join(lines)
