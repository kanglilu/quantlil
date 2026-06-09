from __future__ import annotations

import asyncio
import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from analysis.correlation import CorrelationEngine
from config import get_settings


class FakeStore:
    def __init__(
        self,
        btc_rows: list[dict[str, Any]],
        alt_rows: list[dict[str, Any]],
    ) -> None:
        self.btc_rows = btc_rows
        self.alt_rows = alt_rows
        self.saved: list[dict[str, Any]] = []

    async def fetch_btc_closes_since(
        self, since: str, *, interval: str
    ) -> list[dict[str, Any]]:
        return self.btc_rows

    async def fetch_features_since(self, since: str) -> list[dict[str, Any]]:
        return self.alt_rows

    async def upsert_correlation_results(
        self, rows: list[dict[str, Any]]
    ) -> int:
        self.saved = rows
        return len(rows)


def test_engine_detects_alt_leading_btc_by_one_hour() -> None:
    rng = random.Random(42)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = [rng.uniform(0, 100) for _ in range(50)]

    alt_rows = [
        {
            "source": "test_source",
            "metric_name": "signal",
            "timestamp": (start + timedelta(hours=index)).isoformat(),
            "value": value,
            "label": None,
        }
        for index, value in enumerate(values)
    ]
    btc_rows = [
        {
            "symbol": "BTCUSDT",
            "interval": "15m",
            "timestamp": (
                start + timedelta(hours=index, minutes=45)
            ).isoformat(),
            "close": value,
        }
        for index, value in enumerate(values)
    ]

    store = FakeStore(btc_rows, alt_rows)
    settings = replace(
        get_settings(),
        correlation_lookback_days=365,
        correlation_btc_interval="15m",
    )
    engine = CorrelationEngine(settings=settings, store=store)  # type: ignore[arg-type]

    result = asyncio.run(engine.run())

    lag_one = [row for row in store.saved if row["lag_hours"] == 1]
    assert result["significant"] >= 1
    assert len(lag_one) == 1
    assert lag_one[0]["pearson_r"] > 0.99
    assert lag_one[0]["sample_size"] == 50


def test_engine_returns_informative_result_when_samples_insufficient() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    alt_rows = [
        {
            "source": "fear_greed",
            "metric_name": "index",
            "timestamp": (start + timedelta(days=index)).isoformat(),
            "value": index,
            "label": None,
        }
        for index in range(3)
    ]
    btc_rows = [
        {
            "symbol": "BTCUSDT",
            "interval": "15m",
            "timestamp": (start + timedelta(hours=index)).isoformat(),
            "close": 60_000 + index,
        }
        for index in range(10)
    ]
    store = FakeStore(btc_rows, alt_rows)
    settings = replace(
        get_settings(),
        correlation_lookback_days=365,
        correlation_btc_interval="15m",
    )
    engine = CorrelationEngine(settings=settings, store=store)  # type: ignore[arg-type]

    result = asyncio.run(engine.run())

    assert result["status"] == "insufficient_data"
    assert result["source_samples"] == {
        "fear_greed": 3,
        "dxy": 0,
        "polymarket": 0,
        "fred": 0,
        "google_trends": 0,
    }
    assert "Correlation adaptive window:" in result["message"]
    assert "expanding window ke 365 hari" in result["message"]
    assert "dapat 3 samples" in result["message"]
    assert "polymarket" not in result["insufficient_sources"]
    assert result["results"] == []
    assert store.saved == []


def test_adaptive_window_expands_weekly_and_skips_polymarket() -> None:
    now = datetime(2026, 6, 9, tzinfo=timezone.utc)
    weekly_index = [
        now - timedelta(days=7 * index)
        for index in range(52)
    ]
    features = {
        ("google_trends", "bitcoin"): pd.Series(
            range(52),
            index=pd.DatetimeIndex(weekly_index),
            name="alt_value",
        ).sort_index(),
        ("polymarket", "total_volume_24h"): pd.Series(
            [1, 2, 3],
            index=pd.DatetimeIndex(
                [now - timedelta(days=index) for index in range(3)]
            ),
            name="alt_value",
        ).sort_index(),
    }

    selected, details = CorrelationEngine._select_adaptive_windows(
        features,
        now=now,
        base_window_days=120,
        max_window_days=365,
    )

    trends = details["google_trends.bitcoin"]
    assert trends["base_samples"] < 30
    assert trends["final_samples"] == 52
    assert trends["expanded"] is True
    assert ("google_trends", "bitcoin") in selected

    polymarket = details["polymarket.total_volume_24h"]
    assert polymarket["final_samples"] == 3
    assert polymarket["skipped"] is True
    assert ("polymarket", "total_volume_24h") not in selected
