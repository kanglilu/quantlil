from __future__ import annotations

import asyncio

import pandas as pd

from agents.macro_agent import (
    GOOGLE_TRENDS_RETRY_DELAYS,
    MacroAgent,
)


def test_parse_fred_latest_valid_observation() -> None:
    row = MacroAgent._parse_fred_csv(
        "observation_date,DGS10\n"
        "2026-06-05,4.50\n"
        "2026-06-08,.\n"
        "2026-06-09,4.47\n"
    )

    assert row == {
        "source": "fred",
        "metric_name": "treasury_10y",
        "timestamp": "2026-06-09T00:00:00+00:00",
        "value": "4.47",
    }


def test_parse_google_trends_latest_values() -> None:
    frame = pd.DataFrame(
        {
            "bitcoin": [70, 72],
            "isPartial": [False, True],
        },
        index=pd.to_datetime(
            ["2026-06-09T00:00:00Z", "2026-06-09T01:00:00Z"]
        ),
    )

    row = MacroAgent._parse_google_trend_keyword(frame, "bitcoin")

    assert row == {
        "source": "google_trends",
        "metric_name": "bitcoin",
        "timestamp": "2026-06-09T01:00:00+00:00",
        "value": "72",
    }


def test_google_trends_retries_then_returns_nullable_skip(
    monkeypatch,
) -> None:
    agent = MacroAgent(store=object())  # type: ignore[arg-type]
    sleeps: list[float] = []
    attempts = 0

    async def fail_request(keyword: str) -> pd.DataFrame:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("Google returned 429")

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(agent, "_request_google_trend", fail_request)
    monkeypatch.setattr("agents.macro_agent.asyncio.sleep", fake_sleep)

    row, result = asyncio.run(
        agent._fetch_google_trend_keyword("bitcoin")
    )

    assert attempts == 4
    assert sleeps == list(GOOGLE_TRENDS_RETRY_DELAYS)
    assert result["status"] == "skip"
    assert row["source"] == "google_trends"
    assert row["metric_name"] == "bitcoin"
    assert row["value"] is None


def test_google_trends_waits_between_keywords(monkeypatch) -> None:
    agent = MacroAgent(store=object())  # type: ignore[arg-type]
    sleeps: list[float] = []

    async def fake_fetch(keyword: str):
        row = {
            "source": "google_trends",
            "metric_name": keyword,
            "timestamp": "2026-06-09T00:00:00+00:00",
            "value": "10",
        }
        return row, {"status": "ok", "row": row}

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("agents.macro_agent.random.uniform", lambda *_: 8)
    monkeypatch.setattr("agents.macro_agent.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(agent, "_fetch_google_trend_keyword", fake_fetch)

    rows, metrics = asyncio.run(agent.fetch_google_trends())

    assert sleeps == [8, 30]
    assert [row["metric_name"] for row in rows] == ["bitcoin", "crypto"]
    assert metrics["bitcoin"]["status"] == "ok"
    assert metrics["crypto"]["status"] == "ok"


def test_parse_bdi_latest_close() -> None:
    frame = pd.DataFrame(
        {"Close": [1842.5, 1850.25]},
        index=pd.to_datetime(["2026-06-08", "2026-06-09"]),
    )

    row = MacroAgent._parse_bdi(frame)

    assert row == {
        "source": "bdi",
        "metric_name": "close",
        "timestamp": "2026-06-09T00:00:00+00:00",
        "value": "1850.25",
    }
