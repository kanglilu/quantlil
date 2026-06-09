from __future__ import annotations

import pandas as pd

from backfill import BackfillRunner


def test_parse_fear_greed_history_uses_utc() -> None:
    rows = BackfillRunner._parse_fear_greed_history(
        {
            "data": [
                {
                    "value": "10",
                    "value_classification": "Extreme Fear",
                    "timestamp": "1780963200",
                },
                {
                    "value": "12",
                    "value_classification": "Extreme Fear",
                    "timestamp": "1780876800",
                },
            ]
        }
    )

    assert len(rows) == 2
    assert rows[0] == {
        "source": "fear_greed",
        "metric_name": "index",
        "timestamp": "2026-06-09T00:00:00+00:00",
        "value": "10",
        "label": "Extreme Fear",
    }


def test_parse_fred_history_skips_missing_values() -> None:
    rows = BackfillRunner._parse_fred_history(
        "observation_date,DGS10\n"
        "2026-06-05,4.55\n"
        "2026-06-08,.\n"
        "2026-06-09,4.50\n"
    )

    assert [row["timestamp"] for row in rows] == [
        "2026-06-05T00:00:00+00:00",
        "2026-06-09T00:00:00+00:00",
    ]
    assert [row["value"] for row in rows] == ["4.55", "4.5"]


def test_parse_google_trends_history_uses_utc() -> None:
    frame = pd.DataFrame(
        {"bitcoin": [40, 45]},
        index=pd.to_datetime(["2026-06-01", "2026-06-08"]),
    )

    rows = BackfillRunner._parse_google_trends_history(frame, "bitcoin")

    assert rows == [
        {
            "source": "google_trends",
            "metric_name": "bitcoin",
            "timestamp": "2026-06-01T00:00:00+00:00",
            "value": "40",
        },
        {
            "source": "google_trends",
            "metric_name": "bitcoin",
            "timestamp": "2026-06-08T00:00:00+00:00",
            "value": "45",
        },
    ]


def test_backfill_parsers_produce_data_lake_contract() -> None:
    frame = pd.DataFrame(
        {"Close": [99.5]},
        index=pd.to_datetime(["2026-06-09"]),
    )

    rows = BackfillRunner._parse_market_history(
        frame,
        source="dxy",
        metric_name="close",
        include_label=True,
    )

    assert rows == [
        {
            "source": "dxy",
            "metric_name": "close",
            "timestamp": "2026-06-09T00:00:00+00:00",
            "value": "99.5",
            "label": None,
        }
    ]
