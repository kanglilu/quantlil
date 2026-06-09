from __future__ import annotations

from decimal import Decimal

import pandas as pd

from agents.altdata_agent import AltDataAgent


def test_parse_fear_greed() -> None:
    row = AltDataAgent._parse_fear_greed(
        {
            "data": [
                {
                    "value": "72",
                    "value_classification": "Greed",
                    "timestamp": "1780876800",
                }
            ]
        }
    )

    assert row["source"] == "fear_greed"
    assert row["metric_name"] == "index"
    assert row["value"] == "72"
    assert row["label"] == "Greed"
    assert row["timestamp"] == "2026-06-08T00:00:00+00:00"


def test_parse_dxy() -> None:
    frame = pd.DataFrame(
        {"Close": [98.25, 98.75]},
        index=pd.to_datetime(["2026-06-05", "2026-06-08"]),
    )

    row = AltDataAgent._parse_dxy(frame)

    assert row == {
        "source": "dxy",
        "metric_name": "close",
        "timestamp": "2026-06-08T00:00:00+00:00",
        "value": "98.75",
        "label": None,
    }


def test_sum_polymarket_volume() -> None:
    total = AltDataAgent._sum_market_volume(
        [
            {"volume24hr": 10.5},
            {"volume24hr": "2.25"},
            {"volume24hr": None},
            {"volume24hr": "invalid"},
        ]
    )

    assert total == Decimal("12.75")

