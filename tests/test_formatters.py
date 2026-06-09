from __future__ import annotations

from bot.telegram_bot import TelegramController


def test_format_fetch_result() -> None:
    result = {
        "price": {
            "1m": {"fetched": 60, "upserted": 60},
            "5m": {"fetched": 60, "upserted": 55},
        }
    }

    text = TelegramController._format_fetch_result(result)

    assert "Fetch selesai:" in text
    assert "- 1m: fetched 60, upserted 60" in text
    assert "- 5m: fetched 60, upserted 55" in text


def test_format_correlation_insufficient_data() -> None:
    text = TelegramController._format_correlation_result(
        {
            "status": "insufficient_data",
            "message": (
                "Insufficient data untuk sebagian source: butuh minimal "
                "30 samples dalam window 120 hari. Saat ini: "
                "fear_greed=1, dxy=1, polymarket=1, fred=1, "
                "google_trends=1"
            ),
        }
    )

    assert text == (
        "Insufficient data untuk sebagian source: butuh minimal "
        "30 samples dalam window 120 hari. Saat ini: "
        "fear_greed=1, dxy=1, polymarket=1, fred=1, google_trends=1"
    )


def test_correlation_handler_is_registered() -> None:
    from config import get_settings

    class FakeScheduler:
        def set_daily_summary_callback(self, callback: object) -> None:
            self.callback = callback

        def set_price_heartbeat_callback(self, callback: object) -> None:
            self.heartbeat_callback = callback

        def set_alt_data_heartbeat_callback(self, callback: object) -> None:
            self.alt_heartbeat_callback = callback

    controller = TelegramController(
        settings=get_settings(),
        scheduler=FakeScheduler(),  # type: ignore[arg-type]
    )

    assert controller.application is not None
    commands = {
        command
        for handlers in controller.application.handlers.values()
        for handler in handlers
        for command in getattr(handler, "commands", set())
    }
    assert "correlation" in commands
    assert "samples" in commands


def test_format_sample_counts() -> None:
    text = TelegramController._format_sample_counts(
        {
            "alt_data": {
                "fear_greed": 120,
                "dxy": 83,
                "polymarket": 1,
            },
            "macro_data": {
                "treasury_10y": 497,
                "google_trends_btc": 262,
                "google_trends_crypto": 262,
                "bdi": 0,
            },
            "btc_ohlcv": {
                "1m": 1_051_201,
                "5m": 210_240,
                "15m": 70_080,
            },
        }
    )

    assert "📊 Data Samples per Source:" in text
    assert "→ polymarket  : 1 sample ⏳" in text
    assert "→ google_trends_btc    : 262 samples" in text
    assert "→ bdi                  : 0 (skip)" in text
    assert "→ 1m  : 1,051,201 candles" in text


def test_format_price_heartbeat() -> None:
    text = TelegramController._format_price_heartbeat(
        {
            "1m": {"status": "ok", "fetched": 60, "upserted": 60},
            "5m": {"status": "ok", "fetched": 60, "upserted": 60},
            "15m": {"status": "ok", "fetched": 60, "upserted": 60},
            "finished_at": "2026-06-09T10:00:12+00:00",
        }
    )

    assert text == "✓ 10:00 — price fetched (180 candles)"


def test_price_heartbeat_skips_when_no_rows_upserted() -> None:
    text = TelegramController._format_price_heartbeat(
        {
            "1m": {"status": "ok", "fetched": 60, "upserted": 0},
            "5m": {"status": "error", "fetched": 0, "upserted": 0},
            "finished_at": "2026-06-09T10:00:12+00:00",
        }
    )

    assert text is None


def test_format_alt_data_heartbeat() -> None:
    text = TelegramController._format_alt_data_heartbeat(
        {
            "fear_greed": {"status": "ok"},
            "dxy": {"status": "ok"},
            "polymarket": {"status": "ok"},
            "finished_at": "2026-06-09T06:11:55+00:00",
        }
    )

    assert text == (
        "✓ 06:11 UTC / 13:11 WIB — alt data fetched "
        "(fear_greed=ok, dxy=ok, polymarket=ok)"
    )


def test_format_fetch_result_includes_macro_statuses() -> None:
    text = TelegramController._format_fetch_result(
        {
            "macro_data": {
                "treasury_10y": {
                    "status": "ok",
                    "row": {"value": "4.47"},
                },
                "google_trends": {
                    "status": "ok",
                    "metrics": {
                        "bitcoin": {
                            "status": "ok",
                            "row": {"value": "72"},
                        },
                        "crypto": {
                            "status": "error",
                            "error": "rate limited",
                        },
                    },
                },
                "bdi": {"status": "skip", "error": "no data"},
                "upserted": 2,
            }
        }
    )

    assert "Macro data:" in text
    assert "- treasury_10y: 4.47 (ok)" in text
    assert "- google_trends bitcoin: 72 (ok)" in text
    assert "- google_trends crypto: rate limited (error)" in text
    assert "- bdi: unavailable (skip)" in text
