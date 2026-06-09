from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from dotenv import load_dotenv


load_dotenv()


SYMBOL: Final[str] = "BTCUSDT"
PRICE_INTERVALS: Final[tuple[str, ...]] = ("1m", "5m", "15m")
PRICE_FETCH_LIMIT: Final[int] = 60
LAG_HOURS: Final[tuple[int, ...]] = (0, 1, 4, 6)
CORRELATION_MIN_ABS_R: Final[float] = 0.3
CORRELATION_MAX_P_VALUE: Final[float] = 0.05
CORRELATION_MIN_SAMPLE_SIZE: Final[int] = 30


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_key: str
    telegram_bot_token: str
    telegram_chat_id: str
    fred_api_key: str
    openrouter_api_key: str
    r2_endpoint: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_bucket: str
    r2_region: str
    log_level: str
    log_file: str
    timezone: str
    retry_attempts: int
    retry_base_delay: float
    correlation_lookback_days: int
    correlation_btc_interval: str

    @property
    def supabase_enabled(self) -> bool:
        return bool(self.supabase_url and self.supabase_service_key)

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def r2_enabled(self) -> bool:
        return bool(
            self.r2_endpoint
            and self.r2_access_key_id
            and self.r2_secret_access_key
            and self.r2_bucket
        )


def get_settings() -> Settings:
    return Settings(
        supabase_url=os.getenv("SUPABASE_URL", "").strip(),
        supabase_service_key=os.getenv("SUPABASE_SERVICE_KEY", "").strip(),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        fred_api_key=os.getenv("FRED_API_KEY", "").strip(),
        openrouter_api_key=os.getenv("OPENROUTER_API_KEY", "").strip(),
        r2_endpoint=os.getenv("R2_ENDPOINT", "").strip().rstrip("/"),
        r2_access_key_id=os.getenv("R2_ACCESS_KEY_ID", "").strip(),
        r2_secret_access_key=os.getenv(
            "R2_SECRET_ACCESS_KEY", ""
        ).strip(),
        r2_bucket=os.getenv("R2_BUCKET", "").strip(),
        r2_region=os.getenv("R2_REGION", "auto").strip() or "auto",
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        log_file=os.getenv("LOG_FILE", "logs/quant-pipeline.log").strip(),
        timezone=os.getenv("TZ", "UTC").strip() or "UTC",
        retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
        retry_base_delay=float(os.getenv("RETRY_BASE_DELAY", "1.0")),
        correlation_lookback_days=int(
            os.getenv("CORRELATION_LOOKBACK_DAYS", "120")
        ),
        correlation_btc_interval=os.getenv(
            "CORRELATION_BTC_INTERVAL", "15m"
        ).strip(),
    )
