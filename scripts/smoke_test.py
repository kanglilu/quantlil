from __future__ import annotations

import argparse
import asyncio

from agents.price_agent import PriceAgent
from agents.altdata_agent import AltDataAgent
from agents.macro_agent import MacroAgent
from analysis.correlation import CorrelationEngine
from config import get_settings
from storage.data_lake_reader import DataLakeReader
from storage.supabase_client import SupabaseStore
from storage.r2_client import R2Store


async def run(
    fetch: bool,
    alt: bool,
    macro: bool,
    correlation: bool,
    r2: bool,
) -> int:
    settings = get_settings()
    print(f"supabase_enabled={settings.supabase_enabled}")
    print(f"telegram_enabled={settings.telegram_enabled}")
    print(f"r2_enabled={settings.r2_enabled}")
    print(f"timezone={settings.timezone}")

    store = SupabaseStore(settings)
    latest = await store.latest_btc_ohlcv(limit=1)
    print(f"supabase_query_ok=True latest_rows={len(latest)}")

    if fetch:
        result = await PriceAgent(store).fetch_interval("1m")
        print(f"binance_fetch_ok=True result={result}")

    if alt:
        result = await AltDataAgent(store).fetch_all()
        print(f"alt_data_fetch_ok=True result={result}")

    if macro:
        result = await MacroAgent(store).fetch_all()
        print(f"macro_data_fetch_ok=True result={result}")

    if correlation:
        data_lake = None
        if settings.r2_enabled:
            data_lake = DataLakeReader(R2Store(settings), store)
        result = await CorrelationEngine(
            settings,
            store,
            data_lake=data_lake,
        ).run()
        print(f"correlation_ok=True result={result}")

    if r2:
        result = await R2Store(settings).smoke_test()
        print(f"r2_smoke_ok=True result={result}")

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test Quant Pipeline")
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Also fetch and upsert one batch of BTCUSDT 1m candles.",
    )
    parser.add_argument(
        "--alt",
        action="store_true",
        help="Fetch and upsert all alternative data sources.",
    )
    parser.add_argument(
        "--macro",
        action="store_true",
        help="Fetch and upsert FRED, Google Trends, and BDI.",
    )
    parser.add_argument(
        "--r2",
        action="store_true",
        help="Upload, read, and delete a small Parquet object in R2.",
    )
    parser.add_argument(
        "--correlation",
        action="store_true",
        help="Run lag correlation and upsert significant results.",
    )
    args = parser.parse_args()
    raise SystemExit(
        asyncio.run(
            run(
                fetch=args.fetch,
                alt=args.alt,
                macro=args.macro,
                correlation=args.correlation,
                r2=args.r2,
            )
        )
    )


if __name__ == "__main__":
    main()
