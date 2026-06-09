# Quant Data Pipeline

Internal quant research daemon for fetching BTC market data, storing it in Supabase, and controlling the pipeline from Telegram.

## Implemented

- Supabase schema in `sql/schema.sql`
- BTC OHLCV price agent for `BTCUSDT` intervals `1m`, `5m`, `15m`
- Alternative-data agents:
  - Alternative.me Fear & Greed Index
  - DXY daily close from Yahoo Finance (`DX-Y.NYB`)
  - Polymarket aggregate rolling 24h volume
- Macro-data agents:
  - FRED US Treasury Yield 10Y (`DGS10`)
  - Google Trends for `bitcoin` and `crypto` using `today 3-m`
  - Baltic Dry Index from Yahoo Finance when available
- Pearson lag correlation for `0`, `1`, `4`, and `6` hours
- Supabase upsert helper with `(symbol, interval, timestamp)` dedupe
- APScheduler jobs:
  - price fetch and Telegram heartbeat at minute `00` every hour UTC
  - alternative-data fetch at 00:10, 06:10, 12:10, and 18:10 UTC
    with a Telegram heartbeat after the fetch completes
  - correlation analysis daily at 07:00 UTC
  - macro-data fetch daily at 06:00 UTC
  - daily summary at 07:30 UTC
- Telegram commands:
  - `/status`
  - `/fetch`
  - `/correlation`
  - `/latest`
  - `/alert on|off`
  - `/help`

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Fill `.env`, then create the database tables by running `sql/schema.sql` in Supabase SQL Editor.

## Run

```bash
python main.py
```

If Supabase or Telegram env vars are empty, the app still starts with those integrations disabled.

## Smoke Test

Run this after editing `.env` or after deploying to a new server:

```bash
python -m scripts.smoke_test
python -m scripts.smoke_test --fetch
python -m scripts.smoke_test --alt
python -m scripts.smoke_test --macro
python -m scripts.smoke_test --r2
python -m scripts.smoke_test --correlation
python -m pytest
```

`--fetch` writes one batch of `BTCUSDT` 1m candles to Supabase.
`--alt` scans all active Polymarket markets and can take around two minutes.
Google Trends requests run separately per keyword, wait 30 seconds between
keywords, and back off for 30/60/120 seconds when rate limited.
The R2 smoke test uploads one small Parquet object under `_smoke/`, reads it
back, verifies it, and deletes it automatically.

Before enabling R2 writes in the pipeline, run `sql/r2_objects.sql` in the
Supabase SQL Editor. This table stores only the Parquet object catalog and
file statistics; raw candles remain in R2.

## R2 Data Lake Layout

Scheduled fetches use dual-write:

- Supabase keeps operational rows used by Telegram and the current correlation
  engine.
- R2 keeps daily Zstandard-compressed Parquet files.
- Supabase `r2_objects` catalogs every Parquet file.

Current object layout:

```text
raw/market/btc_ohlcv/symbol=BTCUSDT/interval=1m/year=YYYY/month=MM/day=DD/data.parquet
raw/alternative/fear_greed/metric=index/year=YYYY/month=MM/day=DD/data.parquet
raw/alternative/dxy/metric=close/year=YYYY/month=MM/day=DD/data.parquet
raw/alternative/polymarket/metric=total_volume_24h/year=YYYY/month=MM/day=DD/data.parquet
raw/macro/fred/metric=treasury_10y/year=YYYY/month=MM/day=DD/data.parquet
raw/macro/google_trends/metric=bitcoin/year=YYYY/month=MM/day=DD/data.parquet
```

Daily files are merged and deduplicated when the same partition is fetched
again. An R2 failure is reported in the agent result but does not cancel the
existing Supabase write.

DuckDB belongs to the read/analysis layer. It is introduced after this file
contract is stable, then used to scan the partitioned Parquet files for
feature generation and correlation without loading raw rows into Supabase.

## One-Time Historical Backfill

Run all historical sources into R2:

```bash
python backfill.py
```

The default BTC import covers 730 days for `1m`, `5m`, and `15m`, totaling
roughly 1.33 million candle rows. Raw rows go only to partitioned R2 Parquet
files. Supabase receives only `r2_objects` catalog metadata.

Run or retry selected sources:

```bash
python backfill.py --sources fear_greed dxy treasury_10y
python backfill.py --sources google_trends
python backfill.py --sources btc
```

Every source is isolated: one failure is reported in the final summary while
the remaining sources continue. Deterministic object keys and row-level
deduplication make reruns safe, although a rerun repeats API and R2 work.

## Correlation Semantics

- BTC `15m` candles are converted to hourly close values.
- Alternative data is dataset A and BTC is dataset B.
- Lag `1` compares alt data at time T with BTC close at T+1 hour.
- The default lookback is 120 days.
- Results are stored only when `|r| > 0.3`, `p < 0.05`, and sample size is at least 30.
- Configure the lookback and BTC source interval with:

```env
CORRELATION_LOOKBACK_DAYS=120
CORRELATION_BTC_INTERVAL=15m
```

## Oracle + PM2

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip git npm
python3.11 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
cp .env.example .env
python3 main.py
pm2 start ecosystem.config.js
pm2 save
pm2 startup
```

After `pm2 startup`, run the command printed by PM2, then reboot once and check:

```bash
pm2 status
pm2 logs quant-pipeline
```

## Production Notes

- Runtime logs are written to `logs/quant-pipeline.log`.
- PM2 stdout/stderr logs are written to `logs/pm2-out.log` and `logs/pm2-error.log`.
- Network calls use retry/backoff based on `RETRY_ATTEMPTS` and `RETRY_BASE_DELAY`.
- Keep `.env` only on the server; it is ignored by git.
