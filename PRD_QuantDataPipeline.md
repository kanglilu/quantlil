
# PRD — Quant Data Pipeline & Monitor
**Version:** 0.1  
**Owner:** Lilu  
**Status:** Ready for development

---

## 1. Overview

Internal tool untuk mendukung aktivitas quant trading research. Bukan product publik — pure internal tooling untuk 1 user (Lilu). Tidak ada web UI, tidak ada auth system, tidak ada billing.

**Tujuan utama:**
- Fetch dan simpan data market + alternative data secara otomatis 24/7
- Deteksi korelasi antar dataset secara berkala dengan lag analysis
- Notifikasi hasil ke Telegram tanpa harus mantengin apapun
- Jadi fondasi data untuk backtest hipotesis trading short term

---

## 2. Problem Statement

Untuk membangun strategi quant trading berbasis statistik, dibutuhkan:
1. Data historis dan real-time yang bersih dan terstruktur
2. Proses korelasi otomatis antar dataset dengan lag analysis (bukan manual)
3. Notifikasi anomali atau temuan baru tanpa harus aktif monitoring
4. Controller sederhana untuk trigger proses manual via Telegram

---

## 3. Goals & Non-Goals

**Goals:**
- Data BTC OHLCV (1m, 5m, 15m) tersimpan otomatis setiap jam
- Alternative data (Fear/Greed, DXY, Polymarket, Google Trends, dll) terfetch terjadwal
- Lag analysis otomatis dihitung harian: 0, 1, 4, 6 jam
- Telegram bot kirim daily summary + alert anomali
- Telegram bot terima command manual dari Lilu
- Hypothesis log auto-sync ke Supabase (source of truth)

**Non-Goals:**
- Tidak ada web UI (gunakan Supabase Table Editor untuk lihat/edit data)
- Tidak ada multi-user / auth system
- Tidak ada auto-trading / order execution (fase ini)
- Tidak ada ML model (fase ini)
- Tidak ada Google Sheets / Excel sync (Supabase Table Editor cukup)

---

## 4. Infra Stack

### Compute
| Komponen | Stack | Alasan |
|---|---|---|
| Server utama | Oracle Cloud ARM A1 Free Tier | Gratis permanent, 4 core 24GB RAM |
| Runtime | Python 3.11 | Ekosistem data science terlengkap |
| Process manager | PM2 | Auto-restart jika crash atau reboot |

### Scheduler
| Komponen | Stack | Alasan |
|---|---|---|
| Job scheduler | APScheduler (in-process) | Ringan, tidak butuh Redis/Celery/worker terpisah |

### Storage
| Komponen | Stack | Alasan |
|---|---|---|
| Database utama | Supabase (PostgreSQL) | Sudah dipakai di Onaet, free tier cukup bertahun-tahun |
| UI data management | Supabase Table Editor | Tampilan spreadsheet langsung dari dashboard Supabase |

### Libraries Utama
```
supabase-py         → koneksi Supabase
python-binance      → fetch OHLCV BTC
yfinance            → Baltic Dry Index, DXY fallback
pytrends            → Google Trends
python-telegram-bot → bot notifikasi + controller
APScheduler         → cron jobs
pandas              → data processing
scipy               → korelasi Pearson + p-value
requests            → HTTP calls umum
python-dotenv       → env vars
```

---

## 5. Data Sources

| Data | Library/Source | Frekuensi | Fallback | Auth |
|---|---|---|---|---|
| BTC OHLCV (1m/5m/15m) | python-binance | Tiap 1 jam | — | Tidak perlu (public endpoint) |
| Fear & Greed Index | alternative.me API | Tiap 6 jam | — | Tidak perlu |
| DXY (Dollar Index) | yfinance (`DX-Y.NYB`) | Tiap 6 jam | stooq.com | Tidak perlu |
| Polymarket volume | Polymarket REST API | Tiap 6 jam | — | Tidak perlu |
| Google Trends (`bitcoin`) | pytrends | 1x/hari + random delay | — | Tidak perlu |
| FRED Macro (Treasury 10Y) | FRED API | 1x/hari | — | API key gratis |
| Baltic Dry Index | yfinance (`^BDI`) | 1x/hari | AI scraper agent | Tidak perlu |

### Catatan Baltic Dry Index:
```python
# Logic primary → fallback
try:
    bdi = yfinance.download("^BDI", period="2d")
    # gunakan data
except:
    # fallback: scraper agent mandiri
    bdi = scraper_agent.fetch_bdi()
```

### Catatan Google Trends:
- Interval minimum 1x/hari — jangan lebih sering, rawan rate limit
- Tambah `time.sleep(random.uniform(5, 15))` sebelum setiap request
- Keyword: `["bitcoin", "crypto", "buy bitcoin"]`

---

## 6. Fitur & Scope

### 6.1 Data Fetching Agents

**Price Agent** — tiap 1 jam
- Fetch OHLCV BTC/USDT dari Binance (interval: 1m, 5m, 15m)
- Ambil 60 candle terakhir per fetch
- Deduplikasi berdasarkan `(symbol, interval, timestamp)` sebelum insert
- Simpan ke tabel `btc_ohlcv`

**Alternative Data Agent** — tiap 6 jam
- Fear & Greed Index (nilai numerik + label: Extreme Fear/Fear/Neutral/Greed/Extreme Greed)
- DXY close (yfinance primary, stooq fallback)
- Polymarket total volume harian
- Simpan ke tabel `alt_data`

**Macro Data Agent** — 1x/hari jam 06.00 UTC
- FRED: US Treasury Yield 10Y (`DGS10`)
- Google Trends: keyword `bitcoin`, `crypto`, `buy bitcoin`
- Baltic Dry Index: yfinance primary → scraper fallback
- Simpan ke tabel `macro_data`

---

### 6.2 Analysis Engine

**Lag Correlation Analysis** — 1x/hari jam 07.00 UTC

Hitung Pearson correlation antara setiap pasangan dataset dengan 4 lag:

```
Lag 0  → korelasi simultan (paling relevan buat scalping)
Lag 1  → lead/lag 1 jam
Lag 4  → lead/lag 4 jam
Lag 6  → lead/lag 6 jam
```

Filter hasil: simpan hanya jika `|r| > 0.3` dan `p-value < 0.05` dan `sample_size >= 30`

Simpan ke tabel `correlation_results`

**Anomaly Detection** — realtime (cek tiap 15 menit)

| Kondisi | Alert |
|---|---|
| Data tidak masuk > 2 jam | Stale data alert |
| BTC bergerak > 3% dalam 1 jam | Price spike alert |
| Fear & Greed berubah kategori | Sentiment shift alert |
| Baltic Dry berubah > 2% harian | BDI movement alert |

---

### 6.3 Telegram Bot

**Daily summary** — push otomatis jam 07.30 UTC:
```
📊 Daily Report — 9 Jun 2026

BTC: $105,420 (+1.2% 24h)
Fear & Greed: 72 — Greed
DXY: 103.4 (-0.3%)
BDI: 1,842 (+0.5%)

🔗 Korelasi signifikan hari ini:
→ DXY vs BTC lag 0  (r=-0.71, p=0.002) ✅
→ Polymarket vs BTC lag 1 (r=0.54, p=0.018) ✅
→ Google Trends vs BTC lag 4 (r=0.61, p=0.009) ✅

📋 Hipotesis aktif: 3
✅ Valid: 1 | 🧪 Testing: 2 | ❌ Gugur: 1
```

**Commands (controller manual):**
```
/status          → cek semua agent running / error
/fetch           → trigger manual fetch semua data sekarang
/correlation     → jalankan lag correlation sekarang
/latest          → 5 data terbaru per source
/hypothesis      → list semua hipotesis dari Supabase
/alert [on|off]  → toggle alert sementara
/help            → list semua command
```

---

### 6.4 Hypothesis Auto-Sync

**Source of truth: Supabase tabel `hypothesis_log`**

- Tidak ada sync ke Excel atau Google Sheets
- UI untuk lihat dan edit: **Supabase Table Editor** (supabase.com/dashboard)
- Tampilan persis spreadsheet — bisa filter, sort, edit langsung
- Telegram command `/hypothesis` bisa lihat list ringkas dari HP

---

## 7. Database Schema (Supabase)

```sql
-- Tabel 1: Price data
CREATE TABLE btc_ohlcv (
  id          bigserial PRIMARY KEY,
  symbol      text NOT NULL,           -- 'BTCUSDT'
  interval    text NOT NULL,           -- '1m', '5m', '15m'
  timestamp   timestamptz NOT NULL,
  open        numeric NOT NULL,
  high        numeric NOT NULL,
  low         numeric NOT NULL,
  close       numeric NOT NULL,
  volume      numeric NOT NULL,
  created_at  timestamptz DEFAULT now(),
  UNIQUE (symbol, interval, timestamp)
);
CREATE INDEX ON btc_ohlcv (symbol, interval, timestamp DESC);

-- Tabel 2: Alternative data
CREATE TABLE alt_data (
  id          bigserial PRIMARY KEY,
  source      text NOT NULL,           -- 'fear_greed', 'dxy', 'polymarket'
  metric_name text NOT NULL,
  timestamp   timestamptz NOT NULL,
  value       numeric,
  label       text,                    -- opsional, untuk Fear/Greed kategori
  created_at  timestamptz DEFAULT now(),
  UNIQUE (source, metric_name, timestamp)
);
CREATE INDEX ON alt_data (source, timestamp DESC);

-- Tabel 3: Macro data
CREATE TABLE macro_data (
  id          bigserial PRIMARY KEY,
  source      text NOT NULL,           -- 'fred', 'google_trends', 'bdi'
  metric_name text NOT NULL,
  timestamp   timestamptz NOT NULL,
  value       numeric,
  created_at  timestamptz DEFAULT now(),
  UNIQUE (source, metric_name, timestamp)
);
CREATE INDEX ON macro_data (source, timestamp DESC);

-- Tabel 4: Correlation results
CREATE TABLE correlation_results (
  id              bigserial PRIMARY KEY,
  dataset_a       text NOT NULL,
  dataset_b       text NOT NULL,
  lag_hours       integer NOT NULL,    -- 0, 1, 4, 6
  pearson_r       numeric NOT NULL,
  p_value         numeric NOT NULL,
  sample_size     integer NOT NULL,
  date_calculated date NOT NULL,
  created_at      timestamptz DEFAULT now(),
  UNIQUE (dataset_a, dataset_b, lag_hours, date_calculated)
);
CREATE INDEX ON correlation_results (date_calculated DESC);

-- Tabel 5: Hypothesis log (source of truth)
CREATE TABLE hypothesis_log (
  id                bigserial PRIMARY KEY,
  hypothesis_text   text NOT NULL,
  timeframe         text,              -- '1m', '5m', '15m'
  entry_condition   text,
  sl_pct            numeric,
  tp_pct            numeric,
  filter_condition  text,
  target_sample     integer DEFAULT 300,
  actual_sample     integer,
  win_rate          numeric,
  status            text DEFAULT 'DRAFT',
  -- DRAFT | TESTING | VALID | GUGUR | PAUSE
  notes             text,
  created_at        timestamptz DEFAULT now(),
  updated_at        timestamptz DEFAULT now()
);
```

---

## 8. File Structure

```
quant-pipeline/
├── main.py                  ← entry point, init semua
├── config.py                ← env vars, konstanta, lag settings
├── scheduler.py             ← APScheduler setup + job registry
│
├── agents/
│   ├── price_agent.py       ← Binance OHLCV fetcher
│   ├── altdata_agent.py     ← Fear/Greed, DXY, Polymarket
│   └── macro_agent.py       ← FRED, Google Trends, BDI (+ fallback scraper)
│
├── analysis/
│   ├── correlation.py       ← Pearson + p-value, lag 0/1/4/6
│   └── anomaly.py           ← alert detection, threshold checks
│
├── storage/
│   └── supabase_client.py   ← insert/query/upsert helpers
│
├── bot/
│   └── telegram_bot.py      ← commands + daily report formatter
│
├── requirements.txt
├── .env                     ← API keys — jangan di-commit
└── .env.example             ← template kosong untuk referensi
```

---

## 9. Environment Variables

```env
# Supabase
SUPABASE_URL=
SUPABASE_SERVICE_KEY=

# Telegram
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# FRED (daftar gratis di fred.stlouisfed.org)
FRED_API_KEY=

# OpenRouter (fase 2 — opsional sekarang)
OPENROUTER_API_KEY=
```

---

## 10. Deployment di Oracle ARM

```bash
# 1. Clone repo ke Oracle
git clone <repo> && cd quant-pipeline

# 2. Install dependencies
pip3 install -r requirements.txt

# 3. Setup env
cp .env.example .env
nano .env  # isi API keys

# 4. Test run manual dulu
python3 main.py

# 5. Deploy dengan PM2 (auto-restart)
npm install -g pm2
pm2 start main.py --interpreter python3 --name quant-pipeline
pm2 save
pm2 startup
```

---

## 11. Phases

### Phase 1 — Fondasi (build sekarang)
- [ ] Supabase schema setup (5 tabel + index)
- [ ] Price agent (Binance OHLCV)
- [ ] Supabase insert helpers + deduplikasi
- [ ] Telegram bot basic (daily report + /status + /fetch)
- [ ] APScheduler setup
- [ ] Deploy ke Oracle + PM2

### Phase 2 — Full Data Pipeline
- [ ] Alternative data agents (Fear/Greed, DXY, Polymarket)
- [ ] Macro data agents (FRED, Google Trends, BDI + fallback)
- [ ] Lag correlation engine (0, 1, 4, 6 jam)
- [ ] Anomaly detection + alert
- [ ] Full Telegram commands

### Phase 3 — LLM Reasoning
- [ ] OpenRouter integration (Hermes)
- [ ] Daily hypothesis suggestion dari LLM
- [ ] Auto-update status hipotesis berdasarkan korelasi baru

---

## 12. Decisions Log

| Pertanyaan | Keputusan | Alasan |
|---|---|---|
| Baltic Dry source | yfinance primary, scraper fallback | Gratis, auto-fallback kalau limit |
| Google Trends | Include, 1x/hari + random delay | Data sentimen penting, rate limit dihindari dengan interval panjang |
| Lag analysis | 0, 1, 4, 6 jam | Relevan untuk short term — 24 jam terlalu panjang |
| Hypothesis storage | Supabase Table Editor | Auto-sync, no manual, UI cukup dari dashboard Supabase |
| Web UI | Tidak ada | Internal tool — Supabase dashboard + Telegram sudah cukup |
| Excel / Google Sheets | Tidak dipakai | Supabase Table Editor menggantikan fungsi yang sama |
