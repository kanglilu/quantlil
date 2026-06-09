-- Quant Data Pipeline schema for Supabase/PostgreSQL

CREATE TABLE IF NOT EXISTS btc_ohlcv (
  id          bigserial PRIMARY KEY,
  symbol      text NOT NULL,
  interval    text NOT NULL,
  timestamp   timestamptz NOT NULL,
  open        numeric NOT NULL,
  high        numeric NOT NULL,
  low         numeric NOT NULL,
  close       numeric NOT NULL,
  volume      numeric NOT NULL,
  created_at  timestamptz DEFAULT now(),
  UNIQUE (symbol, interval, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_btc_ohlcv_symbol_interval_timestamp
  ON btc_ohlcv (symbol, interval, timestamp DESC);

CREATE TABLE IF NOT EXISTS alt_data (
  id          bigserial PRIMARY KEY,
  source      text NOT NULL,
  metric_name text NOT NULL,
  timestamp   timestamptz NOT NULL,
  value       numeric,
  label       text,
  created_at  timestamptz DEFAULT now(),
  UNIQUE (source, metric_name, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_alt_data_source_timestamp
  ON alt_data (source, timestamp DESC);

CREATE TABLE IF NOT EXISTS macro_data (
  id          bigserial PRIMARY KEY,
  source      text NOT NULL,
  metric_name text NOT NULL,
  timestamp   timestamptz NOT NULL,
  value       numeric,
  created_at  timestamptz DEFAULT now(),
  UNIQUE (source, metric_name, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_macro_data_source_timestamp
  ON macro_data (source, timestamp DESC);

CREATE TABLE IF NOT EXISTS correlation_results (
  id              bigserial PRIMARY KEY,
  dataset_a       text NOT NULL,
  dataset_b       text NOT NULL,
  lag_hours       integer NOT NULL,
  pearson_r       numeric NOT NULL,
  p_value         numeric NOT NULL,
  sample_size     integer NOT NULL,
  date_calculated date NOT NULL,
  created_at      timestamptz DEFAULT now(),
  UNIQUE (dataset_a, dataset_b, lag_hours, date_calculated)
);

CREATE INDEX IF NOT EXISTS idx_correlation_results_date_calculated
  ON correlation_results (date_calculated DESC);

CREATE TABLE IF NOT EXISTS hypothesis_log (
  id                bigserial PRIMARY KEY,
  hypothesis_text   text NOT NULL,
  timeframe         text,
  entry_condition   text,
  sl_pct            numeric,
  tp_pct            numeric,
  filter_condition  text,
  target_sample     integer DEFAULT 300,
  actual_sample     integer,
  win_rate          numeric,
  status            text DEFAULT 'DRAFT',
  notes             text,
  created_at        timestamptz DEFAULT now(),
  updated_at        timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS r2_objects (
  id            bigserial PRIMARY KEY,
  dataset       text NOT NULL,
  symbol        text,
  interval      text,
  object_key    text NOT NULL UNIQUE,
  min_timestamp timestamptz,
  max_timestamp timestamptz,
  row_count     bigint NOT NULL DEFAULT 0,
  size_bytes    bigint NOT NULL DEFAULT 0,
  etag          text,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_r2_objects_dataset_time
  ON r2_objects (dataset, max_timestamp DESC);
