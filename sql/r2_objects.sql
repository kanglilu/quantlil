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
