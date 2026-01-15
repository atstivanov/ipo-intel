
-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics_staging;
CREATE SCHEMA IF NOT EXISTS analytics_analytics;

-- Ingestion runs
CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
  run_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running',
  notes TEXT
);

-- IPO events (raw)
CREATE TABLE IF NOT EXISTS raw.ipo_events (
  event_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  ipo_date DATE NOT NULL,
  symbol TEXT,
  company_name TEXT NOT NULL,
  exchange TEXT,
  price_range_low NUMERIC,
  price_range_high NUMERIC,
  shares NUMERIC,
  raw_json JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Daily prices (raw)
CREATE TABLE IF NOT EXISTS raw.daily_prices (
  source TEXT NOT NULL,
  symbol TEXT NOT NULL,
  price_date DATE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  raw_row JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, symbol, price_date)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_ipo_events_symbol_date ON raw.ipo_events(symbol, ipo_date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date ON raw.daily_prices(symbol, price_date);

