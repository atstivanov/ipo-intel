-- Track each ingestion execution
CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
  run_id           BIGSERIAL PRIMARY KEY,
  source           TEXT NOT NULL,
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  status           TEXT NOT NULL DEFAULT 'running',
  notes            TEXT
);

-- IPO events (one row per IPO event from Finnhub)
CREATE TABLE IF NOT EXISTS raw.ipo_events (
  source           TEXT NOT NULL DEFAULT 'finnhub',
  ipo_date         DATE NOT NULL,
  symbol           TEXT,
  company_name     TEXT,
  exchange         TEXT,
  price_range_low  NUMERIC,
  price_range_high NUMERIC,
  shares           BIGINT,
  raw_json         JSONB,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, ipo_date, symbol, company_name)
);

-- Daily prices (OHLCV)
CREATE TABLE IF NOT EXISTS raw.daily_prices (
  source           TEXT NOT NULL DEFAULT 'stooq',
  symbol           TEXT NOT NULL,
  price_date       DATE NOT NULL,
  open             NUMERIC,
  high             NUMERIC,
  low              NUMERIC,
  close            NUMERIC,
  volume           BIGINT,
  raw_row          JSONB,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, symbol, price_date)
);
