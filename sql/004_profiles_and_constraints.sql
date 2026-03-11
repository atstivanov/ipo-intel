-- 1) company_profiles: add columns we need
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='raw' AND table_name='company_profiles' AND column_name='source'
  ) THEN
    ALTER TABLE raw.company_profiles ADD COLUMN source text;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='raw' AND table_name='company_profiles' AND column_name='sector'
  ) THEN
    ALTER TABLE raw.company_profiles ADD COLUMN sector text;
  END IF;
END $$;

-- 2) Backfill source for existing rows (assume finnhub if missing)
UPDATE raw.company_profiles
SET source = COALESCE(source, 'finnhub')
WHERE source IS NULL;

-- 3) Enforce uniqueness for multi-source profiles
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'company_profiles_source_symbol_uk'
  ) THEN
    ALTER TABLE raw.company_profiles
      ADD CONSTRAINT company_profiles_source_symbol_uk
      UNIQUE (source, symbol);
  END IF;
END $$;

-- 4) daily_prices unique (you already rely on this in upsert_daily_prices)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'daily_prices_source_symbol_date_uk'
  ) THEN
    ALTER TABLE raw.daily_prices
      ADD CONSTRAINT daily_prices_source_symbol_date_uk
      UNIQUE (source, symbol, price_date);
  END IF;
END $$;

-- 5) symbol_map unique (you already rely on this in upsert_symbol_map)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'symbol_map_vendor_ipo_symbol_uk'
  ) THEN
    ALTER TABLE raw.symbol_map
      ADD CONSTRAINT symbol_map_vendor_ipo_symbol_uk
      UNIQUE (vendor, ipo_symbol);
  END IF;
END $$;
