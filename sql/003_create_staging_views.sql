CREATE OR REPLACE VIEW staging.ipos AS
SELECT
  ipo_date,
  symbol,
  company_name,
  exchange,
  price_range_low,
  price_range_high,
  shares
FROM raw.ipo_events;

CREATE OR REPLACE VIEW staging.prices AS
SELECT
  symbol,
  price_date,
  open, high, low, close, volume
FROM raw.daily_prices;
