# app.py
# Streamlit dashboard for IPO Intel (Postgres + dbt marts)
# Run:
#   pip install streamlit pandas sqlalchemy psycopg2-binary python-dotenv
#   streamlit run app.py
#
# Requires env vars (same as your pipeline):
#   PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB
# Optional:
#   DBT_SCHEMA_ANALYTICS=analytics_analytics
#   DBT_SCHEMA_STAGING=analytics_staging
#   RECENT_DAYS=120

from __future__ import annotations

import os
from datetime import date
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ---------- Helpers ----------
def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return v.strip()

def get_engine():
    host = _env("PG_HOST", "localhost")
    port = _env("PG_PORT", "5432")
    user = _env("PG_USER", "postgres")
    pw = _env("PG_PASSWORD", "")
    db = _env("PG_DB", "postgres")
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def q(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def pct(n: float, d: float) -> float:
    return 0.0 if d == 0 else (n / d) * 100.0

# ---------- Streamlit ----------
st.set_page_config(page_title="IPO Intel", layout="wide")
st.title("IPO Intel — Coverage & Performance (MVP)")

engine = get_engine()

schema_analytics = _env("DBT_SCHEMA_ANALYTICS", "analytics_analytics")
schema_staging = _env("DBT_SCHEMA_STAGING", "analytics_staging")

# Sidebar controls
with st.sidebar:
    st.header("Filters")
    recent_days = st.number_input("Recent IPO window (days)", min_value=30, max_value=365, value=int(_env("RECENT_DAYS", "120")))
    min_trading_days = st.slider("Minimum trading days for 'sufficient' metrics", min_value=5, max_value=100, value=20)
    st.caption("Tip: data gaps are expected (SPACs, symbol types, provider limits).")

# ---------- Load core tables (dbt outputs) ----------
# Expect these dbt relations exist:
#   analytics_staging.stg_ipos_recent
#   analytics_staging.stg_prices_ipo_window_100d
#   analytics_analytics.ipo_metrics_100d
#   analytics_analytics.industry_benchmarks_100d
#   analytics_analytics.ipo_coverage + ipo_coverage_summary (optional but recommended)

# 1) Recent IPO base
ipos = q(engine, f"""
select *
from {schema_staging}.stg_ipos_recent
where ipo_date >= (current_date - (:recent_days || ' days')::interval)
""", {"recent_days": recent_days})

# 2) Price window rows
px = q(engine, f"""
select *
from {schema_staging}.stg_prices_ipo_window_100d
where ipo_date >= (current_date - (:recent_days || ' days')::interval)
""", {"recent_days": recent_days})

# 3) Metrics
metrics = q(engine, f"""
select *
from {schema_analytics}.ipo_metrics_100d
where ipo_date >= (current_date - (:recent_days || ' days')::interval)
""", {"recent_days": recent_days})

# Optional: industry benchmarks
bench = q(engine, f"""
select *
from {schema_analytics}.industry_benchmarks_100d
""")

# ---------- Derive coverage stats ----------
total_ipos = len(ipos)
priceable_ipos = int((ipos.get("is_priceable", pd.Series(dtype=bool)) == True).sum()) if "is_priceable" in ipos.columns else None

# IPOs with >=1 price row
ipos_with_any_prices = px["event_id"].nunique() if len(px) else 0

# Trading days per IPO (within the window you built)
days_by_ipo = (
    px.groupby("event_id")["price_date"]
    .nunique()
    .reset_index(name="n_days")
    if len(px)
    else pd.DataFrame({"event_id": [], "n_days": []})
)

# Merge into ipos for tiering
ipos_cov = ipos.merge(days_by_ipo, on="event_id", how="left")
ipos_cov["n_days"] = ipos_cov["n_days"].fillna(0).astype(int)

# Tiers (simple + explainable)
def tier(n: int) -> str:
    if n >= 60:
        return "A (>=60 days)"
    if n >= min_trading_days:
        return f"B (>= {min_trading_days} days)"
    if n >= 1:
        return "C (1..min-1 days)"
    return "D (0 days)"

ipos_cov["coverage_tier"] = ipos_cov["n_days"].apply(tier)

# Sufficient metrics = having at least min_trading_days price points
sufficient_ids = set(ipos_cov.loc[ipos_cov["n_days"] >= min_trading_days, "event_id"].tolist())
metrics["is_sufficient_history"] = metrics["event_id"].isin(sufficient_ids)

# ---------- TOP KPIs ----------
kpi_cols = st.columns(5)

kpi_cols[0].metric("Recent IPOs", f"{total_ipos:,}")
kpi_cols[1].metric("With any prices", f"{ipos_with_any_prices:,}", f"{pct(ipos_with_any_prices, total_ipos):.1f}%")

if priceable_ipos is not None:
    kpi_cols[2].metric("Priceable (mapped)", f"{priceable_ipos:,}", f"{pct(priceable_ipos, total_ipos):.1f}%")
else:
    kpi_cols[2].metric("Priceable (mapped)", "n/a")

# Metrics availability
metrics_nonnull = int(metrics["return_100d"].notna().sum()) if "return_100d" in metrics.columns else 0
kpi_cols[3].metric("Return computed", f"{metrics_nonnull:,}", f"{pct(metrics_nonnull, total_ipos):.1f}%")

sufficient_count = int(metrics["is_sufficient_history"].sum()) if len(metrics) else 0
kpi_cols[4].metric("Sufficient history", f"{sufficient_count:,}", f"{pct(sufficient_count, total_ipos):.1f}%")

st.divider()

# ---------- Interpretations / narrative ----------
with st.expander("How to read this (interpretation)", expanded=True):
    st.markdown(
        f"""
**What you’re seeing**
- “Recent IPOs” = IPOs in the last **{recent_days} days**.
- “With any prices” = IPOs that have **≥ 1** daily price row in your 0..100 day window.
- “Priceable (mapped)” = IPOs whose symbols were successfully mapped to an Alpha Vantage symbol *and* flagged priceable.
- “Sufficient history” = IPOs with **≥ {min_trading_days} trading days** in your window (a practical threshold to make metrics meaningful).

**Why coverage is not 100% (this is expected)**
- Many “IPOs” are actually **SPAC units / warrants / rights**; those often don’t have clean daily candles from Alpha Vantage.
- Some symbols change right after listing, and free endpoints can have gaps.
- Trading days ≠ calendar days (weekends/holidays), so you’ll never see 100 rows for “100 days”.

**What success looks like for the capstone**
- The dashboard makes missingness *explicit* (tiers), so analysis is based on **sufficient data** only.
- You can track improvements by increasing: mapped %, any-price %, and sufficient-history %.
"""
    )

# ---------- Coverage distribution ----------
left, right = st.columns([1, 1])

with left:
    st.subheader("Coverage tiers (how complete is price history?)")
    tier_counts = ipos_cov["coverage_tier"].value_counts().reset_index()
    tier_counts.columns = ["coverage_tier", "count"]
    st.dataframe(tier_counts, use_container_width=True)
    st.bar_chart(tier_counts.set_index("coverage_tier")["count"])

with right:
    st.subheader("Trading-days distribution")
    if len(ipos_cov):
        st.bar_chart(ipos_cov["n_days"])
    st.caption("If this is heavily skewed toward 0–5 days, the main bottleneck is provider coverage (or symbol type).")

st.divider()

# ---------- Drilldown table ----------
st.subheader("IPO drilldown (sort/filter here)")
cols_show = [
    c for c in [
        "event_id", "ipo_date", "ipo_symbol", "price_symbol", "is_priceable",
        "company_name", "exchange", "industry", "country", "market_cap", "n_days", "coverage_tier"
    ] if c in ipos_cov.columns
]
st.dataframe(
    ipos_cov[cols_show].sort_values(["coverage_tier", "n_days"], ascending=[True, False]),
    use_container_width=True,
    height=380
)

st.divider()

# ---------- Metrics section ----------
st.subheader("Performance metrics (computed from available data)")

# Only show “sufficient history” by default
only_sufficient = st.checkbox("Show only IPOs with sufficient price history", value=True)
mview = metrics.copy()
if only_sufficient:
    mview = mview[mview["is_sufficient_history"] == True]

# A couple of summary stats
mcols = st.columns(4)
if "return_100d" in mview.columns:
    mcols[0].metric("Median 100d return", f"{mview['return_100d'].median():.2%}" if len(mview) else "n/a")
    mcols[1].metric("Avg 100d return", f"{mview['return_100d'].mean():.2%}" if len(mview) else "n/a")
if "max_drawdown_100d" in mview.columns:
    mcols[2].metric("Median max drawdown", f"{mview['max_drawdown_100d'].median():.2%}" if len(mview) else "n/a")
    mcols[3].metric("Avg max drawdown", f"{mview['max_drawdown_100d'].mean():.2%}" if len(mview) else "n/a")

# Show metrics table
metric_cols = [c for c in [
    "event_id", "ipo_date", "ipo_symbol", "industry", "n_days",
    "first_close", "last_close", "return_100d", "max_drawdown_100d", "as_of_date"
] if c in mview.columns]
st.dataframe(mview[metric_cols].sort_values("ipo_date", ascending=False), use_container_width=True, height=360)

with st.expander("Interpretation tips for metrics"):
    st.markdown(
        """
- **return_100d**: total return from the first available close to the last available close within the window.
  - If early days are missing, the “first” close may be later than IPO day. That’s okay, but disclose it (use n_days / tier).
- **max_drawdown_100d**: worst peak-to-trough decline within the observed window.
  - This is very sensitive to sparse data. Only trust it for sufficient tiers.
"""
    )

st.divider()

# ---------- Industry benchmarks ----------
st.subheader("Industry benchmarks (cohort view)")
if len(bench):
    st.dataframe(bench.sort_values([c for c in ["cohort_size", "industry"] if c in bench.columns], ascending=[False, True]), use_container_width=True)
else:
    st.info("No industry benchmark rows found yet (industry might be missing for most profiles).")

with st.expander("Interpretation: what benchmarks are telling you"):
    st.markdown(
        """
Benchmarks are only as good as profile coverage:
- If most industries are null, your Finnhub profile enrichment is the bottleneck (429 rate limits / missing tickers).
- Once profiles backfill, you'll start seeing meaningful cohort sizes and return distributions by industry.
"""
    )

st.caption(f"Data refreshed: {date.today().isoformat()} • Schemas: staging={schema_staging}, analytics={schema_analytics}")
