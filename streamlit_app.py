from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine


st.set_page_config(
    page_title="IPO Intel Dashboard",
    page_icon="📈",
    layout="wide",
)


def get_engine():
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    db = os.getenv("PG_DB", "postgres")
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str)


@st.cache_data(ttl=60)
def load_metrics() -> pd.DataFrame:
    query = """
        select
            event_id,
            ipo_date,
            ipo_symbol,
            price_symbol,
            industry,
            sector,
            price_days,
            first_price_date,
            last_price_date,
            first_close_30d,
            last_close_30d,
            avg_close_30d,
            min_close_30d,
            max_close_30d,
            return_30d_pct,
            coverage_status_30d
        from analytics_analytics.ipo_metrics_30d
        order by ipo_date desc
    """
    return pd.read_sql(query, get_engine())


@st.cache_data(ttl=60)
def load_coverage_summary() -> pd.DataFrame:
    query = """
        select
            coverage_status,
            ipo_count,
            with_industry,
            with_sector,
            avg_price_days
        from analytics_analytics.ipo_coverage_summary_30d
        order by
            case coverage_status
                when 'good_coverage' then 1
                when 'partial_coverage' then 2
                when 'very_low_coverage' then 3
                when 'no_prices' then 4
                else 5
            end
    """
    return pd.read_sql(query, get_engine())


@st.cache_data(ttl=60)
def load_industry_benchmarks() -> pd.DataFrame:
    query = """
        select
            industry,
            ipo_count,
            avg_return_30d_pct,
            avg_price_days
        from analytics_analytics.industry_benchmarks_30d
        order by ipo_count desc, industry
    """
    return pd.read_sql(query, get_engine())


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["ipo_date", "first_price_date", "last_price_date"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["industry"] = out["industry"].fillna("Unknown")
    out["sector"] = out["sector"].fillna("Unknown")
    out["price_days"] = out["price_days"].fillna(0).astype(int)

    out["coverage_band"] = out["price_days"].apply(classify_coverage_band)

    out["price_range_pct_30d"] = (
        ((out["max_close_30d"] - out["min_close_30d"]) / out["first_close_30d"]) * 100.0
    )
    out.loc[out["first_close_30d"].isna() | (out["first_close_30d"] == 0), "price_range_pct_30d"] = None

    return out


def classify_coverage_band(price_days: int) -> str:
    if price_days >= 15:
        return "good_coverage"
    if price_days >= 7:
        return "partial_coverage"
    if price_days >= 1:
        return "very_low_coverage"
    return "no_prices"


def build_executive_interpretation(df: pd.DataFrame) -> str:
    total = len(df)
    if total == 0:
        return "No IPOs match the current filters."

    with_any = int((df["price_days"] >= 1).sum())
    good = int((df["price_days"] >= 15).sum())
    partial = int(((df["price_days"] >= 7) & (df["price_days"] < 15)).sum())
    weak = int(((df["price_days"] >= 1) & (df["price_days"] < 7)).sum())
    no_prices = int((df["price_days"] == 0).sum())

    pct_any = round((with_any / total) * 100, 2) if total else 0
    pct_good = round((good / total) * 100, 2) if total else 0

    priced = df[df["price_days"] >= 15].copy()
    avg_return = priced["return_30d_pct"].dropna().mean() if not priced.empty else None

    if avg_return is None or pd.isna(avg_return):
        performance_line = "There are currently no strongly covered IPOs with usable return metrics under the selected filters."
    elif avg_return >= 0:
        performance_line = f"Among strongly covered IPOs, the average 30-day return is {avg_return:.2f}%."
    else:
        performance_line = f"Among strongly covered IPOs, the average 30-day return is {avg_return:.2f}%, indicating weak short-term post-IPO performance."

    return (
        f"Out of {total} IPOs in the filtered dataset, {with_any} ({pct_any}%) have at least one retrieved "
        f"price observation. {good} IPOs ({pct_good}%) have strong coverage in the first 30 calendar days after IPO, "
        f"defined as at least 15 observed trading days. {partial} IPOs have partial 7–14 day coverage, "
        f"{weak} have only 1–6 days, and {no_prices} currently have no retrieved prices. {performance_line}"
    )


def top_industry_message(df: pd.DataFrame) -> str:
    strong = df[df["price_days"] >= 15].copy()
    strong = strong[strong["return_30d_pct"].notna()]

    if strong.empty:
        return "No industry-level performance insight is available under the current filters because there are no strongly covered IPOs with returns."

    grouped = (
        strong.groupby("industry", as_index=False)
        .agg(
            ipo_count=("event_id", "count"),
            avg_return_30d_pct=("return_30d_pct", "mean"),
        )
        .sort_values(["avg_return_30d_pct", "ipo_count"], ascending=[False, False])
    )

    top = grouped.iloc[0]
    bottom = grouped.iloc[-1]

    return (
        f"Best-performing industry in the current filtered view: {top['industry']} "
        f"(avg 30D return {top['avg_return_30d_pct']:.2f}%, {int(top['ipo_count'])} IPOs). "
        f"Weakest industry: {bottom['industry']} "
        f"(avg 30D return {bottom['avg_return_30d_pct']:.2f}%, {int(bottom['ipo_count'])} IPOs)."
    )


def style_metric(value, is_pct=False):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "n/a"
    if is_pct:
        return f"{value:.2f}%"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def main():
    st.title("📈 IPO Intel Dashboard")
    st.caption("Coverage-aware short-term IPO analytics using a 30-calendar-day post-listing window")

    metrics = add_derived_columns(normalize_dates(load_metrics()))
    coverage_summary = load_coverage_summary()
    industry_benchmarks = load_industry_benchmarks()

    if metrics.empty:
        st.error("No data found in analytics_analytics.ipo_metrics_30d.")
        st.stop()

    st.sidebar.header("Filters")

    industries = ["All"] + sorted(metrics["industry"].dropna().unique().tolist())
    sectors = ["All"] + sorted(metrics["sector"].dropna().unique().tolist())
    coverage_options = ["All", "good_coverage", "partial_coverage", "very_low_coverage", "no_prices"]

    selected_industry = st.sidebar.selectbox("Industry", industries, index=0)
    selected_sector = st.sidebar.selectbox("Sector", sectors, index=0)
    selected_coverage = st.sidebar.selectbox("Coverage status", coverage_options, index=0)
    min_return = st.sidebar.slider("Minimum 30D return %", min_value=-100.0, max_value=300.0, value=-100.0, step=1.0)
    max_return = st.sidebar.slider("Maximum 30D return %", min_value=-100.0, max_value=300.0, value=300.0, step=1.0)

    filtered = metrics.copy()

    if selected_industry != "All":
        filtered = filtered[filtered["industry"] == selected_industry]

    if selected_sector != "All":
        filtered = filtered[filtered["sector"] == selected_sector]

    if selected_coverage != "All":
        filtered = filtered[filtered["coverage_band"] == selected_coverage]

    filtered = filtered[
        filtered["return_30d_pct"].isna()
        | ((filtered["return_30d_pct"] >= min_return) & (filtered["return_30d_pct"] <= max_return))
    ]

    total_ipos = len(filtered)
    with_any_prices = int((filtered["price_days"] >= 1).sum())
    good_coverage = int((filtered["price_days"] >= 15).sum())
    avg_price_days = filtered["price_days"].mean() if total_ipos else None

    strong = filtered[filtered["price_days"] >= 15].copy()
    strong_returns = strong["return_30d_pct"].dropna()
    avg_return = strong_returns.mean() if not strong_returns.empty else None

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("IPOs", total_ipos)
    c2.metric("With Any Prices", with_any_prices)
    c3.metric("Good Coverage", good_coverage)
    c4.metric("Avg Price Days", style_metric(avg_price_days))
    c5.metric("Avg 30D Return %", style_metric(avg_return, is_pct=True))

    st.subheader("Executive interpretation")
    st.info(build_executive_interpretation(filtered))
    st.caption(top_industry_message(filtered))

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Overview",
            "Performance",
            "Industry Analysis",
            "IPO Explorer",
        ]
    )

    with tab1:
        left, right = st.columns(2)

        with left:
            st.subheader("Coverage summary")
            st.dataframe(coverage_summary, use_container_width=True, hide_index=True)

        with right:
            st.subheader("Coverage status distribution")
            coverage_chart = (
                filtered.groupby("coverage_band", dropna=False)
                .size()
                .reset_index(name="ipo_count")
                .sort_values("ipo_count", ascending=False)
            )
            fig = px.bar(
                coverage_chart,
                x="coverage_band",
                y="ipo_count",
                title="IPOs by coverage status",
            )
            st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Price-days distribution")
            fig = px.histogram(
                filtered,
                x="price_days",
                nbins=20,
                title="Distribution of retrieved trading days",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("IPO count by sector")
            sector_counts = (
                filtered.groupby("sector", as_index=False)
                .agg(ipo_count=("event_id", "count"))
                .sort_values("ipo_count", ascending=False)
                .head(15)
            )
            fig = px.bar(
                sector_counts,
                x="sector",
                y="ipo_count",
                title="Top sectors by IPO count",
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Short-term performance insight")

        if strong.empty:
            st.warning("No IPOs with strong 30-day-window coverage under the current filters.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                winners = (
                    strong[strong["return_30d_pct"].notna()]
                    .sort_values("return_30d_pct", ascending=False)
                    .head(10)
                )
                st.markdown("**Top 10 IPOs by 30D return**")
                st.dataframe(
                    winners[
                        ["ipo_symbol", "industry", "sector", "price_days", "return_30d_pct", "price_range_pct_30d"]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

            with col2:
                losers = (
                    strong[strong["return_30d_pct"].notna()]
                    .sort_values("return_30d_pct", ascending=True)
                    .head(10)
                )
                st.markdown("**Bottom 10 IPOs by 30D return**")
                st.dataframe(
                    losers[
                        ["ipo_symbol", "industry", "sector", "price_days", "return_30d_pct", "price_range_pct_30d"]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

            col3, col4 = st.columns(2)

            with col3:
                fig = px.bar(
                    strong.sort_values("return_30d_pct", ascending=False),
                    x="ipo_symbol",
                    y="return_30d_pct",
                    hover_data=["industry", "sector", "price_days"],
                    title="30D return by IPO",
                )
                st.plotly_chart(fig, use_container_width=True)

            with col4:
                plot_df = strong[strong["return_30d_pct"].notna()].copy()
                fig = px.scatter(
                    plot_df,
                    x="price_days",
                    y="return_30d_pct",
                    color="industry",
                    hover_data=["ipo_symbol", "sector"],
                    title="Return vs coverage",
                )
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("Volatility / trading range view")
            range_df = strong[strong["price_range_pct_30d"].notna()].copy()
            fig = px.bar(
                range_df.sort_values("price_range_pct_30d", ascending=False).head(15),
                x="ipo_symbol",
                y="price_range_pct_30d",
                hover_data=["industry", "sector", "return_30d_pct"],
                title="Largest 30D trading ranges",
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Industry benchmarks")

        if industry_benchmarks.empty:
            st.warning("No industry benchmark data available.")
        else:
            st.dataframe(industry_benchmarks, use_container_width=True, hide_index=True)

            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    industry_benchmarks.sort_values("avg_return_30d_pct", ascending=False),
                    x="industry",
                    y="avg_return_30d_pct",
                    hover_data=["ipo_count", "avg_price_days"],
                    title="Average 30D return by industry",
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.scatter(
                    industry_benchmarks,
                    x="ipo_count",
                    y="avg_return_30d_pct",
                    size="avg_price_days",
                    hover_name="industry",
                    title="Industry scale vs return",
                )
                st.plotly_chart(fig, use_container_width=True)

            strong_industry = filtered[
                (filtered["price_days"] >= 15) & filtered["return_30d_pct"].notna()
            ].copy()

            if not strong_industry.empty:
                analyst_view = (
                    strong_industry.groupby(["industry", "sector"], as_index=False)
                    .agg(
                        ipo_count=("event_id", "count"),
                        avg_return_30d_pct=("return_30d_pct", "mean"),
                        median_return_30d_pct=("return_30d_pct", "median"),
                        avg_price_days=("price_days", "mean"),
                    )
                    .sort_values(["avg_return_30d_pct", "ipo_count"], ascending=[False, False])
                )

                st.subheader("Business analyst view: industry/sector opportunity scan")
                st.dataframe(analyst_view, use_container_width=True, hide_index=True)

    with tab4:
        st.subheader("IPO explorer")

        symbols = sorted(filtered["ipo_symbol"].dropna().unique().tolist())
        selected_symbol = st.selectbox("Select IPO symbol", symbols) if symbols else None

        if selected_symbol is None:
            st.warning("No IPO symbols available under the current filters.")
        else:
            row = filtered[filtered["ipo_symbol"] == selected_symbol].iloc[0]

            col1, col2, col3 = st.columns(3)
            col1.metric("IPO Symbol", row["ipo_symbol"])
            col2.metric("Coverage Status", row["coverage_band"])
            col3.metric("30D Return %", style_metric(row["return_30d_pct"], is_pct=True))

            detail = pd.DataFrame(
                [
                    {"field": "Event ID", "value": row["event_id"]},
                    {"field": "IPO Date", "value": row["ipo_date"]},
                    {"field": "Price Symbol", "value": row["price_symbol"]},
                    {"field": "Industry", "value": row["industry"]},
                    {"field": "Sector", "value": row["sector"]},
                    {"field": "Price Days", "value": row["price_days"]},
                    {"field": "First Price Date", "value": row["first_price_date"]},
                    {"field": "Last Price Date", "value": row["last_price_date"]},
                    {"field": "First Close 30D", "value": row["first_close_30d"]},
                    {"field": "Last Close 30D", "value": row["last_close_30d"]},
                    {"field": "Average Close 30D", "value": row["avg_close_30d"]},
                    {"field": "Min Close 30D", "value": row["min_close_30d"]},
                    {"field": "Max Close 30D", "value": row["max_close_30d"]},
                    {"field": "Price Range % 30D", "value": row["price_range_pct_30d"]},
                ]
            )

            st.dataframe(detail, use_container_width=True, hide_index=True)

            st.subheader("Analyst interpretation")
            if row["price_days"] >= 15 and pd.notna(row["return_30d_pct"]):
                if row["return_30d_pct"] > 0:
                    st.success(
                        f"{row['ipo_symbol']} has strong short-term coverage and a positive 30-day return of "
                        f"{row['return_30d_pct']:.2f}%. This makes it suitable for comparative post-IPO analysis."
                    )
                else:
                    st.warning(
                        f"{row['ipo_symbol']} has strong short-term coverage but a negative 30-day return of "
                        f"{row['return_30d_pct']:.2f}%. It is suitable for analysis and may indicate weak early aftermarket performance."
                    )
            elif row["price_days"] >= 7:
                st.info(
                    f"{row['ipo_symbol']} has only partial coverage ({row['price_days']} trading days). "
                    f"It can support directional analysis, but should be treated cautiously in benchmarking."
                )
            elif row["price_days"] >= 1:
                st.info(
                    f"{row['ipo_symbol']} has minimal price coverage ({row['price_days']} trading days). "
                    f"It is not reliable for benchmarking."
                )
            else:
                st.error(
                    f"{row['ipo_symbol']} currently has no retrieved prices, so performance analysis is not available."
                )

        st.subheader("Full IPO-level table")
        display_cols = [
            "ipo_date",
            "ipo_symbol",
            "price_symbol",
            "industry",
            "sector",
            "price_days",
            "coverage_band",
            "first_close_30d",
            "last_close_30d",
            "return_30d_pct",
            "price_range_pct_30d",
        ]
        st.dataframe(
            filtered[display_cols].sort_values(["price_days", "ipo_date"], ascending=[False, False]),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()