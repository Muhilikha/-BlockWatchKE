import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text
from database.db_connection import engine

st.set_page_config(page_title="BlockWatchKE", page_icon="🇰🇪", layout="wide")

@st.cache_data(ttl=60)
def load_summary():
    with engine.connect() as conn:
        total_tx  = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
        total_vol = conn.execute(text("SELECT COALESCE(SUM(amount_usdt),0) FROM transactions")).scalar()
        alerts    = conn.execute(text("SELECT COUNT(*) FROM alerts WHERE status='open'")).scalar()
        scored    = conn.execute(text("SELECT COUNT(*) FROM risk_scores")).scalar()
    return {"total_tx": total_tx, "total_vol": float(total_vol), "alerts": alerts, "scored": scored}

@st.cache_data(ttl=60)
def load_recent_transactions(limit=100):
    return pd.read_sql(f"""
        SELECT tx_hash, sender, receiver, amount_usdt,
               to_timestamp(timestamp/1000.0) AS block_time
        FROM transactions ORDER BY timestamp DESC LIMIT {limit}
    """, engine)

@st.cache_data(ttl=60)
def load_alerts():
    return pd.read_sql("""
        SELECT a.id, a.tx_hash, a.wallet, a.alert_type, a.risk_score,
               a.status, a.created_at, t.amount_usdt, t.sender, t.receiver
        FROM alerts a
        LEFT JOIN transactions t ON a.tx_hash = t.tx_hash
        WHERE a.status = 'open'
        ORDER BY a.risk_score DESC LIMIT 500
    """, engine)

@st.cache_data(ttl=300)
def load_volume_over_time():
    return pd.read_sql("""
        SELECT date_trunc('minute', to_timestamp(timestamp/1000.0)) AS minute,
               COUNT(*) AS tx_count, SUM(amount_usdt) AS total_usdt
        FROM transactions GROUP BY 1 ORDER BY 1
    """, engine)

@st.cache_data(ttl=300)
def load_risk_distribution():
    return pd.read_sql("""
        SELECT CASE
            WHEN total_score >= 80 THEN 'Critical (80-100)'
            WHEN total_score >= 60 THEN 'High (60-79)'
            WHEN total_score >= 40 THEN 'Medium (40-59)'
            ELSE 'Low (0-39)' END AS risk_band,
            COUNT(*) AS count
        FROM risk_scores GROUP BY 1 ORDER BY 2 DESC
    """, engine)

@st.cache_data(ttl=300)
def load_top_wallets():
    return pd.read_sql("""
        SELECT sender AS wallet, COUNT(*) AS tx_count, SUM(amount_usdt) AS total_sent
        FROM transactions GROUP BY sender ORDER BY total_sent DESC LIMIT 20
    """, engine)

@st.cache_data(ttl=300)
def load_corridors():
    path = "data/processed/payment_corridors.csv"
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

@st.cache_data(ttl=300)
def load_centrality():
    path = "data/processed/wallet_centrality.csv"
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

st.sidebar.title("BlockWatchKE 🇰🇪")
st.sidebar.caption("Blockchain Intelligence Platform")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigation", [
    "📊 Overview", "🔴 Live Alerts", "💸 Transactions",
    "🗺️ Corridors", "🕸️ Graph Analytics"
])
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

if page == "📊 Overview":
    st.title("📊 BlockWatchKE — Overview")
    st.caption("Real-time Tron USDT monitoring for Kenya")
    s = load_summary()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Transactions",  f"{s['total_tx']:,}")
    c2.metric("Total USDT Volume",   f"${s['total_vol']:,.0f}")
    c3.metric("Open Alerts",         f"{s['alerts']:,}")
    c4.metric("Transactions Scored", f"{s['scored']:,}")
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📈 Volume Over Time")
        vol = load_volume_over_time()
        if not vol.empty:
            fig = px.area(vol, x="minute", y="total_usdt", color_discrete_sequence=["#00d4aa"])
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.subheader("🎯 Risk Distribution")
        risk = load_risk_distribution()
        if not risk.empty:
            fig = px.pie(risk, names="risk_band", values="count",
                color_discrete_map={"Critical (80-100)":"#ff0000","High (60-79)":"#ff6600",
                                    "Medium (40-59)":"#ffaa00","Low (0-39)":"#00aa44"})
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    st.subheader("🏦 Top 20 Wallets by Volume")
    top = load_top_wallets()
    if not top.empty:
        fig = px.bar(top, x="total_sent", y="wallet", orientation="h",
                     color="total_sent", color_continuous_scale="Teal")
        fig.update_layout(height=500, showlegend=False, yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)

elif page == "🔴 Live Alerts":
    st.title("🔴 Live Alerts")
    df = load_alerts()
    if df.empty:
        st.info("No open alerts.")
    else:
        st.metric("Open Alerts", f"{len(df):,}")
        min_score = st.slider("Minimum Risk Score", 0, 100, 60)
        filtered = df[df["risk_score"] >= min_score]
        st.dataframe(filtered[["tx_hash","sender","receiver","amount_usdt","risk_score","alert_type","created_at"]],
                     use_container_width=True, height=500)
        st.download_button("⬇️ Export CSV", filtered.to_csv(index=False), "alerts.csv", "text/csv")

elif page == "💸 Transactions":
    st.title("💸 Recent Transactions")
    limit = st.slider("Number of transactions", 50, 500, 100)
    df = load_recent_transactions(limit)
    search = st.text_input("🔍 Search by wallet address")
    if search:
        df = df[df["sender"].str.contains(search, case=False, na=False) |
                df["receiver"].str.contains(search, case=False, na=False)]
    st.dataframe(df, use_container_width=True, height=500)
    c1, c2 = st.columns(2)
    c1.metric("Showing", f"{len(df):,} transactions")
    c2.metric("Volume",  f"${df['amount_usdt'].sum():,.2f} USDT")

elif page == "🗺️ Corridors":
    st.title("🗺️ Payment Corridors")
    corridors = load_corridors()
    if corridors.empty:
        st.info("Run Phase 4 first.")
    else:
        fig = px.bar(corridors.head(20), x="total_usdt", y=corridors.head(20).index,
                     orientation="h", color="total_usdt", color_continuous_scale="Reds",
                     hover_data=["sender","receiver","tx_count"])
        fig.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(corridors, use_container_width=True)

elif page == "🕸️ Graph Analytics":
    st.title("🕸️ Graph Analytics")
    centrality = load_centrality()
    if centrality.empty:
        st.info("Run Phase 4 first.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Wallets",    f"{len(centrality):,}")
        c2.metric("High Betweenness", f"{(centrality['betweenness'] > 0.01).sum():,}")
        c3.metric("Total Volume",     f"${centrality['total_volume'].sum():,.0f}")
        st.markdown("---")
        st.subheader("Top 30 Wallets by Betweenness Centrality")
        top = centrality.nlargest(30, "betweenness")
        fig = px.scatter(top, x="total_volume", y="betweenness", size="total_volume",
                         color="betweenness", color_continuous_scale="Reds",
                         hover_data=["address"])
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(centrality.head(100)[["address","total_volume","betweenness",
                                           "in_degree_usdt","out_degree_usdt"]],
                     use_container_width=True, height=400)
        st.download_button("⬇️ Export CSV", centrality.to_csv(index=False), "centrality.csv", "text/csv")
