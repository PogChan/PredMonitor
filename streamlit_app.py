import time
import os
import pandas as pd
import streamlit as st
from store.trade_store import InMemoryTradeStore, SqliteTradeStore
from store.mock_feed import start_mock_feed_thread
from ui_utils import format_usd, format_time, shorten_address, format_price, format_quantity

st.set_page_config(layout="wide", page_title="Whale Hunter")

# Styling to match the dark/premium aesthetic
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    div[data-testid="stMetricValue"] {
        font-size: 24px;
        color: #e6e6e6;
    }
    div[data-testid="stMetricLabel"] {
        color: #a0a0a0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_store():
    feed_mode = os.getenv("DASH_FEED_MODE", "mock").lower()
    path = os.getenv("TRADE_DB_PATH", "data/trades.db")

    if feed_mode == "db":
        store = SqliteTradeStore(path)
    else:
        store = InMemoryTradeStore()
        # Start mock feed in background
        start_mock_feed_thread(store)
    return store

store = get_store()

# Header
c1, c2 = st.columns([3, 1])
c1.title("🐋 Whale Hunter")
c1.caption("Smart money flow across Polymarket and Kalshi")
with c2:
    live = st.toggle("Live Stream", value=True)

# Metrics Grid
m1, m2, m3, m4 = st.columns(4)
metrics = st.empty()

# Tabs
tab_flow, tab_lead = st.tabs(["🌊 Live Flow", "🏆 Leaderboard"])

with tab_flow:
    flow_table = st.empty()

with tab_lead:
    leader_table = st.empty()

def render():
    # 1. Update Metrics
    stats = store.stats()

    # We rebuild metrics manually to avoid full rerun flicker if possible,
    # but Streamlit reruns the whole script in loop anyway.
    # Using st.empty() containers inside columns is tricky,
    # better to clear and rewrite a container holding all metrics.
    with metrics.container():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active Wallets (24h)", stats.get("wallets", "--"))
        c2.metric("Trades (24h)", stats.get("trades", "--"))
        c3.metric("Flow Rate", stats.get("flow", "--"))

        last_ts = stats.get("last")
        last_str = format_time(last_ts) if last_ts else "--"
        c4.metric("Last Trade", last_str)

    # 2. Update Flow Table
    recent = store.recent_trades(min_size_usd=1000, limit=60)
    if recent:
        data = []
        for t in recent:
            data.append({
                "Time": format_time(t.timestamp),
                "Platform": (t.platform or "").upper(),
                "Market": t.market_label or t.market,
                "Side": (t.side or "NA").upper(),
                "Size": t.size_usd,
                "Price": t.price,
                "Qty": t.quantity,
                "Actor": shorten_address(t.actor_address),
            })

        df = pd.DataFrame(data)

        # Color Styling for Side
        def color_side(val):
            color = "#ff4b4b" if val in ["NO", "SELL", "SHORT"] else "#09ab3b" if val in ["YES", "BUY", "LONG"] else ""
            return f'color: {color}; font-weight: bold'

        styled_df = df.style.map(color_side, subset=["Side"])\
                      .format({"Size": "${:,.0f}", "Price": "{:.3f}", "Qty": "{:,.1f}"})

        flow_table.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Timestamp": st.column_config.TextColumn("Time", width="small"),
                "Market": st.column_config.TextColumn("Market", width="large"),
                "Size": st.column_config.ProgressColumn("Size ($)", min_value=0, max_value=20000, format="$%d"),
            }
        )
    else:
        flow_table.info("Waiting for trades...")

    # 3. Update Leaderboard
    leaders = store.leaderboard(limit=15)
    if leaders:
        ldf = pd.DataFrame(leaders)
        ldf = ldf.rename(columns={"address": "Address", "volume": "Volume ($)", "position": "Position"})
        ldf["Address"] = ldf["Address"].apply(shorten_address)

        styled_leads = ldf.style.format({"Volume ($)": "${:,.0f}"}).background_gradient(subset=["Volume ($)"], cmap="Greens")
        leader_table.dataframe(styled_leads, use_container_width=True, hide_index=True)
    else:
        leader_table.info("No leaderboard data yet.")

if live:
    # Run loop
    render()
    time.sleep(1)
    st.rerun()
else:
    # Render once
    render()
