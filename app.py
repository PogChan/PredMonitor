import os
import random
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dash import Dash, Input, Output, dcc, html

from store.trade_store import InMemoryTradeStore, SqliteTradeStore, Trade
from store.mock_feed import start_mock_feed_thread
from ingest.ingest_utils import parse_csv_env
from ui_utils import (
    format_usd,
    format_time,
    format_price,
    format_quantity,
    shorten_address,
    shorten_cluster_id,
)



FEED_MODE = os.getenv("DASH_FEED_MODE", "mock").lower()
FLOW_MIN_USD = float(os.getenv("DASH_FLOW_MIN_USD", "1000"))
FLOW_LIMIT = int(os.getenv("DASH_FLOW_LIMIT", "60"))
LEADERBOARD_LIMIT = int(os.getenv("DASH_LEADERBOARD_LIMIT", "14"))
FLOW_LOOKBACK_HOURS = float(os.getenv("DASH_FLOW_LOOKBACK_HOURS", "6"))
FLOW_MAX_LIMIT = int(os.getenv("DASH_FLOW_MAX_LIMIT", "200"))
INTERESTING_TRADE_USD = float(os.getenv("DASH_INTERESTING_TRADE_USD", "5000"))
INTERESTING_MARKET_SHARE = float(os.getenv("DASH_INTERESTING_MARKET_SHARE", "0.05"))
MARKET_QUERY_DEFAULT = os.getenv("DASH_MARKET_QUERY", "")
DASH_DEBUG = os.getenv("DASH_DEBUG", "false").lower() in {"1", "true", "yes", "on"}
DASH_HOT_RELOAD = os.getenv("DASH_HOT_RELOAD", "").lower() in {"1", "true", "yes", "on"}
DASH_HOT_RELOAD_INTERVAL = int(os.getenv("DASH_HOT_RELOAD_INTERVAL", "1000"))
TRADE_DB_PATH = os.getenv("TRADE_DB_PATH", "data/trades.db")
FOCUS_DEFAULT = [
    value.lower() for value in parse_csv_env(os.getenv("DASH_MARKET_FOCUS", ""))
]

if FEED_MODE == "db":
    STORE = SqliteTradeStore(TRADE_DB_PATH)
else:
    STORE = InMemoryTradeStore()


# Formatting and Mock Feed logic moved to ui_utils.py and store/mock_feed.py


app = Dash(__name__, title="Whale Hunter")

app.layout = html.Div(
    className="app-shell",
    children=[
        html.Div(
            className="top-bar",
            children=[
                html.Div(
                    className="title-block",
                    children=[
                        html.H1("Whale Hunter"),
                        html.P("Smart money flow across Polymarket and Kalshi"),
                    ],
                ),
                html.Div(
                    className="status-stack",
                    children=[
                        html.Div(f"Feed: {FEED_MODE}", className="status-pill"),
                        html.Div("Mode: Live Flow", className="status-pill"),
                    ],
                ),
            ],
        ),
        html.Div(
            className="stat-grid",
            children=[
                html.Div(
                    className="stat-card",
                    children=[
                        html.Div("Active Wallets (24h)", className="stat-label"),
                        html.Div("--", id="stat-wallets", className="stat-value"),
                    ],
                ),
                html.Div(
                    className="stat-card",
                    children=[
                        html.Div("Trades (24h)", className="stat-label"),
                        html.Div("--", id="stat-trades", className="stat-value"),
                    ],
                ),
                html.Div(
                    className="stat-card",
                    children=[
                        html.Div("Flow Rate", className="stat-label"),
                        html.Div("--", id="stat-flow", className="stat-value"),
                    ],
                ),
                html.Div(
                    className="stat-card",
                    children=[
                        html.Div("Last Trade", className="stat-label"),
                        html.Div("--", id="stat-last", className="stat-value"),
                    ],
                ),
            ],
        ),
        html.Div(
            className="filter-bar",
            children=[
                html.Div("Filters", className="filter-label"),
                html.Div(
                    className="filter-group",
                    children=[
                        html.Span("Lookback", className="filter-group-label"),
                        dcc.Dropdown(
                            id="lookback-filter",
                            className="filter-select",
                            clearable=False,
                            options=[
                                {"label": "1h", "value": 1},
                                {"label": "3h", "value": 3},
                                {"label": "6h", "value": 6},
                                {"label": "12h", "value": 12},
                                {"label": "24h", "value": 24},
                                {"label": "3d", "value": 72},
                                {"label": "7d", "value": 168},
                                {"label": "All", "value": 0},
                            ],
                            value=FLOW_LOOKBACK_HOURS if FLOW_LOOKBACK_HOURS in {0, 1, 3, 6, 12, 24, 72, 168} else 6,
                        ),
                    ],
                ),
                html.Div(
                    className="filter-group",
                    children=[
                        html.Span("Min USD", className="filter-group-label"),
                        dcc.Input(
                            id="min-size-filter",
                            className="filter-input",
                            type="number",
                            min=0,
                            step=100,
                            value=FLOW_MIN_USD,
                        ),
                    ],
                ),
                html.Div(
                    className="filter-group",
                    children=[
                        html.Span("Limit", className="filter-group-label"),
                        dcc.Input(
                            id="limit-filter",
                            className="filter-input",
                            type="number",
                            min=1,
                            step=10,
                            value=FLOW_LIMIT,
                        ),
                    ],
                ),
                html.Div(
                    className="filter-group",
                    children=[
                        html.Span("Market", className="filter-group-label"),
                        dcc.Input(
                            id="market-filter",
                            className="filter-input filter-input--text",
                            type="text",
                            placeholder="Search market or company",
                            value=MARKET_QUERY_DEFAULT,
                        ),
                    ],
                ),
                html.Div(
                    className="filter-group",
                    children=[
                        html.Span("Platform", className="filter-group-label"),
                        dcc.Checklist(
                            id="platform-filter",
                            className="focus-toggle",
                            options=[
                                {"label": "Polymarket", "value": "polymarket"},
                                {"label": "Kalshi", "value": "kalshi"},
                            ],
                            value=["polymarket", "kalshi"],
                            inline=True,
                        ),
                    ],
                ),
                html.Div(
                    className="filter-group",
                    children=[
                        html.Span("Focus", className="filter-group-label"),
                        dcc.Checklist(
                            id="focus-filter",
                            className="focus-toggle",
                            options=[
                                {"label": "Niche", "value": "niche"},
                                {"label": "Stock", "value": "stock"},
                            ],
                            value=FOCUS_DEFAULT,
                            inline=True,
                        ),
                    ],
                ),
                html.Div(
                    className="filter-group",
                    children=[
                        dcc.Checklist(
                            id="interesting-filter",
                            className="focus-toggle",
                            options=[{"label": "Interesting only", "value": "interesting"}],
                            value=[],
                            inline=True,
                        ),
                    ],
                ),
            ],
        ),
        dcc.Tabs(
            id="tabs",
            value="flow",
            parent_className="tabs",
            className="tabs-container",
            children=[
                dcc.Tab(
                    label="Live Flow",
                    value="flow",
                    className="tab",
                    selected_className="tab--selected",
                    children=[
                        html.Div(
                            className="panel",
                            children=[html.Div(id="flow-list", className="flow-list")],
                        )
                    ],
                ),
                dcc.Tab(
                    label="Leaderboard",
                    value="leaders",
                    className="tab",
                    selected_className="tab--selected",
                    children=[
                        html.Div(
                            className="panel",
                            children=[
                                html.Table(
                                    className="leaderboard-table",
                                    children=[
                                        html.Thead(
                                            html.Tr(
                                                children=[
                                                    html.Th("Address"),
                                                    html.Th("24h Volume"),
                                                    html.Th("Accumulated Position"),
                                                ]
                                            )
                                        ),
                                        html.Tbody(id="leaderboard-body"),
                                    ],
                                )
                            ],
                        )
                    ],
                ),
                dcc.Tab(
                    label="Wallets",
                    value="wallets",
                    className="tab",
                    selected_className="tab--selected",
                    children=[
                        html.Div(
                            className="panel",
                            children=[
                                html.Div(
                                    className="wallet-controls",
                                    children=[
                                        dcc.Dropdown(
                                            id="wallet-select",
                                            className="filter-select",
                                            placeholder="Select wallet from leaderboard",
                                            options=[],
                                        ),
                                        html.Div(
                                            "Wallet tracking only available for Polymarket.",
                                            className="wallet-note",
                                        ),
                                    ],
                                ),
                                html.Div(id="wallet-summary", className="wallet-summary"),
                                html.Div(id="wallet-trades", className="flow-list"),
                            ],
                        )
                    ],
                ),
            ],
        ),
        dcc.Interval(id="tick", interval=1000, n_intervals=0),
    ],
)


def sanitize_min_size(value: Optional[float]) -> float:
    try:
        if value is None:
            return FLOW_MIN_USD
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return FLOW_MIN_USD


def sanitize_limit(value: Optional[float]) -> int:
    try:
        if value is None:
            return FLOW_LIMIT
        limit = int(value)
    except (TypeError, ValueError):
        return FLOW_LIMIT
    if limit < 1:
        return 1
    return min(limit, FLOW_MAX_LIMIT)


def normalize_market_query(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).strip().lower()


def resolve_since_ts(lookback_hours: Optional[float]) -> Optional[float]:
    if lookback_hours is None:
        return None
    try:
        hours = float(lookback_hours)
    except (TypeError, ValueError):
        return None
    if hours <= 0:
        return 0.0
    return time.time() - hours * 3600


def trade_is_interesting(trade: Trade) -> bool:
    if trade.size_usd >= INTERESTING_TRADE_USD:
        return True
    if trade.market_volume and trade.market_volume > 0:
        share = trade.size_usd / trade.market_volume
        if share >= INTERESTING_MARKET_SHARE:
            return True
    return False


def flow_row(trade: Trade) -> html.Div:
    side = trade.side.lower() if trade.side else "na"
    side_label = side.upper() if side != "na" else "N/A"
    side_class = f"flow-side flow-side--{side}"
    details = []
    badges = []
    details.append(f"Qty {format_quantity(trade.quantity)}")
    details.append(f"Px {format_price(trade.price)}")
    if trade.market_volume:
        details.append(f"Vol {format_usd(trade.market_volume)}")
        if trade.market_volume > 0:
            share = trade.size_usd / trade.market_volume
            details.append(f"Impact {share:.1%}")
    if trade.market_is_niche:
        badges.append(html.Span("Niche", className="flow-badge flow-badge--niche"))
    if trade.market_is_stock:
        badges.append(html.Span("Stock", className="flow-badge flow-badge--stock"))
    if trade.size_usd >= INTERESTING_TRADE_USD:
        badges.append(html.Span("Whale", className="flow-badge flow-badge--whale"))
    if trade.market_volume and trade.market_volume > 0:
        if trade.size_usd / trade.market_volume >= INTERESTING_MARKET_SHARE:
            badges.append(html.Span("Impact", className="flow-badge flow-badge--impact"))
    cluster_label = shorten_cluster_id(trade.cluster_id)
    if cluster_label:
        badges.append(
            html.Span(f"Cluster {cluster_label}", className="flow-badge flow-badge--cluster")
        )
    market_label = trade.market_label or trade.market or "Unknown Market"
    return html.Div(
        className="flow-row",
        children=[
            html.Div(
                className="flow-meta",
                children=[
                    html.Span(format_time(trade.timestamp), className="flow-time"),
                    html.Span(market_label, className="flow-market"),
                ],
            ),
            html.Div(
                className="flow-main",
                children=[
                    html.Span(format_usd(trade.size_usd), className="flow-amount"),
                    html.Span(side_label, className=side_class),
                    html.Span(shorten_address(trade.actor_address or "anon"), className="flow-actor"),
                    html.Div(badges, className="flow-badges") if badges else None,
                    html.Span(" | ".join(details), className="flow-details"),
                ],
            ),
            html.Div((trade.platform or "unknown").upper(), className="flow-platform"),
        ],
    )


@app.callback(
    Output("flow-list", "children"),
    Output("leaderboard-body", "children"),
    Output("stat-wallets", "children"),
    Output("stat-trades", "children"),
    Output("stat-flow", "children"),
    Output("stat-last", "children"),
    Output("wallet-select", "options"),
    Output("wallet-summary", "children"),
    Output("wallet-trades", "children"),
    Input("tick", "n_intervals"),
    Input("focus-filter", "value"),
    Input("lookback-filter", "value"),
    Input("min-size-filter", "value"),
    Input("limit-filter", "value"),
    Input("market-filter", "value"),
    Input("platform-filter", "value"),
    Input("interesting-filter", "value"),
    Input("wallet-select", "value"),
)
def refresh_dashboard(
    _ticks: int,
    focus_filters: Optional[List[str]],
    lookback_hours: Optional[float],
    min_size_value: Optional[float],
    limit_value: Optional[float],
    market_query_value: Optional[str],
    platform_filters: Optional[List[str]],
    interesting_filters: Optional[List[str]],
    selected_wallet: Optional[str],
) -> Tuple[
    List[html.Div],
    List[html.Tr],
    str,
    str,
    str,
    str,
    List[Dict[str, str]],
    List[html.Div],
    List[html.Div],
]:
    min_size = sanitize_min_size(min_size_value)
    limit = sanitize_limit(limit_value)
    since_ts = resolve_since_ts(lookback_hours)
    market_query = normalize_market_query(market_query_value)
    platforms = [value.lower() for value in (platform_filters or []) if value]
    recent = STORE.recent_trades(
        min_size_usd=min_size,
        limit=limit,
        since_ts=since_ts,
        platforms=platforms,
    )
    if market_query:
        recent = [
            trade
            for trade in recent
            if market_query
            in (trade.market_label or trade.market or "").lower()
        ]
    focus = {value.lower() for value in (focus_filters or []) if value}
    if focus:
        recent = [
            trade
            for trade in recent
            if ("niche" in focus and trade.market_is_niche)
            or ("stock" in focus and trade.market_is_stock)
        ]
    if interesting_filters and "interesting" in interesting_filters:
        recent = [trade for trade in recent if trade_is_interesting(trade)]
    flow_items = [flow_row(trade) for trade in recent]
    if not flow_items:
        flow_items = [html.Div("No trades for the current filters.", className="empty-state")]
    leaderboard = STORE.leaderboard(limit=LEADERBOARD_LIMIT, since_ts=since_ts)
    leaderboard_rows = [
        html.Tr(
            children=[
                html.Td(shorten_address(entry["address"])),
                html.Td(format_usd(entry.get("volume"))),
                html.Td(entry["position"]),
            ]
        )
        for entry in leaderboard
    ]
    if not leaderboard_rows:
        leaderboard_rows = [
            html.Tr(
                children=[
                    html.Td(
                        html.Div("No wallets yet", className="empty-state"),
                        colSpan=3,
                    )
                ]
            )
        ]
    stats = STORE.stats()
    last_ts = stats.get("last") if isinstance(stats, dict) else None
    last_value = format_time(last_ts) if isinstance(last_ts, (int, float)) else "--"
    wallet_options = [
        {"label": shorten_address(entry["address"]), "value": entry["address"]}
        for entry in leaderboard
        if entry.get("address")
    ]
    wallet_summary = []
    wallet_trades = []
    if selected_wallet:
        summary = STORE.wallet_summary(selected_wallet, since_ts=since_ts)
        if summary:
            yes_volume = summary.get("yes_volume", 0.0)
            no_volume = summary.get("no_volume", 0.0)
            position = "YES" if yes_volume >= no_volume else "NO"
            wallet_summary = [
                html.Div(
                    className="wallet-summary-card",
                    children=[
                        html.Div("Trades", className="wallet-summary-label"),
                        html.Div(f"{int(summary.get('trades', 0)):,}", className="wallet-summary-value"),
                    ],
                ),
                html.Div(
                    className="wallet-summary-card",
                    children=[
                        html.Div("Volume", className="wallet-summary-label"),
                        html.Div(format_usd(summary.get("volume")), className="wallet-summary-value"),
                    ],
                ),
                html.Div(
                    className="wallet-summary-card",
                    children=[
                        html.Div("Bias", className="wallet-summary-label"),
                        html.Div(position, className="wallet-summary-value"),
                    ],
                ),
                html.Div(
                    className="wallet-summary-card",
                    children=[
                        html.Div("Last Seen", className="wallet-summary-label"),
                        html.Div(format_time(summary.get("last_ts")), className="wallet-summary-value"),
                    ],
                ),
            ]
        else:
            wallet_summary = [html.Div("No trades for that wallet.", className="empty-state")]
        wallet_recent = STORE.recent_trades(
            min_size_usd=min_size,
            limit=limit,
            since_ts=since_ts,
            platforms=platforms,
            wallet=selected_wallet,
        )
        if market_query:
            wallet_recent = [
                trade
                for trade in wallet_recent
                if market_query
                in (trade.market_label or trade.market or "").lower()
            ]
        if focus:
            wallet_recent = [
                trade
                for trade in wallet_recent
                if ("niche" in focus and trade.market_is_niche)
                or ("stock" in focus and trade.market_is_stock)
            ]
        if interesting_filters and "interesting" in interesting_filters:
            wallet_recent = [trade for trade in wallet_recent if trade_is_interesting(trade)]
        wallet_trades = [flow_row(trade) for trade in wallet_recent]
        if not wallet_trades:
            wallet_trades = [html.Div("No wallet trades yet.", className="empty-state")]
    else:
        wallet_summary = [html.Div("Select a wallet to inspect.", className="empty-state")]
        wallet_trades = [html.Div("Pick a wallet to view trades.", className="empty-state")]
    return (
        flow_items,
        leaderboard_rows,
        stats["wallets"],
        stats["trades"],
        stats["flow"],
        last_value,
        wallet_options,
        wallet_summary,
        wallet_trades,
    )


if FEED_MODE == "mock":
    start_mock_feed_thread(STORE)


if __name__ == "__main__":
    host = os.getenv("DASH_HOST", "0.0.0.0")
    port = int(os.getenv("DASH_PORT", "8050"))
    app.run(
        host=host,
        port=port,
        debug=DASH_DEBUG,
        dev_tools_hot_reload=DASH_HOT_RELOAD or DASH_DEBUG,
        dev_tools_hot_reload_interval=DASH_HOT_RELOAD_INTERVAL,
    )
