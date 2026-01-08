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
TRADE_DB_PATH = os.getenv("TRADE_DB_PATH", "data/trades.db")
FOCUS_DEFAULT = [
    value.lower() for value in parse_csv_env(os.getenv("DASH_MARKET_FOCUS", "niche,stock"))
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
                html.Div("Focus", className="filter-label"),
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
            ],
        ),
        dcc.Interval(id="tick", interval=1000, n_intervals=0),
    ],
)


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
    if trade.market_is_niche:
        badges.append(html.Span("Niche", className="flow-badge flow-badge--niche"))
    if trade.market_is_stock:
        badges.append(html.Span("Stock", className="flow-badge flow-badge--stock"))
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
    Input("tick", "n_intervals"),
    Input("focus-filter", "value"),
)
def refresh_dashboard(
    _ticks: int, focus_filters: Optional[List[str]]
) -> Tuple[List[html.Div], List[html.Tr], str, str, str, str]:
    recent = STORE.recent_trades(min_size_usd=FLOW_MIN_USD, limit=FLOW_LIMIT)
    focus = {value.lower() for value in (focus_filters or []) if value}
    if focus:
        recent = [
            trade
            for trade in recent
            if ("niche" in focus and trade.market_is_niche)
            or ("stock" in focus and trade.market_is_stock)
        ]
    flow_items = [flow_row(trade) for trade in recent]
    if not flow_items:
        flow_items = [html.Div("Waiting for trades...", className="empty-state")]
    leaderboard = STORE.leaderboard(limit=LEADERBOARD_LIMIT)
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
    return (
        flow_items,
        leaderboard_rows,
        stats["wallets"],
        stats["trades"],
        stats["flow"],
        last_value,
    )


if FEED_MODE == "mock":
    start_mock_feed_thread(STORE)


if __name__ == "__main__":
    host = os.getenv("DASH_HOST", "0.0.0.0")
    port = int(os.getenv("DASH_PORT", "8050"))
    app.run(host=host, port=port, debug=False)
