# PredMonitor

## Filters

Polymarket event filters apply when discovering RTDS event slugs or CLOB market lists. If any filter is set, wildcard subscriptions are skipped and the event list is filtered locally.

Examples:

```
POLYMARKET_EVENT_KEYWORDS=earnings,eps,deliveries
POLYMARKET_EVENT_CATEGORIES=Companies
POLYMARKET_EVENT_COMPANIES=AAPL,TSLA,GOOGL
POLYMARKET_EVENT_TAGS=earnings
POLYMARKET_EVENT_EXCLUDE_KEYWORDS=resolved,settled
POLYMARKET_EVENTS_PARAMS=tag_id=123
```

Kalshi filters auto-discover market tickers when no `KALSHI_MARKET_TICKERS` are supplied. The market list is fetched and filtered locally.

Examples:

```
KALSHI_MARKET_KEYWORDS=earnings,deliveries
KALSHI_MARKET_COMPANIES=AAPL,TSLA,GOOGL
KALSHI_MARKET_EXCLUDE_KEYWORDS=resolved,settled
KALSHI_MARKETS_URL=https://api.elections.kalshi.com/trade-api/v2/markets
KALSHI_MARKETS_PARAMS=status=open
```

## Market Focus (Niche vs Stock)

The Dash app can focus on niche and stock-oriented markets. Trades are labeled using market metadata plus keyword rules. Long-dated markets (by year in the title) and mainstream topics can be excluded from the focus buckets.

Environment variables:

```
# Dash UI focus toggles (comma-separated)
DASH_MARKET_FOCUS=niche,stock

# Keyword classification (comma-separated)
MARKET_NICHE_KEYWORDS=arrest,indictment,investigation,venezuela,maduro
MARKET_STOCK_KEYWORDS=earnings,eps,revenue,guidance,ipo,stock,shares
MARKET_EXCLUDE_KEYWORDS=bitcoin,btc,ethereum,eth,crypto,super bowl,nfl,nba

# Long-dated filter (years ahead of current year)
MARKET_MAX_YEARS_AHEAD=1

# Optional: classify low-liquidity markets as niche
MARKET_NICHE_MAX_VOLUME_USD=500000
```

Notes:
- If a market is marked as `niche` or `stock`, it will appear when that focus is selected in the UI.
- Set `MARKET_*_KEYWORDS=off` to disable the default list for that bucket.

## Dash Flow Filters

Environment variables:

```
# Default filters for the Live Flow tab
DASH_FLOW_MIN_USD=1000
DASH_FLOW_LIMIT=60
DASH_FLOW_LOOKBACK_HOURS=6
DASH_FLOW_MAX_LIMIT=200
DASH_MARKET_QUERY=

# "Interesting" flag thresholds
DASH_INTERESTING_TRADE_USD=5000
DASH_INTERESTING_MARKET_SHARE=0.05

# Live reload controls
DASH_DEBUG=true
DASH_HOT_RELOAD=true
DASH_HOT_RELOAD_INTERVAL=1000
```

Notes:
- Set `DASH_FLOW_LOOKBACK_HOURS=0` to show all trades.
- Interesting trades are flagged by size or share of market volume.

## Polymarket Trade Sources

The ingest supports multiple Polymarket stream modes:

```
POLYMARKET_STREAM_MODE=clob   # order book updates (no size)
POLYMARKET_STREAM_MODE=rtds   # RTDS trades stream
POLYMARKET_STREAM_MODE=data   # REST poller via data-api
```

For the REST poller:

```
POLYMARKET_DATA_TRADES_URL=https://data-api.polymarket.com/trades
POLYMARKET_DATA_POLL_SECONDS=2
POLYMARKET_DATA_LIMIT=200
```

## Semantic Clustering

Trades can be grouped into cross-venue clusters using fuzzy string matching so that similar
Polymarket/Kalshi questions share a `cluster_id`.

Environment variables:

```
SEMANTIC_CLUSTER_ENABLED=true
SEMANTIC_CLUSTER_THRESHOLD=85
```

Notes:
- Clusters are assigned during ingestion and stored in the `whale_flows.cluster_id` column.
