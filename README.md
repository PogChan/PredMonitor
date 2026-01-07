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
