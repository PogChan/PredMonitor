from dataclasses import dataclass
from typing import Dict, List
import os
from ingest.ingest_utils import parse_csv_env, parse_bool_env, parse_query_params

@dataclass(frozen=True)
class Settings:
    polymarket_ws_url: str
    polymarket_markets_url: str
    polymarket_top_n: int
    polymarket_market_ids: List[str]
    polymarket_channel: str
    polymarket_subscribe_mode: str
    polymarket_stream_mode: str
    polymarket_rtds_url: str
    polymarket_rtds_topic: str
    polymarket_rtds_type: str
    polymarket_rtds_event_slugs: List[str]
    polymarket_rtds_wildcard: bool
    polymarket_rtds_chunk_size: int
    polymarket_rtds_subscribe_pause: float
    polymarket_rtds_subscribe_mode: str
    polymarket_events_url: str
    polymarket_events_limit: int
    polymarket_events_max_pages: int
    polymarket_events_params: Dict[str, str]
    polymarket_event_keywords: List[str]
    polymarket_event_exclude_keywords: List[str]
    polymarket_event_categories: List[str]
    polymarket_event_subcategories: List[str]
    polymarket_event_tags: List[str]
    polymarket_event_companies: List[str]
    polymarket_l2_enabled: bool
    polymarket_l2_api_key: str
    polymarket_l2_api_secret: str
    polymarket_l2_passphrase: str
    polymarket_l2_request_path: str
    polymarket_ping_interval: float
    polymarket_ping_timeout: float
    polymarket_reconnect_min: float
    polymarket_reconnect_max: float
    kalshi_trades_url: str
    kalshi_ws_url: str
    kalshi_ws_path: str
    kalshi_ws_enabled: bool
    kalshi_poll_enabled: bool
    kalshi_ws_channels: List[str]
    kalshi_market_tickers: List[str]
    kalshi_markets_url: str
    kalshi_markets_limit: int
    kalshi_markets_max_pages: int
    kalshi_markets_params: Dict[str, str]
    kalshi_market_keywords: List[str]
    kalshi_market_exclude_keywords: List[str]
    kalshi_market_categories: List[str]
    kalshi_market_subcategories: List[str]
    kalshi_market_tags: List[str]
    kalshi_market_companies: List[str]
    kalshi_access_key: str
    kalshi_private_key: str
    kalshi_signing_algo: str
    kalshi_poll_seconds: float
    http_timeout_seconds: float
    polymarket_whale_threshold_usd: float
    polymarket_whale_window_seconds: int
    kalshi_yes_threshold_usd: float
    kalshi_yes_window_seconds: int
    trade_window_seconds: int
    trade_db_path: str
    persist_trades: bool
    enable_polymarket: bool
    enable_kalshi: bool
    kalshi_reconnect_min: float
    kalshi_reconnect_max: float
    zscore_window_seconds: int
    zscore_threshold: float
    zscore_min_samples: int
    zscore_cooldown_seconds: float
    sweep_window_ms: int
    sweep_min_trades: int
    sweep_cooldown_seconds: float


def load_settings() -> Settings:
    return Settings(
        polymarket_ws_url=os.getenv(
            "POLYMARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        ),
        polymarket_markets_url=os.getenv(
            "POLYMARKET_MARKETS_URL", "https://gamma-api.polymarket.com/markets"
        ),
        polymarket_top_n=int(os.getenv("POLYMARKET_TOP_N", "50")),
        polymarket_market_ids=parse_csv_env(os.getenv("POLYMARKET_MARKET_IDS", "")),
        polymarket_channel=os.getenv("POLYMARKET_WS_CHANNEL", "trades"),
        polymarket_subscribe_mode=os.getenv("POLYMARKET_SUBSCRIBE_MODE", "bulk"),
        polymarket_stream_mode=os.getenv("POLYMARKET_STREAM_MODE", "rtds"),
        polymarket_rtds_url=os.getenv(
            "POLYMARKET_RTDS_URL", "wss://ws-live-data.polymarket.com"
        ),
        polymarket_rtds_topic=os.getenv("POLYMARKET_RTDS_TOPIC", "activity"),
        polymarket_rtds_type=os.getenv("POLYMARKET_RTDS_TYPE", "trades"),
        polymarket_rtds_event_slugs=parse_csv_env(os.getenv("POLYMARKET_RTDS_EVENT_SLUGS", "")),
        polymarket_rtds_wildcard=parse_bool_env(os.getenv("POLYMARKET_RTDS_WILDCARD", "true")),
        polymarket_rtds_chunk_size=int(os.getenv("POLYMARKET_RTDS_CHUNK_SIZE", "500")),
        polymarket_rtds_subscribe_pause=float(os.getenv("POLYMARKET_RTDS_SUBSCRIBE_PAUSE", "0.01")),
        polymarket_rtds_subscribe_mode=os.getenv("POLYMARKET_RTDS_SUBSCRIBE_MODE", "simple"),
        polymarket_events_url=os.getenv(
            "POLYMARKET_EVENTS_URL", "https://gamma-api.polymarket.com/events"
        ),
        polymarket_events_limit=int(os.getenv("POLYMARKET_EVENTS_LIMIT", "100")),
        polymarket_events_max_pages=int(os.getenv("POLYMARKET_EVENTS_MAX_PAGES", "50")),
        polymarket_events_params=parse_query_params(os.getenv("POLYMARKET_EVENTS_PARAMS", "")),
        polymarket_event_keywords=parse_csv_env(os.getenv("POLYMARKET_EVENT_KEYWORDS", "")),
        polymarket_event_exclude_keywords=parse_csv_env(
            os.getenv("POLYMARKET_EVENT_EXCLUDE_KEYWORDS", "")
        ),
        polymarket_event_categories=parse_csv_env(os.getenv("POLYMARKET_EVENT_CATEGORIES", "")),
        polymarket_event_subcategories=parse_csv_env(
            os.getenv("POLYMARKET_EVENT_SUBCATEGORIES", "")
        ),
        polymarket_event_tags=parse_csv_env(os.getenv("POLYMARKET_EVENT_TAGS", "")),
        polymarket_event_companies=parse_csv_env(os.getenv("POLYMARKET_EVENT_COMPANIES", "")),
        polymarket_l2_enabled=parse_bool_env(os.getenv("POLYMARKET_L2_ENABLED", "false")),
        polymarket_l2_api_key=os.getenv("POLYMARKET_API_KEY", ""),
        polymarket_l2_api_secret=os.getenv("POLYMARKET_API_SECRET", ""),
        polymarket_l2_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE", ""),
        polymarket_l2_request_path=os.getenv("POLYMARKET_L2_REQUEST_PATH", "/"),
        polymarket_ping_interval=float(os.getenv("POLYMARKET_PING_INTERVAL", "20")),
        polymarket_ping_timeout=float(os.getenv("POLYMARKET_PING_TIMEOUT", "20")),
        polymarket_reconnect_min=float(os.getenv("POLYMARKET_RECONNECT_MIN", "2")),
        polymarket_reconnect_max=float(os.getenv("POLYMARKET_RECONNECT_MAX", "60")),
        kalshi_trades_url=os.getenv(
            "KALSHI_TRADES_URL", "https://api.elections.kalshi.com/trade-api/v2/markets/trades"
        ),
        kalshi_ws_url=os.getenv(
            "KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2"
        ),
        kalshi_ws_path=os.getenv("KALSHI_WS_PATH", "/trade-api/ws/v2"),
        kalshi_ws_enabled=parse_bool_env(os.getenv("KALSHI_WS_ENABLED", "true")),
        kalshi_poll_enabled=parse_bool_env(os.getenv("KALSHI_POLL_ENABLED", "false")),
        kalshi_ws_channels=parse_csv_env(os.getenv("KALSHI_WS_CHANNELS", "trade")),
        kalshi_market_tickers=parse_csv_env(os.getenv("KALSHI_MARKET_TICKERS", "")),
        kalshi_markets_url=os.getenv(
            "KALSHI_MARKETS_URL", "https://api.elections.kalshi.com/trade-api/v2/markets"
        ),
        kalshi_markets_limit=int(os.getenv("KALSHI_MARKETS_LIMIT", "200")),
        kalshi_markets_max_pages=int(os.getenv("KALSHI_MARKETS_MAX_PAGES", "50")),
        kalshi_markets_params=parse_query_params(os.getenv("KALSHI_MARKETS_PARAMS", "")),
        kalshi_market_keywords=parse_csv_env(os.getenv("KALSHI_MARKET_KEYWORDS", "")),
        kalshi_market_exclude_keywords=parse_csv_env(
            os.getenv("KALSHI_MARKET_EXCLUDE_KEYWORDS", "")
        ),
        kalshi_market_categories=parse_csv_env(os.getenv("KALSHI_MARKET_CATEGORIES", "")),
        kalshi_market_subcategories=parse_csv_env(os.getenv("KALSHI_MARKET_SUBCATEGORIES", "")),
        kalshi_market_tags=parse_csv_env(os.getenv("KALSHI_MARKET_TAGS", "")),
        kalshi_market_companies=parse_csv_env(os.getenv("KALSHI_MARKET_COMPANIES", "")),
        kalshi_access_key=os.getenv("KALSHI_ACCESS_KEY", ""),
        kalshi_private_key=os.getenv("KALSHI_PRIVATE_KEY", ""),
        kalshi_signing_algo=os.getenv("KALSHI_SIGNING_ALGO", "ed25519"),
        kalshi_poll_seconds=float(os.getenv("KALSHI_POLL_SECONDS", "2")),
        http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "15")),
        polymarket_whale_threshold_usd=float(os.getenv("POLYMARKET_WHALE_THRESHOLD_USD", "10000")),
        polymarket_whale_window_seconds=int(os.getenv("POLYMARKET_WHALE_WINDOW_SECONDS", "21600")),
        kalshi_yes_threshold_usd=float(os.getenv("KALSHI_YES_THRESHOLD_USD", "50000")),
        kalshi_yes_window_seconds=int(os.getenv("KALSHI_YES_WINDOW_SECONDS", "3600")),
        trade_window_seconds=int(os.getenv("TRADE_WINDOW_SECONDS", "86400")),
        trade_db_path=os.getenv("TRADE_DB_PATH", "data/trades.db"),
        persist_trades=parse_bool_env(os.getenv("PERSIST_TRADES", "true")),
        enable_polymarket=parse_bool_env(os.getenv("ENABLE_POLYMARKET", "true")),
        enable_kalshi=parse_bool_env(os.getenv("ENABLE_KALSHI", "true")),
        kalshi_reconnect_min=float(os.getenv("KALSHI_RECONNECT_MIN", "2")),
        kalshi_reconnect_max=float(os.getenv("KALSHI_RECONNECT_MAX", "60")),
        zscore_window_seconds=int(os.getenv("ZSCORE_WINDOW_SECONDS", "3600")),
        zscore_threshold=float(os.getenv("ZSCORE_THRESHOLD", "3.0")),
        zscore_min_samples=int(os.getenv("ZSCORE_MIN_SAMPLES", "30")),
        zscore_cooldown_seconds=float(os.getenv("ZSCORE_COOLDOWN_SECONDS", "30")),
        sweep_window_ms=int(os.getenv("SWEEP_WINDOW_MS", "50")),
        sweep_min_trades=int(os.getenv("SWEEP_MIN_TRADES", "5")),
        sweep_cooldown_seconds=float(os.getenv("SWEEP_COOLDOWN_SECONDS", "1.0")),
    )
