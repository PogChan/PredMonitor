import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Sequence, Set, Tuple
import aiohttp
from websockets.asyncio.client import connect

from ingest.ingest_settings import Settings
from ingest.market_metadata import MarketMeta
from ingest.ingest_utils import (
    parse_timestamp,
    normalize_side,
    normalize_filter_terms,
    build_text_blob,
    match_any_keyword,
    match_any_value,
    extract_tag_names,
    extract_company_match,
)

LOG = logging.getLogger("whale_hunter.kalshi")

async def kalshi_ws_listener(
    session: aiohttp.ClientSession, settings: Settings, detector: Any
) -> None:
    reconnect_delay = settings.kalshi_reconnect_min
    while True:
        headers = build_kalshi_auth_headers(settings)
        if not headers:
            LOG.warning("Kalshi WS credentials missing; set KALSHI_ACCESS_KEY/KALSHI_PRIVATE_KEY.")
            await asyncio.sleep(30)
            continue
        market_tickers, metadata = await resolve_kalshi_market_tickers(session, settings)
        if metadata:
            detector.update_market_metadata("kalshi", metadata)
        if kalshi_filters_active(settings) and not market_tickers:
            LOG.warning("Kalshi market filter returned no tickers; retrying soon.")
            await asyncio.sleep(30)
            continue
        try:
            async with connect(
                settings.kalshi_ws_url,
                additional_headers=headers,
                ping_interval=settings.polymarket_ping_interval,
                ping_timeout=settings.polymarket_ping_timeout,
            ) as websocket:
                await kalshi_ws_subscribe(websocket, settings, market_tickers)
                reconnect_delay = settings.kalshi_reconnect_min
                async for message in websocket:
                    for trade in extract_kalshi_ws_trades(message):
                        detector.handle_kalshi_trade(trade)
        except Exception as exc:
            LOG.warning("Kalshi websocket error=%s reconnecting in %.1fs", exc, reconnect_delay)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, settings.kalshi_reconnect_max)


async def kalshi_poller(
    session: aiohttp.ClientSession, settings: Settings, detector: Any
) -> None:
    filters = build_kalshi_filters(settings)
    filters_active = kalshi_filters_active(settings, filters)
    allowed_markets: Set[str] = set(settings.kalshi_market_tickers)
    if filters_active and not allowed_markets:
        while True:
            tickers, metadata = await fetch_kalshi_market_tickers(session, settings, filters)
            allowed_markets = set(tickers)
            if metadata:
                detector.update_market_metadata("kalshi", metadata)
            if allowed_markets:
                break
            LOG.warning("Kalshi market filter returned no tickers; poller paused.")
            await asyncio.sleep(30)
    latest_timestamp = 0.0
    seen_trade_ids: Deque[str] = deque()
    seen_trade_id_set: Set[str] = set()
    seen_trade_ids_limit = 5000
    while True:
        try:
            async with session.get(settings.kalshi_trades_url) as response:
                if response.status >= 400:
                    LOG.warning("Kalshi trades request failed status=%s", response.status)
                    await asyncio.sleep(settings.kalshi_poll_seconds)
                    continue
                payload = await response.json(content_type=None)
        except Exception as exc:
            LOG.warning("Kalshi trades request failed error=%s", exc)
            await asyncio.sleep(settings.kalshi_poll_seconds)
            continue
        trades = extract_kalshi_trades(payload)
        for trade in trades:
            if allowed_markets:
                market = str(
                    trade.get("market") or trade.get("ticker") or trade.get("market_ticker") or ""
                )
                if market and market not in allowed_markets:
                    continue
            timestamp = parse_timestamp(
                trade.get("timestamp")
                or trade.get("time")
                or trade.get("created_time")
                or trade.get("createdAt")
                or trade.get("ts")
            )
            trade_id = trade.get("trade_id") or trade.get("id")
            if trade_id:
                trade_id = str(trade_id)
                if trade_id in seen_trade_id_set:
                    continue
            if timestamp < latest_timestamp:
                continue
            detector.handle_kalshi_trade(trade)
            latest_timestamp = max(latest_timestamp, timestamp)
            if trade_id:
                seen_trade_id_set.add(trade_id)
                seen_trade_ids.append(trade_id)
                while len(seen_trade_ids) > seen_trade_ids_limit:
                    dropped = seen_trade_ids.popleft()
                    seen_trade_id_set.discard(dropped)
        await asyncio.sleep(settings.kalshi_poll_seconds)


def build_kalshi_auth_headers(settings: Settings) -> Dict[str, str]:
    if not settings.kalshi_access_key or not settings.kalshi_private_key:
        return {}
    timestamp = str(int(time.time() * 1000))
    path = normalize_kalshi_path(settings.kalshi_ws_path)
    message = f"{timestamp}GET{path}"
    algo = resolve_kalshi_signing_algo(settings.kalshi_signing_algo, settings.kalshi_private_key)
    signature = sign_kalshi_message(message, settings.kalshi_private_key, algo)
    return {
        "KALSHI-ACCESS-KEY": settings.kalshi_access_key,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


def normalize_kalshi_path(path: str) -> str:
    cleaned = (path or "").strip()
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned


def resolve_kalshi_signing_algo(algo: str, private_key: str) -> str:
    cleaned = (algo or "").strip().lower()
    if cleaned in {"rsa-pss", "rsa_pss", "rsapss"}:
        return "rsa-pss"
    if cleaned in {"hmac-sha256", "ed25519"}:
        if cleaned == "ed25519" and looks_like_rsa_private_key(private_key):
            LOG.warning("Kalshi key looks like RSA; overriding KALSHI_SIGNING_ALGO to rsa-pss.")
            return "rsa-pss"
        return cleaned
    if looks_like_rsa_private_key(private_key):
        return "rsa-pss"
    return "ed25519"


def sign_kalshi_message(message: str, private_key: str, algo: str) -> str:
    algo = algo.strip().lower()
    if algo == "hmac-sha256":
        digest = hmac.new(private_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")
    if algo == "rsa-pss":
        return sign_kalshi_rsa_pss(message, private_key)
    if algo != "ed25519":
        raise ValueError(f"Unsupported KALSHI_SIGNING_ALGO={algo}")
    try:
        from nacl.signing import SigningKey
    except ImportError as exc:
        raise ImportError("pynacl is required for ed25519 Kalshi signing") from exc
    key_bytes = decode_kalshi_private_key(private_key)
    signing_key = SigningKey(key_bytes)
    signature = signing_key.sign(message.encode("utf-8")).signature
    return base64.b64encode(signature).decode("utf-8")


def decode_kalshi_private_key(private_key: str) -> bytes:
    cleaned = private_key.strip()
    if cleaned.startswith("0x"):
        cleaned = cleaned[2:]
    try:
        return bytes.fromhex(cleaned)
    except ValueError:
        return base64.b64decode("".join(cleaned.split()))


def looks_like_rsa_private_key(private_key: str) -> bool:
    cleaned = (private_key or "").strip()
    if "BEGIN RSA PRIVATE KEY" in cleaned or "BEGIN PRIVATE KEY" in cleaned:
        return True
    compact = "".join(cleaned.split())
    return len(compact) > 128


def sign_kalshi_rsa_pss(message: str, private_key: str) -> str:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except ImportError as exc:
        raise ImportError("cryptography is required for rsa-pss Kalshi signing") from exc
    key = load_rsa_private_key(private_key)
    signature = key.sign(
        message.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def load_rsa_private_key(private_key: str):
    from cryptography.hazmat.primitives import serialization

    cleaned = (private_key or "").strip()
    if "BEGIN" in cleaned:
        key_bytes = cleaned.encode("utf-8")
        try:
            return serialization.load_pem_private_key(key_bytes, password=None)
        except ValueError as exc:
            raise ValueError("Invalid PEM encoded RSA private key") from exc
    compact = "".join(cleaned.split())
    try:
        key_bytes = base64.b64decode(compact)
    except (binascii.Error, ValueError):
        key_bytes = bytes.fromhex(compact)
    try:
        return serialization.load_der_private_key(key_bytes, password=None)
    except ValueError:
        return serialization.load_pem_private_key(key_bytes, password=None)


async def kalshi_ws_subscribe(
    websocket: Any, settings: Settings, market_tickers: Sequence[str]
) -> None:
    params: Dict[str, Any] = {"channels": settings.kalshi_ws_channels}
    if market_tickers:
        if len(market_tickers) == 1:
            params["market_ticker"] = market_tickers[0]
        else:
            params["market_tickers"] = list(market_tickers)
    subscription = {"id": 1, "cmd": "subscribe", "params": params}
    await websocket.send(json.dumps(subscription))


def extract_kalshi_ws_trades(message: Any) -> List[Dict[str, Any]]:
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="ignore")
    if isinstance(message, str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return []
    elif isinstance(message, dict):
        payload = message
    else:
        return []
    msg_type = normalize_side(payload.get("type") or payload.get("channel") or "")
    if msg_type and msg_type not in {"trade", "trades"}:
        return []
    data = payload.get("data") or payload.get("trade") or payload.get("trades") or payload.get("payload")
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def extract_kalshi_trades(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        trades = payload.get("trades") or payload.get("data") or payload.get("results")
        if isinstance(trades, list):
            return [trade for trade in trades if isinstance(trade, dict)]
        return []
    if isinstance(payload, list):
        return [trade for trade in payload if isinstance(trade, dict)]
    return []


def build_kalshi_filters(settings: Settings) -> Dict[str, List[str]]:
    return {
        "keywords": normalize_filter_terms(settings.kalshi_market_keywords),
        "exclude_keywords": normalize_filter_terms(settings.kalshi_market_exclude_keywords),
        "categories": normalize_filter_terms(settings.kalshi_market_categories),
        "subcategories": normalize_filter_terms(settings.kalshi_market_subcategories),
        "tags": normalize_filter_terms(settings.kalshi_market_tags),
        "companies": normalize_filter_terms(settings.kalshi_market_companies),
    }


def kalshi_filters_active(settings: Settings, filters: Optional[Dict[str, List[str]]] = None) -> bool:
    if filters is None:
        filters = build_kalshi_filters(settings)
    return any(filters.values())


async def resolve_kalshi_market_tickers(
    session: aiohttp.ClientSession, settings: Settings
) -> Tuple[List[str], Dict[str, MarketMeta]]:
    if settings.kalshi_market_tickers:
        return settings.kalshi_market_tickers, {}
    if not kalshi_filters_active(settings):
        return [], {}
    filters = build_kalshi_filters(settings)
    return await fetch_kalshi_market_tickers(session, settings, filters)


async def fetch_kalshi_market_tickers(
    session: aiohttp.ClientSession, settings: Settings, filters: Dict[str, List[str]]
) -> Tuple[List[str], Dict[str, MarketMeta]]:
    tickers: List[str] = []
    metadata: Dict[str, MarketMeta] = {}
    cursor = ""
    for _ in range(settings.kalshi_markets_max_pages):
        params: Dict[str, str] = {"limit": str(settings.kalshi_markets_limit)}
        params.update(settings.kalshi_markets_params)
        if cursor:
            params.setdefault("cursor", cursor)
        try:
            async with session.get(settings.kalshi_markets_url, params=params) as response:
                if response.status >= 400:
                    LOG.warning("Kalshi markets request failed status=%s", response.status)
                    break
                payload = await response.json(content_type=None)
        except Exception as exc:
            LOG.warning("Kalshi markets request failed error=%s", exc)
            break
        items = extract_kalshi_market_items(payload)
        if not items:
            break
        matched = 0
        matched_items: List[Dict[str, Any]] = []
        for item in items:
            if not kalshi_market_matches(item, filters):
                continue
            ticker = extract_kalshi_market_ticker(item)
            if ticker:
                tickers.append(ticker)
                matched += 1
                matched_items.append(item)
        LOG.info(
            "Kalshi markets filtered matched=%d total=%d cursor=%s",
            matched,
            len(items),
            cursor or "start",
        )
        metadata.update(build_kalshi_market_metadata(matched_items))
        cursor = extract_kalshi_next_cursor(payload)
        if not cursor:
            break
    return list(dict.fromkeys(tickers)), metadata


def extract_kalshi_market_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("markets", "data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def extract_kalshi_market_ticker(item: Dict[str, Any]) -> str:
    for key in ("ticker", "market_ticker", "marketTicker", "symbol", "id"):
        value = item.get(key)
        if value:
            return str(value)
    return ""


def extract_kalshi_text_fields(item: Dict[str, Any]) -> List[str]:
    fields: List[str] = []
    for key in (
        "title",
        "subtitle",
        "description",
        "question",
        "ticker",
        "market_ticker",
        "event_ticker",
    ):
        value = item.get(key)
        if value:
            fields.append(str(value))
    return fields


def extract_kalshi_categories(item: Dict[str, Any]) -> List[str]:
    categories: List[str] = []
    for key in ("category", "category_name", "categoryName", "series"):
        value = item.get(key)
        if value:
            categories.append(str(value))
    return categories


def extract_kalshi_subcategories(item: Dict[str, Any]) -> List[str]:
    subcategories: List[str] = []
    for key in ("subcategory", "sub_category", "subcategory_name", "subcategoryName"):
        value = item.get(key)
        if value:
            subcategories.append(str(value))
    return subcategories


def kalshi_market_volume(item: Dict[str, Any]) -> Optional[float]:
    for key in (
        "volume_24h",
        "volume24h",
        "volume",
        "open_interest",
        "openInterest",
        "open_interest_usd",
    ):
        value = item.get(key)
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def build_kalshi_market_metadata(items: Sequence[Dict[str, Any]]) -> Dict[str, MarketMeta]:
    metadata: Dict[str, MarketMeta] = {}
    for item in items:
        label = (
            item.get("title")
            or item.get("question")
            or item.get("subtitle")
            or item.get("ticker")
            or item.get("market_ticker")
        )
        text_fields = extract_kalshi_text_fields(item)
        if label and label not in text_fields:
            text_fields.append(str(label))
        categories = extract_kalshi_categories(item)
        subcategories = extract_kalshi_subcategories(item)
        tags = extract_tag_names(item.get("tags") or item.get("tag") or item.get("tag_name"))
        text_blob = build_text_blob(text_fields + categories + subcategories + tags)
        volume = kalshi_market_volume(item)
        meta = MarketMeta(label=str(label or ""), text_blob=text_blob, volume=volume)
        keys = set()
        ticker = extract_kalshi_market_ticker(item)
        if ticker:
            keys.add(ticker)
        event_ticker = item.get("event_ticker") or item.get("eventTicker")
        if event_ticker:
            keys.add(str(event_ticker))
        for key in keys:
            metadata[key] = meta
    return metadata


def extract_kalshi_next_cursor(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("next_cursor", "next", "cursor", "nextCursor", "next_token", "nextToken"):
            value = payload.get(key)
            if value:
                return str(value)
    return ""


def kalshi_market_matches(item: Dict[str, Any], filters: Dict[str, List[str]]) -> bool:
    categories = extract_kalshi_categories(item)
    subcategories = extract_kalshi_subcategories(item)
    tags = extract_tag_names(item.get("tags") or item.get("tag") or item.get("tag_name"))
    text_fields = extract_kalshi_text_fields(item)
    text_blob = build_text_blob(text_fields + categories + subcategories + tags)

    if filters["exclude_keywords"] and match_any_keyword(text_blob, filters["exclude_keywords"]):
        return False
    if filters["categories"] and not match_any_value(categories, filters["categories"]):
        return False
    if filters["subcategories"] and not match_any_value(subcategories, filters["subcategories"]):
        return False
    if filters["tags"] and not match_any_value(tags, filters["tags"]):
        return False
    if filters["keywords"] and not match_any_keyword(text_blob, filters["keywords"]):
        return False
    if filters["companies"] and not extract_company_match(text_blob, filters["companies"]):
        return False
    return True
