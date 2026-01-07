import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from ingest.ingest_settings import Settings
from ingest.market_metadata import MarketMeta
from ingest.ingest_utils import (
    normalize_market_id,
    normalize_side,
    to_float,
    parse_query_params,
    normalize_filter_terms,
    build_text_blob,
    match_any_keyword,
    match_any_value,
    extract_tag_names,
    extract_company_match,
)

LOG = logging.getLogger("whale_hunter.polymarket")
DEFAULT_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WhaleHunter/1.0)"
}


async def polymarket_listener(
    session: aiohttp.ClientSession, settings: Settings, detector: Any
) -> None:
    mode = settings.polymarket_stream_mode.strip().lower()
    if mode == "rtds":
        await polymarket_rtds_listener(session, settings, detector)
        return
    await polymarket_clob_listener(session, settings, detector)


async def polymarket_rtds_listener(
    session: aiohttp.ClientSession, settings: Settings, detector: Any
) -> None:
    while True:
        event_slugs, metadata = await resolve_polymarket_event_slugs(session, settings)
        if metadata:
            detector.update_market_metadata("polymarket", metadata)
        if not event_slugs:
            LOG.warning("No Polymarket event slugs to subscribe to, retrying soon")
            await asyncio.sleep(30)
            continue
        shards = chunk_list(event_slugs, settings.polymarket_rtds_chunk_size)
        tasks = [
            asyncio.create_task(polymarket_rtds_worker(idx, shard, settings, detector))
            for idx, shard in enumerate(shards)
        ]
        await asyncio.gather(*tasks)
        await asyncio.sleep(5)


async def polymarket_rtds_worker(
    shard_id: int, event_slugs: Sequence[str], settings: Settings, detector: Any
) -> None:
    reconnect_delay = settings.polymarket_reconnect_min
    saw_first_message = False
    while True:
        headers = build_polymarket_auth_headers(settings)
        try:
            async with connect(
                settings.polymarket_rtds_url,
                additional_headers=headers or None,
                ping_interval=settings.polymarket_ping_interval,
                ping_timeout=settings.polymarket_ping_timeout,
            ) as websocket:
                await subscribe_polymarket_rtds(websocket, event_slugs, settings, shard_id)
                reconnect_delay = settings.polymarket_reconnect_min
                async for message in websocket:
                    if not saw_first_message:
                        LOG.info(
                            "Polymarket RTDS shard=%s first message=%s",
                            shard_id,
                            str(message)[:300],
                        )
                        saw_first_message = True
                    for trade in extract_polymarket_trades(message):
                        detector.handle_polymarket_trade(trade)
        except ConnectionClosedOK as exc:
            LOG.info(
                "Polymarket RTDS shard=%s closed code=%s reason=%s reconnecting in %.1fs",
                shard_id,
                exc.code,
                exc.reason,
                reconnect_delay,
            )
        except ConnectionClosedError as exc:
            LOG.info(
                "Polymarket RTDS shard=%s closed unexpectedly code=%s reason=%s reconnecting in %.1fs",
                shard_id,
                exc.code,
                exc.reason,
                reconnect_delay,
            )
        except Exception as exc:
            LOG.warning(
                "Polymarket RTDS shard=%s error=%s reconnecting in %.1fs",
                shard_id,
                exc,
                reconnect_delay,
            )
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, settings.polymarket_reconnect_max)


async def subscribe_polymarket_rtds(
    websocket: Any,
    event_slugs: Sequence[str],
    settings: Settings,
    shard_id: int,
) -> None:
    unique_slugs = list(dict.fromkeys(event_slugs))
    LOG.info(
        "Polymarket RTDS shard=%s subscribing to %d event slugs",
        shard_id,
        len(unique_slugs),
    )
    for slug in unique_slugs:
        payload = build_rtds_subscription(slug, settings)
        await websocket.send(json.dumps(payload))
        await asyncio.sleep(settings.polymarket_rtds_subscribe_pause)


def build_rtds_subscription(slug: str, settings: Settings) -> Dict[str, Any]:
    mode = settings.polymarket_rtds_subscribe_mode.strip().lower()
    if mode == "command":
        return {
            "type": "subscribe",
            "topic": settings.polymarket_rtds_topic,
            "event_slug": slug,
            "resources": [settings.polymarket_rtds_type],
        }
    return {
        "topic": settings.polymarket_rtds_topic,
        "type": settings.polymarket_rtds_type,
        "event_slug": slug,
    }


async def resolve_polymarket_event_slugs(
    session: aiohttp.ClientSession, settings: Settings
) -> Tuple[List[str], Dict[str, MarketMeta]]:
    if settings.polymarket_rtds_event_slugs:
        return settings.polymarket_rtds_event_slugs, {}
    if settings.polymarket_rtds_wildcard and not polymarket_filters_active(settings):
        return ["*"], {}
    return await fetch_polymarket_event_slugs(session, settings)


async def fetch_polymarket_event_slugs(
    session: aiohttp.ClientSession, settings: Settings
) -> Tuple[List[str], Dict[str, MarketMeta]]:
    slugs: List[str] = []
    metadata: Dict[str, MarketMeta] = {}
    offset = 0
    filters = build_polymarket_filters(settings)
    filters_active = polymarket_filters_active(settings, filters)
    for _ in range(settings.polymarket_events_max_pages):
        params = {
            "limit": str(settings.polymarket_events_limit),
            "offset": str(offset),
            "active": "true",
            "closed": "false",
        }
        params.update(settings.polymarket_events_params)
        try:
            async with session.get(
                settings.polymarket_events_url,
                params=params,
                headers=DEFAULT_HTTP_HEADERS,
            ) as response:
                if response.status >= 400:
                    LOG.warning("Polymarket events request failed status=%s", response.status)
                    break
                payload = await response.json(content_type=None)
        except Exception as exc:
            LOG.warning("Polymarket events request failed error=%s", exc)
            break
        items = extract_event_items(payload)
        if not items:
            break
        matched = 0
        matched_items: List[Dict[str, Any]] = []
        for item in items:
            if filters_active and not polymarket_item_matches(item, filters):
                continue
            slug = extract_event_slug(item)
            if slug:
                slugs.append(slug)
                matched += 1
                matched_items.append(item)
        metadata.update(
            build_polymarket_market_metadata(matched_items if filters_active else items)
        )
        if filters_active:
            LOG.info(
                "Polymarket events filtered matched=%d total=%d offset=%d",
                matched,
                len(items),
                offset,
            )
        offset += settings.polymarket_events_limit
    return list(dict.fromkeys(slugs)), metadata


def extract_event_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("events", "data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def extract_event_slug(item: Dict[str, Any]) -> str:
    for key in ("slug", "event_slug", "eventSlug", "event"):
        value = item.get(key)
        if value:
            return str(value)
    return ""


async def polymarket_clob_listener(
    session: aiohttp.ClientSession, settings: Settings, detector: Any
) -> None:
    while True:
        market_ids, metadata = await fetch_top_polymarket_market_ids(session, settings)
        if metadata:
            detector.update_market_metadata("polymarket", metadata)
        if not market_ids:
            LOG.warning("No Polymarket markets to subscribe to, retrying soon")
            await asyncio.sleep(30)
            continue
        subscribe_mode = settings.polymarket_subscribe_mode.strip().lower()
        if subscribe_mode in {"shard", "sharded"}:
            shards = chunk_list(market_ids, settings.polymarket_rtds_chunk_size)
            tasks = [
                asyncio.create_task(polymarket_clob_worker(idx, shard, settings, detector))
                for idx, shard in enumerate(shards)
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(5)
        elif subscribe_mode in {"single", "per_market", "market"}:
            tasks = [
                asyncio.create_task(
                    polymarket_clob_worker(market_id, [market_id], settings, detector)
                )
                for market_id in market_ids
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(5)
        else:
            await polymarket_clob_worker("single", market_ids, settings, detector)


async def polymarket_clob_worker(
    shard_id: Any, market_ids: Sequence[str], settings: Settings, detector: Any
) -> None:
    reconnect_delay = settings.polymarket_reconnect_min
    while True:
        headers = build_polymarket_auth_headers(settings)
        try:
            async with connect(
                settings.polymarket_ws_url,
                additional_headers=headers or None,
                ping_interval=settings.polymarket_ping_interval,
                ping_timeout=settings.polymarket_ping_timeout,
            ) as websocket:
                await subscribe_polymarket_clob(websocket, market_ids, settings, shard_id)
                reconnect_delay = settings.polymarket_reconnect_min
                async for message in websocket:
                    for trade in extract_polymarket_trades(message):
                        detector.handle_polymarket_trade(trade)
        except ConnectionClosedOK as exc:
            LOG.info(
                "Polymarket CLOB shard=%s closed code=%s reason=%s reconnecting in %.1fs",
                shard_id,
                exc.code,
                exc.reason,
                reconnect_delay,
            )
        except ConnectionClosedError as exc:
            LOG.info(
                "Polymarket CLOB shard=%s closed unexpectedly code=%s reason=%s reconnecting in %.1fs",
                shard_id,
                exc.code,
                exc.reason,
                reconnect_delay,
            )
        except Exception as exc:
            LOG.warning(
                "Polymarket CLOB shard=%s error=%s reconnecting in %.1fs",
                shard_id,
                exc,
                reconnect_delay,
            )
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, settings.polymarket_reconnect_max)


async def subscribe_polymarket_clob(
    websocket: Any, market_ids: Sequence[str], settings: Settings, shard_id: Any
) -> None:
    unique_ids = list(dict.fromkeys(market_ids))
    LOG.info(
        "Polymarket CLOB shard=%s subscribing to %d Token IDs",
        shard_id,
        len(unique_ids),
    )
    if unique_ids:
        LOG.info("Polymarket CLOB shard=%s sample Token IDs: %s", shard_id, unique_ids[:3])
    for market_id in unique_ids:
        payload = {"type": "subscribe", "channel": settings.polymarket_channel, "market": market_id}
        await websocket.send(json.dumps(payload))
        await asyncio.sleep(0.005)
    LOG.info("Polymarket CLOB shard=%s sent subscriptions", shard_id)


async def fetch_top_polymarket_market_ids(
    session: aiohttp.ClientSession, settings: Settings
) -> Tuple[List[str], Dict[str, MarketMeta]]:
    if settings.polymarket_market_ids:
        return settings.polymarket_market_ids, {}
    params = {"limit": str(settings.polymarket_top_n)}
    params.update(parse_query_params(os.getenv("POLYMARKET_MARKETS_PARAMS", "")))
    params.setdefault("active", "true")
    params.setdefault("closed", "false")
    try:
        async with session.get(
            settings.polymarket_markets_url,
            params=params,
            headers=DEFAULT_HTTP_HEADERS,
        ) as response:
            if response.status >= 400:
                LOG.warning("Polymarket markets request failed status=%s", response.status)
                return [], {}
            payload = await response.json(content_type=None)
    except Exception as exc:
        LOG.warning("Polymarket markets request failed error=%s", exc)
        return [], {}

    items = extract_market_items(payload)
    filters = build_polymarket_filters(settings)
    filters_active = polymarket_filters_active(settings, filters)
    active_items = [item for item in items if is_market_active(item)]
    if filters_active:
        active_items = [item for item in active_items if polymarket_item_matches(item, filters)]
    sorted_items = sorted(active_items, key=market_volume, reverse=True)
    metadata = build_polymarket_market_metadata(active_items)

    market_ids: List[str] = []
    for item in sorted_items:
        token_ids = item.get("clobTokenIds") or item.get("clob_token_ids")
        if token_ids and isinstance(token_ids, str):
            try:
                token_ids = json.loads(token_ids)
            except json.JSONDecodeError:
                token_ids = []

        if token_ids and isinstance(token_ids, list):
            for token_id in token_ids:
                if token_id:
                    market_ids.append(str(token_id))
        else:
            market_id = normalize_market_id(item)
            if market_id:
                market_ids.append(market_id)

    final_ids = []
    for item in sorted_items[: settings.polymarket_top_n]:
        token_ids = item.get("clobTokenIds") or item.get("clob_token_ids")
        if token_ids and isinstance(token_ids, str):
            try:
                token_ids = json.loads(token_ids)
            except json.JSONDecodeError:
                token_ids = []

        if token_ids and isinstance(token_ids, list):
            final_ids.extend([str(t) for t in token_ids])
        else:
            mid = normalize_market_id(item)
            if mid:
                final_ids.append(mid)

    return final_ids, metadata


def extract_market_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("markets", "data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def build_polymarket_market_metadata(items: Sequence[Dict[str, Any]]) -> Dict[str, MarketMeta]:
    metadata: Dict[str, MarketMeta] = {}
    for item in items:
        label = (
            item.get("title")
            or item.get("question")
            or item.get("name")
            or item.get("subtitle")
            or item.get("slug")
            or item.get("event_slug")
            or item.get("market_slug")
        )
        text_fields = extract_polymarket_text_fields(item)
        if label and label not in text_fields:
            text_fields.append(str(label))
        categories = extract_polymarket_categories(item)
        subcategories = extract_polymarket_subcategories(item)
        tags = extract_tag_names(item.get("tags") or item.get("tag") or item.get("tag_name"))
        text_blob = build_text_blob(text_fields + categories + subcategories + tags)
        volume = market_volume(item)
        meta = MarketMeta(label=str(label or ""), text_blob=text_blob, volume=volume)
        keys = set()
        market_id = normalize_market_id(item)
        if market_id:
            keys.add(market_id)
        for key in ("slug", "event_slug", "eventSlug", "market_slug", "marketSlug"):
            value = item.get(key)
            if value:
                keys.add(str(value))
        token_ids = item.get("clobTokenIds") or item.get("clob_token_ids")
        if token_ids and isinstance(token_ids, str):
            try:
                token_ids = json.loads(token_ids)
            except json.JSONDecodeError:
                token_ids = []
        if token_ids and isinstance(token_ids, list):
            keys.update([str(token_id) for token_id in token_ids if token_id])
        for key in keys:
            metadata[key] = meta
    return metadata


def build_polymarket_filters(settings: Settings) -> Dict[str, List[str]]:
    return {
        "keywords": normalize_filter_terms(settings.polymarket_event_keywords),
        "exclude_keywords": normalize_filter_terms(settings.polymarket_event_exclude_keywords),
        "categories": normalize_filter_terms(settings.polymarket_event_categories),
        "subcategories": normalize_filter_terms(settings.polymarket_event_subcategories),
        "tags": normalize_filter_terms(settings.polymarket_event_tags),
        "companies": normalize_filter_terms(settings.polymarket_event_companies),
    }


def polymarket_filters_active(
    settings: Settings, filters: Optional[Dict[str, List[str]]] = None
) -> bool:
    if filters is None:
        filters = build_polymarket_filters(settings)
    if settings.polymarket_events_params:
        return True
    return any(filters.values())


def polymarket_item_matches(item: Dict[str, Any], filters: Dict[str, List[str]]) -> bool:
    categories = extract_polymarket_categories(item)
    subcategories = extract_polymarket_subcategories(item)
    tags = extract_tag_names(item.get("tags") or item.get("tag") or item.get("tag_name"))
    text_fields = extract_polymarket_text_fields(item)
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


def extract_polymarket_text_fields(item: Dict[str, Any]) -> List[str]:
    fields: List[str] = []
    for key in (
        "title",
        "name",
        "question",
        "description",
        "subtitle",
        "slug",
        "event_slug",
        "eventSlug",
        "market_slug",
        "marketSlug",
    ):
        value = item.get(key)
        if value:
            fields.append(str(value))
    return fields


def extract_polymarket_categories(item: Dict[str, Any]) -> List[str]:
    categories: List[str] = []
    for key in ("category", "category_name", "categoryName"):
        value = item.get(key)
        if value:
            categories.append(str(value))
    return categories


def extract_polymarket_subcategories(item: Dict[str, Any]) -> List[str]:
    subcategories: List[str] = []
    for key in ("subcategory", "sub_category", "subcategory_name", "subcategoryName"):
        value = item.get(key)
        if value:
            subcategories.append(str(value))
    return subcategories


def is_market_active(item: Dict[str, Any]) -> bool:
    if "active" in item and not item.get("active"):
        return False
    if "closed" in item and item.get("closed"):
        return False
    if "archived" in item and item.get("archived"):
        return False
    return True


def market_volume(item: Dict[str, Any]) -> float:
    for key in ("volume24hr", "volume_24hr", "volume24h", "volume", "liquidity"):
        value = to_float(item.get(key))
        if value is not None:
            return value
    return 0.0


def extract_polymarket_trades(message: Any) -> List[Dict[str, Any]]:
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="ignore")
    if isinstance(message, str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return []
    elif isinstance(message, (dict, list)):
        payload = message
    else:
        return []

    if isinstance(payload, list):
        results = []
        for item in payload:
            results.extend(extract_polymarket_trades(item))
        return results

    event_type = normalize_side(
        payload.get("event")
        or payload.get("type")
        or payload.get("channel")
        or payload.get("topic")
        or payload.get("event_type")
    )
    if event_type and event_type not in {"trade", "trades", "activity"}:
        return []

    if event_type == "error":
        message = payload.get("message") or payload.get("error") or payload.get("reason")
        if message:
            LOG.warning("Polymarket websocket message error=%s", message)
        else:
            LOG.warning("Polymarket websocket message error payload=%s", str(payload)[:200])
        return []

    data = payload.get("data") or payload.get("trade") or payload.get("trades") or payload.get("payload")
    if isinstance(data, dict):
        nested = data.get("trades") or data.get("trade") or data.get("data")
        if isinstance(nested, dict):
            return [nested]
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(payload, dict) and looks_like_trade(payload):
        return [payload]
    return []


def looks_like_trade(payload: Dict[str, Any]) -> bool:
    keys = {
        "taker_address",
        "maker_address",
        "size",
        "price",
        "market",
        "market_id",
        "market_slug",
        "event_slug",
    }
    return any(key in payload for key in keys)


def chunk_list(items: Sequence[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        return [list(items)]
    return [list(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]


def build_polymarket_auth_headers(settings: Settings) -> Dict[str, str]:
    if not settings.polymarket_l2_enabled:
        return {}
    if not (
        settings.polymarket_l2_api_key
        and settings.polymarket_l2_api_secret
        and settings.polymarket_l2_passphrase
    ):
        return {}
    timestamp = str(int(time.time()))
    signature = build_polymarket_signature(
        timestamp,
        "GET",
        settings.polymarket_l2_request_path,
        "",
        settings.polymarket_l2_api_secret,
    )
    return {
        "Poly-Api-Key": settings.polymarket_l2_api_key,
        "Poly-Api-Passphrase": settings.polymarket_l2_passphrase,
        "Poly-Api-Timestamp": timestamp,
        "Poly-Api-Signature": signature,
    }


def build_polymarket_signature(
    timestamp: str, method: str, path: str, body: str, api_secret: str
) -> str:
    secret = decode_polymarket_api_secret(api_secret)
    prehash = f"{timestamp}{method.upper()}{path}{body}".encode("utf-8")
    digest = hmac.new(secret, prehash, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def decode_polymarket_api_secret(api_secret: str) -> bytes:
    cleaned = api_secret.strip()
    try:
        return base64.b64decode(cleaned)
    except (ValueError, binascii.Error):
        return cleaned.encode("utf-8")
