import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from py_clob_client.client import ClobClient


GAMMA_API = os.getenv("POLYMARKET_GAMMA_API", "https://gamma-api.polymarket.com")
DATA_API = os.getenv("POLYMARKET_DATA_API", "https://data-api.polymarket.com")
CLOB_API = os.getenv("POLYMARKET_CLOB_API", "https://clob.polymarket.com")


def fetch_markets(
    limit: int = 10,
    active: bool = True,
    closed: bool = False,
    order: str = "volume24hr",
    ascending: bool = False,
    base_url: str = GAMMA_API,
    timeout_seconds: int = 15,
) -> List[Dict[str, Any]]:
    params = {
        "limit": limit,
        "active": active,
        "closed": closed,
        "order": order,
        "ascending": ascending,
    }
    response = requests.get(f"{base_url}/markets", params=params, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("markets", [])
    return []


def parse_clob_token_ids(market: Dict[str, Any]) -> List[str]:
    token_ids = market.get("clobTokenIds") or market.get("clob_token_ids") or []
    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except json.JSONDecodeError:
            return []
    if isinstance(token_ids, list):
        return [str(token_id) for token_id in token_ids]
    return []


def split_yes_no_token_ids(token_ids: Iterable[str]) -> Tuple[Optional[str], Optional[str]]:
    ids = list(token_ids)
    if len(ids) >= 2:
        return ids[0], ids[1]
    if len(ids) == 1:
        return ids[0], None
    return None, None


def get_order_book_snapshot(
    token_id: str,
    depth: int = 5,
    clob_api: str = CLOB_API,
) -> Dict[str, List[Dict[str, str]]]:
    client = ClobClient(clob_api)
    book = client.get_order_book(token_id)
    bids = sorted(book.bids, key=lambda item: _to_float(item.price), reverse=True)
    asks = sorted(book.asks, key=lambda item: _to_float(item.price))
    return {
        "market": str(book.market),
        "token_id": str(book.asset_id),
        "bids": [{"price": bid.price, "size": bid.size} for bid in bids[:depth]],
        "asks": [{"price": ask.price, "size": ask.size} for ask in asks[:depth]],
    }


def get_midpoint(token_id: str, clob_api: str = CLOB_API) -> Optional[float]:
    client = ClobClient(clob_api)
    midpoint = client.get_midpoint(token_id)
    return _to_float(midpoint.get("mid"))


def get_best_prices(token_id: str, clob_api: str = CLOB_API) -> Dict[str, Optional[float]]:
    client = ClobClient(clob_api)
    buy = client.get_price(token_id, side="BUY")
    sell = client.get_price(token_id, side="SELL")
    return {
        "buy": _to_float(buy.get("price")),
        "sell": _to_float(sell.get("price")),
    }


def get_spread(token_id: str, clob_api: str = CLOB_API) -> Optional[float]:
    client = ClobClient(clob_api)
    spread = client.get_spread(token_id)
    return _to_float(spread.get("spread"))


def track_price(
    token_id: str,
    duration_seconds: int = 30,
    interval_seconds: int = 5,
    clob_api: str = CLOB_API,
) -> List[float]:
    client = ClobClient(clob_api)
    start_time = time.time()
    prices: List[float] = []
    while time.time() - start_time < duration_seconds:
        midpoint = client.get_midpoint(token_id)
        price = _to_float(midpoint.get("mid"))
        if price is not None:
            prices.append(price)
        time.sleep(interval_seconds)
    return prices


def get_user_positions(
    wallet_address: str,
    base_url: str = DATA_API,
    timeout_seconds: int = 15,
) -> Dict[str, Any]:
    response = requests.get(
        f"{base_url}/positions",
        params={"user": wallet_address},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
