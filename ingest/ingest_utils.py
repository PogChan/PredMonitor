import json
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import parse_qsl

def parse_csv_env(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_timestamp(value: Any) -> float:
    if value is None:
        return time.time()
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1e12:
            timestamp /= 1000.0
        return timestamp
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(cleaned).timestamp()
        except ValueError:
            return time.time()
    return time.time()


def normalize_market_id(trade: Dict[str, Any]) -> str:
    for key in ("market", "market_id", "marketId", "condition_id", "conditionId", "id"):
        value = trade.get(key)
        if value:
            return str(value)
    return ""


def normalize_wallet(value: Any) -> str:
    if not value:
        return ""
    return str(value).lower()


def normalize_side(value: Any) -> str:
    if not value:
        return ""
    cleaned = str(value).strip().lower()
    if not cleaned:
        return ""
    parts = {part for part in re.split(r"[^a-z]+", cleaned) if part}
    if "sell" in parts and "no" in parts:
        return "yes"
    if "buy" in parts and "no" in parts:
        return "no"
    if "sell" in parts and "yes" in parts:
        return "no"
    if "buy" in parts and "yes" in parts:
        return "yes"
    if cleaned in {"buy", "bid", "long", "yes"}:
        return "yes"
    if cleaned in {"sell", "ask", "short", "no"}:
        return "no"
    return cleaned


def to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_price(value: Any) -> Optional[float]:
    price = to_float(value)
    if price is None:
        return None
    if price > 1.5:
        price = price / 100.0
    return price


def extract_quantity(trade: Dict[str, Any]) -> Optional[float]:
    for field in ("size", "trade_size", "quantity", "qty", "count"):
        value = to_float(trade.get(field))
        if value is not None:
            return value
    return None


def extract_price(trade: Dict[str, Any]) -> Optional[float]:
    for field in ("price", "price_usd", "priceUsd", "price_cents", "yes_price", "no_price"):
        price = normalize_price(trade.get(field))
        if price is not None:
            return price
    return None


def extract_trade_id(trade: Dict[str, Any]) -> Optional[str]:
    for field in ("trade_id", "id", "hash", "tx_hash", "txHash"):
        value = trade.get(field)
        if value:
            return str(value)
    return None


def backfill_trade_numbers(
    size_usd: float, price: Optional[float], quantity: Optional[float]
) -> Tuple[Optional[float], Optional[float]]:
    if size_usd <= 0:
        return price, quantity
    if price is None and quantity:
        price = size_usd / quantity
    if quantity is None and price:
        quantity = size_usd / price
    return price, quantity


def extract_size_usd(trade: Dict[str, Any]) -> float:
    direct_fields = ("size_usd", "sizeUsd", "volume_usd", "volumeUsd", "notional")
    for field in direct_fields:
        value = to_float(trade.get(field))
        if value is not None:
            return value
    size = extract_quantity(trade)
    price = extract_price(trade)
    if size is None or price is None:
        return 0.0
    if size <= 0 or price <= 0:
        return 0.0
    return size * price


def parse_query_params(value: str) -> Dict[str, str]:
    cleaned = value.strip()
    if not cleaned:
        return {}
    if cleaned.startswith("{"):
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                return {str(key): str(val) for key, val in parsed.items()}
            return {}
        except json.JSONDecodeError:
            return {}
    return {key: val for key, val in parse_qsl(cleaned, keep_blank_values=True)}


def normalize_filter_terms(values: Sequence[str]) -> List[str]:
    return [value.strip().lower() for value in values if value and value.strip()]


def build_text_blob(values: Iterable[Any]) -> str:
    parts: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if item is None:
                    continue
                parts.append(str(item))
            continue
        parts.append(str(value))
    return " ".join(parts).lower()


def match_any_keyword(text: str, keywords: Sequence[str]) -> bool:
    if not keywords or not text:
        return False
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def match_any_value(values: Iterable[str], keywords: Sequence[str]) -> bool:
    if not keywords:
        return False
    for value in values:
        if not value:
            continue
        lowered = str(value).lower()
        for keyword in keywords:
            if keyword in lowered:
                return True
    return False


def extract_tag_names(tags: Any) -> List[str]:
    if isinstance(tags, list):
        names: List[str] = []
        for tag in tags:
            if isinstance(tag, dict):
                name = tag.get("name") or tag.get("tag") or tag.get("label")
                if name:
                    names.append(str(name))
            elif tag:
                names.append(str(tag))
        return names
    if isinstance(tags, dict):
        name = tags.get("name") or tags.get("tag") or tags.get("label")
        return [str(name)] if name else []
    if tags:
        return [str(tags)]
    return []


def extract_company_match(text: str, company_terms: Sequence[str]) -> Optional[str]:
    if not company_terms or not text:
        return None
    lowered = text.lower()
    for term in company_terms:
        if term in lowered:
            return term
    return None
