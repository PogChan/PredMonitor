from datetime import datetime, timezone, timedelta
from typing import Optional

def format_usd(value: Optional[float]) -> str:
    if value is None:
        return "--"
    return f"${value:,.0f}"

def format_time(timestamp: Optional[float]) -> str:
    if not timestamp:
        return "--"
    try:
        # Convert to UTC-aware datetime first
        dt_utc = datetime.fromtimestamp(timestamp, timezone.utc)
        # Adjust to User's local time (US/Eastern = UTC-5)
        # We use a fixed offset here based on the user's metadata to ensure correctness
        # regardless of the server/container timezone.
        dt_local = dt_utc.astimezone(timezone(timedelta(hours=-5)))
        return dt_local.strftime("%H:%M:%S (%Y-%m-%d)")
    except (ValueError, TypeError):
        return "--"

def format_price(value: Optional[float]) -> str:
    if value is None:
        return "--"
    return f"{value:.3f}"

def format_quantity(value: Optional[float]) -> str:
    if value is None:
        return "--"
    if value >= 1000:
        return f"{value:,.0f}"
    return f"{value:.2f}"

def shorten_address(address: str) -> str:
    if not address:
        return "anon"
    if len(address) <= 12:
        return address
    return f"{address[:6]}...{address[-4:]}"

def shorten_cluster_id(value: Optional[str]) -> str:
    if not value:
        return ""
    value = str(value)
    if len(value) <= 10:
        return value
    return f"{value[:4]}...{value[-4:]}"
