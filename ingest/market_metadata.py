from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MarketMeta:
    label: str
    text_blob: str
    volume: Optional[float]
    category: Optional[str] = None
