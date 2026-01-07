from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import re
from typing import List, Optional

from ingest.ingest_utils import normalize_filter_terms, parse_csv_env

DEFAULT_STOCK_KEYWORDS = [
    "earnings",
    "eps",
    "revenue",
    "guidance",
    "ipo",
    "stock",
    "shares",
    "share price",
    "dividend",
    "buyback",
    "split",
    "nasdaq",
    "s&p",
    "spx",
    "dow",
    "dow jones",
]

DEFAULT_NICHE_KEYWORDS = [
    "arrest",
    "indictment",
    "raid",
    "investigation",
    "whistleblower",
    "leak",
    "scandal",
    "coup",
    "assassination",
    "extradition",
    "sanction",
    "venezuela",
    "maduro",
    "bankruptcy",
    "default",
    "delist",
    "fraud",
    "subpoena",
    "sec",
    "doj",
]

DEFAULT_EXCLUDE_KEYWORDS = [
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "crypto",
    "super bowl",
    "nfl",
    "nba",
    "mlb",
    "nhl",
    "world cup",
    "champion",
    "playoff",
    "season",
    "ufc",
    "f1",
    "formula 1",
    "olympics",
    "soccer",
]

YEAR_PATTERN = re.compile(r"\b(20\d{2})\b")


@dataclass(frozen=True)
class MarketClassification:
    is_niche: bool
    is_stock: bool
    is_excluded: bool
    is_long_dated: bool
    matched_niche: List[str]
    matched_stock: List[str]
    matched_exclude: List[str]


@dataclass(frozen=True)
class MarketClassifierConfig:
    niche_keywords: List[str]
    stock_keywords: List[str]
    exclude_keywords: List[str]
    max_years_ahead: int
    niche_max_volume: Optional[float]


def _load_terms(env_name: str, defaults: List[str]) -> List[str]:
    raw = os.getenv(env_name, "").strip()
    if raw.lower() in {"none", "off", "false", "0"}:
        return []
    if raw:
        return normalize_filter_terms(parse_csv_env(raw))
    return normalize_filter_terms(defaults)


def _load_float(env_name: str) -> Optional[float]:
    raw = os.getenv(env_name, "").strip()
    if not raw:
        return None
    if raw.lower() in {"none", "off", "false"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _load_int(env_name: str, default: int) -> int:
    raw = os.getenv(env_name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class MarketClassifier:
    def __init__(self, config: MarketClassifierConfig) -> None:
        self.config = config

    @classmethod
    def from_env(cls) -> "MarketClassifier":
        config = MarketClassifierConfig(
            niche_keywords=_load_terms("MARKET_NICHE_KEYWORDS", DEFAULT_NICHE_KEYWORDS),
            stock_keywords=_load_terms("MARKET_STOCK_KEYWORDS", DEFAULT_STOCK_KEYWORDS),
            exclude_keywords=_load_terms("MARKET_EXCLUDE_KEYWORDS", DEFAULT_EXCLUDE_KEYWORDS),
            max_years_ahead=_load_int("MARKET_MAX_YEARS_AHEAD", 1),
            niche_max_volume=_load_float("MARKET_NICHE_MAX_VOLUME_USD"),
        )
        return cls(config)

    def classify(self, text: str, volume: Optional[float]) -> MarketClassification:
        text_blob = (text or "").lower()
        matched_niche = self._match_terms(text_blob, self.config.niche_keywords)
        matched_stock = self._match_terms(text_blob, self.config.stock_keywords)
        matched_exclude = self._match_terms(text_blob, self.config.exclude_keywords)
        is_long_dated = self._is_long_dated(text_blob)

        is_niche = bool(matched_niche)
        if volume is not None and self.config.niche_max_volume is not None:
            if volume <= self.config.niche_max_volume:
                is_niche = True

        is_stock = bool(matched_stock)
        is_excluded = bool(matched_exclude) or is_long_dated

        if is_excluded:
            is_niche = False
            is_stock = False

        return MarketClassification(
            is_niche=is_niche,
            is_stock=is_stock,
            is_excluded=is_excluded,
            is_long_dated=is_long_dated,
            matched_niche=matched_niche,
            matched_stock=matched_stock,
            matched_exclude=matched_exclude,
        )

    def _is_long_dated(self, text: str) -> bool:
        if self.config.max_years_ahead <= 0:
            return False
        current_year = datetime.utcnow().year
        max_year = current_year + self.config.max_years_ahead
        for match in YEAR_PATTERN.findall(text):
            try:
                year = int(match)
            except ValueError:
                continue
            if year > max_year:
                return True
        return False

    def _match_terms(self, text: str, terms: List[str]) -> List[str]:
        matches: List[str] = []
        for term in terms:
            if not term:
                continue
            if self._term_in_text(term, text):
                matches.append(term)
        return matches

    def _term_in_text(self, term: str, text: str) -> bool:
        if any(not char.isalnum() for char in term):
            return term in text
        if len(term) <= 3:
            return re.search(rf"\\b{re.escape(term)}\\b", text) is not None
        return term in text
