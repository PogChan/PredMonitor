import asyncio
import logging
import os
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import aiohttp

from store.trade_store import SqliteTradeStore, Trade
from ingest.ingest_settings import Settings, load_settings
from ingest.ingest_utils import (
    parse_timestamp,
    normalize_market_id,
    normalize_wallet,
    normalize_side,
    extract_price,
    extract_quantity,
    extract_trade_id,
    extract_size_usd,
    backfill_trade_numbers,
)
from ingest.market_metadata import MarketMeta
from ingest.polymarket_ingest import polymarket_listener
from ingest.kalshi_ingest import kalshi_poller, kalshi_ws_listener
from market_classifier import MarketClassifier


LOG = logging.getLogger("whale_hunter.ingest")


@dataclass(frozen=True)
class TradeRecord:
    timestamp: float
    platform: str
    market: str
    size_usd: float
    side: str
    actor_address: Optional[str]


class SlidingWindowSum:
    def __init__(self, max_age_seconds: int) -> None:
        self.max_age_seconds = max_age_seconds
        self.items: Deque[Tuple[float, float]] = deque()
        self.total = 0.0

    def add(self, timestamp: float, value: float) -> float:
        self.items.append((timestamp, value))
        self.total += value
        self.prune(timestamp)
        return self.total

    def prune(self, now: float) -> None:
        cutoff = now - self.max_age_seconds
        while self.items and self.items[0][0] < cutoff:
            _, value = self.items.popleft()
            self.total -= value


class TradeWindow:
    def __init__(self, max_age_seconds: int) -> None:
        self.max_age_seconds = max_age_seconds
        self.items: Deque[TradeRecord] = deque()

    def add(self, record: TradeRecord) -> None:
        self.items.append(record)
        self.prune(record.timestamp)

    def prune(self, now: float) -> None:
        cutoff = now - self.max_age_seconds
        while self.items and self.items[0].timestamp < cutoff:
            self.items.popleft()


class RollingStatsWindow:
    def __init__(self, max_age_seconds: int, min_samples: int) -> None:
        self.max_age_seconds = max_age_seconds
        self.min_samples = min_samples
        self.items: Deque[Tuple[float, float]] = deque()
        self.sum = 0.0
        self.sum_sq = 0.0

    def add(self, timestamp: float, value: float) -> Optional[float]:
        self.items.append((timestamp, value))
        self.sum += value
        self.sum_sq += value * value
        self.prune(timestamp)
        count = len(self.items)
        if count < self.min_samples:
            return None
        mean = self.sum / count
        variance = max(self.sum_sq / count - mean * mean, 0.0)
        if variance <= 0.0:
            return None
        return (value - mean) / variance**0.5

    def prune(self, now: float) -> None:
        cutoff = now - self.max_age_seconds
        while self.items and self.items[0][0] < cutoff:
            _, value = self.items.popleft()
            self.sum -= value
            self.sum_sq -= value * value


class ZScoreDetector:
    def __init__(
        self, window_seconds: int, threshold: float, min_samples: int, cooldown_seconds: float
    ) -> None:
        self.window_seconds = window_seconds
        self.threshold = threshold
        self.min_samples = min_samples
        self.cooldown_seconds = cooldown_seconds
        self.windows: Dict[Tuple[str, str], RollingStatsWindow] = {}
        self.last_alert: Dict[Tuple[str, str], float] = {}

    def add_trade(self, platform: str, market: str, timestamp: float, size_usd: float) -> None:
        key = (platform, market)
        window = self.windows.get(key)
        if window is None:
            window = RollingStatsWindow(self.window_seconds, self.min_samples)
            self.windows[key] = window
        zscore = window.add(timestamp, size_usd)
        if zscore is None:
            return
        last_alert = self.last_alert.get(key, 0.0)
        if zscore >= self.threshold and timestamp - last_alert >= self.cooldown_seconds:
            self.last_alert[key] = timestamp
            LOG.warning(
                "Z-score whale spike platform=%s market=%s z=%.2f size_usd=%.2f",
                platform,
                market,
                zscore,
                size_usd,
            )


class SweepDetector:
    def __init__(self, window_ms: int, min_trades: int, cooldown_seconds: float) -> None:
        self.window_seconds = window_ms / 1000.0
        self.min_trades = min_trades
        self.cooldown_seconds = cooldown_seconds
        self.buffers: Dict[Tuple[str, str, str], Deque[Tuple[float, Optional[float], float]]] = {}
        self.last_alert: Dict[Tuple[str, str, str], float] = {}

    def add_trade(
        self,
        platform: str,
        market: str,
        side: str,
        timestamp: float,
        price: Optional[float],
        size_usd: float,
    ) -> None:
        key = (platform, market, side)
        buffer = self.buffers.setdefault(key, deque())
        buffer.append((timestamp, price, size_usd))
        cutoff = timestamp - self.window_seconds
        while buffer and buffer[0][0] < cutoff:
            buffer.popleft()
        if len(buffer) < self.min_trades:
            return
        prices = [item[1] for item in buffer if item[1] is not None]
        if len(prices) < 2:
            return
        if max(prices) == min(prices):
            return
        last_alert = self.last_alert.get(key, 0.0)
        if timestamp - last_alert < self.cooldown_seconds:
            return
        total_size = sum(item[2] for item in buffer)
        self.last_alert[key] = timestamp
        LOG.warning(
            "Sweep detected platform=%s market=%s side=%s trades=%d total_usd=%.2f",
            platform,
            market,
            side,
            len(buffer),
            total_size,
        )


class WalletVolumeTracker:
    def __init__(self, window_seconds: int, threshold_usd: float) -> None:
        self.window_seconds = window_seconds
        self.threshold_usd = threshold_usd
        self.wallets: Dict[str, SlidingWindowSum] = {}
        self.flagged: Set[str] = set()

    def add_trade(self, wallet: str, timestamp: float, size_usd: float) -> Tuple[bool, float]:
        if not wallet:
            return False, 0.0
        window = self.wallets.get(wallet)
        if window is None:
            window = SlidingWindowSum(self.window_seconds)
            self.wallets[wallet] = window
        total = window.add(timestamp, size_usd)
        if total >= self.threshold_usd and wallet not in self.flagged:
            self.flagged.add(wallet)
            return True, total
        if total < self.threshold_usd and wallet in self.flagged:
            self.flagged.remove(wallet)
        return False, total


class SmurfDetector:
    def __init__(
        self,
        trade_window_seconds: int,
        polymarket_window_seconds: int,
        polymarket_threshold_usd: float,
        kalshi_window_seconds: int,
        kalshi_threshold_usd: float,
        zscore_window_seconds: int,
        zscore_threshold: float,
        zscore_min_samples: int,
        zscore_cooldown_seconds: float,
        sweep_window_ms: int,
        sweep_min_trades: int,
        sweep_cooldown_seconds: float,
        store: Optional[SqliteTradeStore],
        persist_trades: bool,
        market_classifier: MarketClassifier,
    ) -> None:
        self.trade_window = TradeWindow(trade_window_seconds)
        self.wallet_tracker = WalletVolumeTracker(polymarket_window_seconds, polymarket_threshold_usd)
        self.kalshi_yes_window = SlidingWindowSum(kalshi_window_seconds)
        self.kalshi_threshold_usd = kalshi_threshold_usd
        self.kalshi_alert_active = False
        self.zscore_detector = ZScoreDetector(
            window_seconds=zscore_window_seconds,
            threshold=zscore_threshold,
            min_samples=zscore_min_samples,
            cooldown_seconds=zscore_cooldown_seconds,
        )
        self.sweep_detector = SweepDetector(
            window_ms=sweep_window_ms,
            min_trades=sweep_min_trades,
            cooldown_seconds=sweep_cooldown_seconds,
        )
        self.store = store
        self.persist_trades = persist_trades
        self.market_classifier = market_classifier
        self.market_metadata: Dict[Tuple[str, str], MarketMeta] = {}

    def update_market_metadata(self, platform: str, metadata: Dict[str, MarketMeta]) -> None:
        for key, meta in metadata.items():
            if key:
                self.market_metadata[(platform, str(key))] = meta

    def _lookup_market_meta(self, platform: str, keys: List[Optional[str]]) -> Optional[MarketMeta]:
        for key in keys:
            if not key:
                continue
            meta = self.market_metadata.get((platform, str(key)))
            if meta:
                return meta
        return None

    def _extract_market_label(self, trade: Dict[str, Any], fallback: str) -> str:
        for key in (
            "title",
            "question",
            "name",
            "subtitle",
            "market_slug",
            "marketSlug",
            "event_slug",
            "eventSlug",
            "slug",
            "market",
            "ticker",
            "market_ticker",
        ):
            value = trade.get(key)
            if value:
                return str(value)
        return fallback

    def handle_polymarket_trade(self, trade: Dict[str, Any]) -> None:
        timestamp = parse_timestamp(
            trade.get("timestamp")
            or trade.get("time")
            or trade.get("created_at")
            or trade.get("createdAt")
            or trade.get("ts")
        )
        market = normalize_market_id(trade)
        meta = self._lookup_market_meta(
            "polymarket",
            [
                market,
                trade.get("market_slug"),
                trade.get("marketSlug"),
                trade.get("event_slug"),
                trade.get("eventSlug"),
                trade.get("slug"),
            ],
        )
        label = self._extract_market_label(trade, market)
        if meta and meta.label:
            label = meta.label
        text_blob = meta.text_blob if meta and meta.text_blob else label
        classification = self.market_classifier.classify(
            text_blob, meta.volume if meta else None
        )
        taker = normalize_wallet(trade.get("taker_address") or trade.get("taker") or trade.get("takerAddress"))
        maker = normalize_wallet(trade.get("maker_address") or trade.get("maker") or trade.get("makerAddress"))
        side = normalize_side(trade.get("side") or trade.get("taker_side") or trade.get("takerSide"))
        price = extract_price(trade)
        quantity = extract_quantity(trade)
        trade_id = extract_trade_id(trade)
        size_usd = extract_size_usd(trade)
        if size_usd <= 0:
            return
        price, quantity = backfill_trade_numbers(size_usd, price, quantity)
        actor = taker or maker
        self.zscore_detector.add_trade("polymarket", market, timestamp, size_usd)
        self.sweep_detector.add_trade("polymarket", market, side, timestamp, price, size_usd)
        self.trade_window.add(
            TradeRecord(
                timestamp=timestamp,
                platform="polymarket",
                market=market,
                size_usd=size_usd,
                side=side,
                actor_address=actor,
            )
        )
        if self.store and self.persist_trades:
            self.store.add_trade(
                Trade(
                    timestamp=timestamp,
                    platform="polymarket",
                    market=market,
                    size_usd=size_usd,
                    side=side,
                    actor_address=actor,
                    price=price,
                    quantity=quantity,
                    trade_id=trade_id,
                    market_label=label,
                    market_is_niche=classification.is_niche,
                    market_is_stock=classification.is_stock,
                    market_volume=meta.volume if meta else None,
                )
            )
        for wallet in {taker, maker}:
            if not wallet:
                continue
            flagged, total = self.wallet_tracker.add_trade(wallet, timestamp, size_usd)
            if flagged:
                LOG.info(
                    "Whale flagged wallet=%s total_usd=%.2f window_hours=6 market=%s",
                    wallet,
                    total,
                    market,
                )

    def handle_kalshi_trade(self, trade: Dict[str, Any]) -> None:
        timestamp = parse_timestamp(
            trade.get("timestamp")
            or trade.get("time")
            or trade.get("created_time")
            or trade.get("createdAt")
            or trade.get("ts")
        )
        market = str(trade.get("market") or trade.get("ticker") or trade.get("market_ticker") or "")
        meta = self._lookup_market_meta(
            "kalshi",
            [
                market,
                trade.get("ticker"),
                trade.get("market_ticker"),
                trade.get("event_ticker"),
                trade.get("eventTicker"),
            ],
        )
        label = self._extract_market_label(trade, market)
        if meta and meta.label:
            label = meta.label
        text_blob = meta.text_blob if meta and meta.text_blob else label
        classification = self.market_classifier.classify(
            text_blob, meta.volume if meta else None
        )
        side = normalize_side(trade.get("side") or trade.get("taker_side") or "")
        price = extract_price(trade)
        quantity = extract_quantity(trade)
        trade_id = extract_trade_id(trade)
        size_usd = extract_size_usd(trade)
        if size_usd <= 0:
            return
        price, quantity = backfill_trade_numbers(size_usd, price, quantity)
        self.zscore_detector.add_trade("kalshi", market, timestamp, size_usd)
        self.sweep_detector.add_trade("kalshi", market, side, timestamp, price, size_usd)
        self.trade_window.add(
            TradeRecord(
                timestamp=timestamp,
                platform="kalshi",
                market=market,
                size_usd=size_usd,
                side=side,
                actor_address=None,
            )
        )
        if self.store and self.persist_trades:
            self.store.add_trade(
                Trade(
                    timestamp=timestamp,
                    platform="kalshi",
                    market=market,
                    size_usd=size_usd,
                    side=side,
                    actor_address=None,
                    price=price,
                    quantity=quantity,
                    trade_id=trade_id,
                    market_label=label,
                    market_is_niche=classification.is_niche,
                    market_is_stock=classification.is_stock,
                    market_volume=meta.volume if meta else None,
                )
            )
        if side == "yes":
            total = self.kalshi_yes_window.add(timestamp, size_usd)
            if total >= self.kalshi_threshold_usd and not self.kalshi_alert_active:
                self.kalshi_alert_active = True
                LOG.warning(
                    "Kalshi YES accumulation alert total_usd=%.2f window_hours=1 market=%s",
                    total,
                    market,
                )
            elif total < self.kalshi_threshold_usd and self.kalshi_alert_active:
                self.kalshi_alert_active = False


async def main() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    market_classifier = MarketClassifier.from_env()
    store = SqliteTradeStore(settings.trade_db_path) if settings.persist_trades else None
    detector = SmurfDetector(
        trade_window_seconds=settings.trade_window_seconds,
        polymarket_window_seconds=settings.polymarket_whale_window_seconds,
        polymarket_threshold_usd=settings.polymarket_whale_threshold_usd,
        kalshi_window_seconds=settings.kalshi_yes_window_seconds,
        kalshi_threshold_usd=settings.kalshi_yes_threshold_usd,
        zscore_window_seconds=settings.zscore_window_seconds,
        zscore_threshold=settings.zscore_threshold,
        zscore_min_samples=settings.zscore_min_samples,
        zscore_cooldown_seconds=settings.zscore_cooldown_seconds,
        sweep_window_ms=settings.sweep_window_ms,
        sweep_min_trades=settings.sweep_min_trades,
        sweep_cooldown_seconds=settings.sweep_cooldown_seconds,
        store=store,
        persist_trades=settings.persist_trades,
        market_classifier=market_classifier,
    )
    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        if settings.enable_polymarket:
            tasks.append(asyncio.create_task(polymarket_listener(session, settings, detector)))
        if settings.enable_kalshi:
            if settings.kalshi_ws_enabled:
                if settings.kalshi_access_key and settings.kalshi_private_key:
                    tasks.append(asyncio.create_task(kalshi_ws_listener(session, settings, detector)))
                else:
                    LOG.warning(
                        "Kalshi WS disabled: missing KALSHI_ACCESS_KEY or KALSHI_PRIVATE_KEY."
                    )
            if settings.kalshi_poll_enabled:
                tasks.append(asyncio.create_task(kalshi_poller(session, settings, detector)))
        if not tasks:
            LOG.warning("No ingestion tasks enabled. Set ENABLE_POLYMARKET or ENABLE_KALSHI.")
            return
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
