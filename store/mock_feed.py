import random
import threading
import time
from typing import Optional
from store.trade_store import Trade

def build_mock_trade(rng: random.Random) -> Trade:
    markets = [
        "BTC > 50K - End of Month",
        "ETH Spot > 3K",
        "US Election - 2024",
        "Fed Cut By September",
        "Solana > 200",
        "Tech Rally Q4",
    ]
    whales = [
        "0x9f8c4a1d2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e",
        "0x3b7a1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b",
    ]
    wallets = whales + [
        "0x1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d",
        "0x7f6e5d4c3b2a1908f7e6d5c4b3a291807f6e5d4c3",
        "0x5a4b3c2d1e0f9a8b7c6d5e4f3a2b1c0d9e8f7a6b",
        "0x8f7e6d5c4b3a291807f6e5d4c3b2a1908f7e6d5c4",
    ]
    size_usd = float(rng.choice([120, 420, 760, 1100, 2600, 7400, 12500]))
    side = rng.choice(["yes", "no"])
    platform = rng.choice(["polymarket", "kalshi"])
    actor = rng.choice(wallets if platform == "polymarket" else [None, None, None])
    price = round(rng.uniform(0.15, 0.85), 3)
    quantity = round(size_usd / price, 2)
    return Trade(
        timestamp=time.time(),
        platform=platform,
        market=rng.choice(markets),
        size_usd=size_usd,
        side=side,
        actor_address=actor,
        price=price,
        quantity=quantity,
    )

def run_mock_feed(store, interval_seconds=(0.6, 1.3)):
    rng = random.Random()
    while True:
        store.add_trade(build_mock_trade(rng))
        if rng.random() < 0.18:
            store.add_trade(build_mock_trade(rng))
        time.sleep(rng.uniform(*interval_seconds))

def start_mock_feed_thread(store, daemon=True):
    thread = threading.Thread(target=run_mock_feed, args=(store,), daemon=daemon)
    thread.start()
    return thread
