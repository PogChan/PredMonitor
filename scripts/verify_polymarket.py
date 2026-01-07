import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOG = logging.getLogger("verify_polymarket")

# Add current directory to path
sys.path.append(os.getcwd())

from ingest.ingest_settings import load_settings, Settings
from ingest.polymarket_ingest import polymarket_listener

@dataclass
class MockDetector:
    def handle_polymarket_trade(self, trade: Dict[str, Any]) -> None:
        LOG.info(f"Received Trade: {trade.get('market_id')} {trade.get('price')} {trade.get('size')}")

    def handle_kalshi_trade(self, trade: Dict[str, Any]) -> None:
        pass

    def update_market_metadata(self, platform: str, metadata: Dict[str, Any]) -> None:
        return None

async def main():
    settings = load_settings()
    # Override settings for quick test
    # e.g. set top N to 5 to avoid spamming
    # We can't easily modify the frozen dataclass, so we just rely on valid env or defaults.

    LOG.info("Starting Polymarket Verification...")

    detector = MockDetector()

    import aiohttp
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Run listener for a fixed duration then exit
        task = asyncio.create_task(polymarket_listener(session, settings, detector))

        # specific check: wait 15 seconds. If we see trades, we are good.
        await asyncio.sleep(15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    LOG.info("Verification Complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
