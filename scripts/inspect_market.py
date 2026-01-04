import aiohttp
import asyncio
import json
import logging
from websockets.asyncio.client import connect

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("inspect")

async def main():
    # 1. Fetch one active market
    async with aiohttp.ClientSession() as session:
        url = "https://gamma-api.polymarket.com/markets"
        params = {"limit": "1", "active": "true", "closed": "false", "volume_min": "10000"}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            if isinstance(data, list):
                market = data[0]
            else:
                market = data.get("markets", [])[0]

    LOG.info(f"Market Title: {market.get('question')}")
    LOG.info(f"Market ID (Condition ID?): {market.get('conditionId')}")
    LOG.info(f"Market clobTokenIds: {market.get('clobTokenIds')}")

    raw_ids = market.get('clobTokenIds') or []
    if isinstance(raw_ids, str):
        try:
             raw_ids = json.loads(raw_ids)
        except:
             raw_ids = []

    LOG.info(f"clobTokenIds parsed: {raw_ids}")

    if not raw_ids:
        LOG.error("No token IDs found!")
        return

    token_id = str(raw_ids[0])
    LOG.info(f"Using Token ID: {token_id}")

    # 2. Try to subscribe
    ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    LOG.info(f"Connecting to {ws_url}...")

    try:
        async with connect(ws_url) as websocket:
            # Send Subscribe
            payload = {"type": "subscribe", "channel": "trades", "market": token_id}
            LOG.info(f"Sending: {json.dumps(payload)}")
            await websocket.send(json.dumps(payload))

            # Listen for 30 seconds
            LOG.info("Listening...")
            for _ in range(30):
                try:
                    msg = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    LOG.info(f"Received: {msg}")
                except asyncio.TimeoutError:
                    pass
    except Exception as e:
        LOG.error(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
