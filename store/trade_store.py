import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Trade:
    timestamp: float
    platform: str
    market: str
    size_usd: float
    side: str
    actor_address: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    trade_id: Optional[str] = None
    market_label: Optional[str] = None
    market_is_niche: Optional[bool] = None
    market_is_stock: Optional[bool] = None
    market_volume: Optional[float] = None
    cluster_id: Optional[str] = None


class InMemoryTradeStore:
    def __init__(self, maxlen: int = 2000) -> None:
        self._maxlen = maxlen
        self._trades: List[Trade] = []
        self._lock = threading.Lock()

    def add_trade(self, trade: Trade) -> None:
        if trade.size_usd < 100:
            return
        with self._lock:
            self._trades.append(trade)
            if len(self._trades) > self._maxlen:
                self._trades = self._trades[-self._maxlen :]

    def recent_trades(
        self,
        min_size_usd: float,
        limit: int,
        since_ts: Optional[float] = None,
        platforms: Optional[List[str]] = None,
        wallet: Optional[str] = None,
    ) -> List[Trade]:
        with self._lock:
            trades = [trade for trade in self._trades if trade.size_usd >= min_size_usd]
        if since_ts is not None:
            trades = [trade for trade in trades if trade.timestamp >= since_ts]
        if platforms:
            allowed = {platform.lower() for platform in platforms}
            trades = [
                trade
                for trade in trades
                if (trade.platform or "").lower() in allowed
            ]
        if wallet:
            trades = [trade for trade in trades if trade.actor_address == wallet]
        return list(reversed(trades))[:limit]

    def stats(self) -> Dict[str, str]:
        now = time.time()
        cutoff_24h = now - 86400
        cutoff_minute = now - 60
        with self._lock:
            trades = list(self._trades)
        trades_24h = [trade for trade in trades if trade.timestamp >= cutoff_24h]
        trades_minute = [trade for trade in trades if trade.timestamp >= cutoff_minute]
        wallets = {trade.actor_address for trade in trades_24h if trade.actor_address}
        last_trade = max(trades, key=lambda item: item.timestamp, default=None)
        return {
            "wallets": f"{len(wallets):,}",
            "trades": f"{len(trades_24h):,}",
            "flow": f"{len(trades_minute):,}/min",
            "last": last_trade.timestamp if last_trade else None,
        }

    def leaderboard(self, limit: int, since_ts: Optional[float] = None) -> List[Dict[str, str]]:
        now = time.time()
        cutoff = since_ts if since_ts is not None else now - 86400
        totals: Dict[str, Dict[str, float]] = {}
        with self._lock:
            trades = list(self._trades)
        for trade in trades:
            if not trade.actor_address or trade.timestamp < cutoff:
                continue
            stats = totals.setdefault(trade.actor_address, {"volume": 0.0, "yes": 0.0, "no": 0.0})
            stats["volume"] += trade.size_usd
            side = (trade.side or "").lower()
            if side in {"yes", "buy"}:
                stats["yes"] += trade.size_usd
            elif side in {"no", "sell"}:
                stats["no"] += trade.size_usd
        ranked = sorted(totals.items(), key=lambda item: item[1]["volume"], reverse=True)[:limit]
        results = []
        for wallet, stats in ranked:
            if stats["yes"] == 0 and stats["no"] == 0:
                position = "N/A"
            else:
                position = "YES" if stats["yes"] >= stats["no"] else "NO"
            results.append(
                {
                    "address": wallet,
                    "volume": stats["volume"],
                    "position": position,
                }
            )
        return results

    def wallet_summary(self, wallet: str, since_ts: Optional[float] = None) -> Optional[Dict[str, float]]:
        if not wallet:
            return None
        now = time.time()
        cutoff = since_ts if since_ts is not None else now - 86400
        with self._lock:
            trades = [trade for trade in self._trades if trade.actor_address == wallet]
        if cutoff is not None:
            trades = [trade for trade in trades if trade.timestamp >= cutoff]
        if not trades:
            return None
        yes_volume = sum(
            trade.size_usd for trade in trades if (trade.side or "").lower() in {"yes", "buy"}
        )
        no_volume = sum(
            trade.size_usd for trade in trades if (trade.side or "").lower() in {"no", "sell"}
        )
        total_volume = sum(trade.size_usd for trade in trades)
        last_ts = max(trade.timestamp for trade in trades)
        return {
            "trades": float(len(trades)),
            "volume": total_volume,
            "yes_volume": yes_volume,
            "no_volume": no_volume,
            "last_ts": last_ts,
        }


class SqliteTradeStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS whale_flows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    platform TEXT NOT NULL,
                    market TEXT,
                    market_label TEXT,
                    size_usd REAL NOT NULL,
                    side TEXT,
                    actor_address TEXT,
                    price REAL,
                    quantity REAL,
                    trade_id TEXT,
                    market_is_niche INTEGER,
                    market_is_stock INTEGER,
                    market_volume REAL,
                    cluster_id TEXT,
                    UNIQUE(platform, trade_id) ON CONFLICT IGNORE
                )
                """
            )
            existing = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(whale_flows)").fetchall()
            }
            self._add_column_if_missing(conn, existing, "market_label", "TEXT")
            self._add_column_if_missing(conn, existing, "market_is_niche", "INTEGER")
            self._add_column_if_missing(conn, existing, "market_is_stock", "INTEGER")
            self._add_column_if_missing(conn, existing, "market_volume", "REAL")
            self._add_column_if_missing(conn, existing, "cluster_id", "TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_whale_flows_ts ON whale_flows(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_whale_flows_actor ON whale_flows(actor_address)")

    def _add_column_if_missing(
        self, conn: sqlite3.Connection, existing: set, name: str, ddl: str
    ) -> None:
        if name not in existing:
            conn.execute(f"ALTER TABLE whale_flows ADD COLUMN {name} {ddl}")

    def _bool_to_int(self, value: Optional[bool]) -> Optional[int]:
        if value is None:
            return None
        return 1 if value else 0

    def _int_to_bool(self, value: Optional[int]) -> Optional[bool]:
        if value is None:
            return None
        return bool(value)

    def add_trade(self, trade: Trade) -> None:
        if trade.size_usd < 100:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO whale_flows (
                    timestamp,
                    platform,
                    market,
                    market_label,
                    size_usd,
                    side,
                    actor_address,
                    price,
                    quantity,
                    trade_id,
                    market_is_niche,
                    market_is_stock,
                    market_volume,
                    cluster_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.timestamp,
                    trade.platform,
                    trade.market,
                    trade.market_label,
                    trade.size_usd,
                    trade.side,
                    trade.actor_address,
                    trade.price,
                    trade.quantity,
                    trade.trade_id,
                    self._bool_to_int(trade.market_is_niche),
                    self._bool_to_int(trade.market_is_stock),
                    trade.market_volume,
                    trade.cluster_id,
                ),
            )

    def recent_trades(
        self,
        min_size_usd: float,
        limit: int,
        since_ts: Optional[float] = None,
        platforms: Optional[List[str]] = None,
        wallet: Optional[str] = None,
    ) -> List[Trade]:
        where = ["size_usd >= ?"]
        params: List[object] = [min_size_usd]
        if since_ts is not None:
            where.append("timestamp >= ?")
            params.append(since_ts)
        if platforms:
            placeholders = ", ".join(["?"] * len(platforms))
            where.append(f"lower(platform) IN ({placeholders})")
            params.extend([platform.lower() for platform in platforms])
        if wallet:
            where.append("actor_address = ?")
            params.append(wallet)
        query = f"""
            SELECT timestamp, platform, market, market_label, size_usd, side, actor_address,
                   price, quantity, trade_id, market_is_niche, market_is_stock, market_volume,
                   cluster_id
            FROM whale_flows
            WHERE {" AND ".join(where)}
            ORDER BY timestamp DESC
            LIMIT ?
        """
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            Trade(
                timestamp=row["timestamp"],
                platform=row["platform"],
                market=row["market"] or "",
                market_label=row["market_label"],
                size_usd=row["size_usd"],
                side=row["side"] or "",
                actor_address=row["actor_address"],
                price=row["price"],
                quantity=row["quantity"],
                trade_id=row["trade_id"],
                market_is_niche=self._int_to_bool(row["market_is_niche"]),
                market_is_stock=self._int_to_bool(row["market_is_stock"]),
                market_volume=row["market_volume"],
                cluster_id=row["cluster_id"],
            )
            for row in rows
        ]

    def stats(self) -> Dict[str, str]:
        now = time.time()
        cutoff_24h = now - 86400
        cutoff_minute = now - 60
        with self._connect() as conn:
            trades_24h = conn.execute(
                "SELECT COUNT(*) AS count FROM whale_flows WHERE timestamp >= ?",
                (cutoff_24h,),
            ).fetchone()["count"]
            trades_minute = conn.execute(
                "SELECT COUNT(*) AS count FROM whale_flows WHERE timestamp >= ?",
                (cutoff_minute,),
            ).fetchone()["count"]
            wallets = conn.execute(
                """
                SELECT COUNT(DISTINCT actor_address) AS count
                FROM whale_flows
                WHERE timestamp >= ? AND actor_address IS NOT NULL AND actor_address != ''
                """,
                (cutoff_24h,),
            ).fetchone()["count"]
            last = conn.execute(
                "SELECT MAX(timestamp) AS last_ts FROM whale_flows"
            ).fetchone()["last_ts"]
        return {
            "wallets": f"{wallets:,}",
            "trades": f"{trades_24h:,}",
            "flow": f"{trades_minute:,}/min",
            "last": last,
        }

    def leaderboard(self, limit: int, since_ts: Optional[float] = None) -> List[Dict[str, str]]:
        now = time.time()
        cutoff = since_ts if since_ts is not None else now - 86400
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT actor_address,
                       SUM(size_usd) AS volume,
                       SUM(CASE WHEN lower(side) IN ('yes', 'buy') THEN size_usd ELSE 0 END) AS yes_volume,
                       SUM(CASE WHEN lower(side) IN ('no', 'sell') THEN size_usd ELSE 0 END) AS no_volume
                FROM whale_flows
                WHERE timestamp >= ? AND actor_address IS NOT NULL AND actor_address != ''
                GROUP BY actor_address
                ORDER BY volume DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
        results = []
        for row in rows:
            yes_volume = row["yes_volume"] or 0.0
            no_volume = row["no_volume"] or 0.0
            if yes_volume == 0 and no_volume == 0:
                position = "N/A"
            else:
                position = "YES" if yes_volume >= no_volume else "NO"
            results.append(
                {
                    "address": row["actor_address"],
                    "volume": row["volume"],
                    "position": position,
                }
            )
        return results

    def wallet_summary(self, wallet: str, since_ts: Optional[float] = None) -> Optional[Dict[str, float]]:
        if not wallet:
            return None
        now = time.time()
        cutoff = since_ts if since_ts is not None else now - 86400
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS trades,
                       SUM(size_usd) AS volume,
                       SUM(CASE WHEN lower(side) IN ('yes', 'buy') THEN size_usd ELSE 0 END) AS yes_volume,
                       SUM(CASE WHEN lower(side) IN ('no', 'sell') THEN size_usd ELSE 0 END) AS no_volume,
                       MAX(timestamp) AS last_ts
                FROM whale_flows
                WHERE actor_address = ? AND timestamp >= ?
                """,
                (wallet, cutoff),
            ).fetchone()
        if not row or not row["trades"]:
            return None
        return {
            "trades": float(row["trades"]),
            "volume": row["volume"] or 0.0,
            "yes_volume": row["yes_volume"] or 0.0,
            "no_volume": row["no_volume"] or 0.0,
            "last_ts": row["last_ts"],
        }
