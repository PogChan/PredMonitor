import sqlite3
import time
from datetime import datetime

DB_PATH = "data/trades.db"

def check_kalshi_data():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check total count
        cursor.execute("SELECT COUNT(*) as count FROM whale_flows WHERE lower(platform) = 'kalshi'")
        total = cursor.fetchone()['count']

        # Check recent
        cursor.execute("""
            SELECT timestamp, market, size_usd
            FROM whale_flows
            WHERE lower(platform) = 'kalshi'
            ORDER BY timestamp DESC
            LIMIT 5
        """)
        rows = cursor.fetchall()

        print(f"Total Kalshi rows in DB: {total}")
        if rows:
            print("Recent Kalshi trades:")
            for row in rows:
                ts = datetime.fromtimestamp(row['timestamp']).isoformat()
                print(f" - {ts} | {row['market']} | ${row['size_usd']:.2f}")
        else:
            print("No Kalshi trades found.")

    except Exception as e:
        print(f"Error checking DB: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_kalshi_data()
