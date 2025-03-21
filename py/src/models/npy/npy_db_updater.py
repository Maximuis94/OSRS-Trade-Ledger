from dataclasses import dataclass
from typing import List, Optional
import sqlite3
from datetime import datetime
from pathlib import Path

@dataclass
class NpyDbConfig:
    """Configuration for NPY database updates"""
    source_db_path: Path
    target_db_path: Path
    schema_path: Path
    batch_size: int = 1000
    min_timestamp: Optional[int] = None
    max_timestamp: Optional[int] = None

class NpyDbUpdater:
    """
    Manages NPY database updates with efficient SQL-based computations.
    """
    def __init__(self, config: NpyDbConfig):
        self.config = config
        self.source_conn = sqlite3.connect(str(config.source_db_path))
        self.target_conn = sqlite3.connect(str(config.target_db_path))
        self._initialize_database()

    def _initialize_database(self):
        """Initialize database schema if needed"""
        with open(self.config.schema_path) as f:
            self.target_conn.executescript(f.read())
        self.target_conn.commit()

    def update_item_data(self, item_id: int, start_timestamp: int, end_timestamp: int):
        """Update data for a specific item within the given timeframe"""
        # First, get wiki data
        wiki_data = self._get_wiki_data(item_id, start_timestamp, end_timestamp)
        
        # Then, get trading data
        trading_data = self._get_trading_data(item_id, start_timestamp, end_timestamp)
        
        # Merge and insert data
        self._merge_and_insert_data(item_id, wiki_data, trading_data)

    def _get_wiki_data(self, item_id: int, start_ts: int, end_ts: int) -> List[tuple]:
        """Get wiki price data using efficient SQL"""
        return self.source_conn.execute("""
            WITH timestamps AS (
                SELECT generate_series(?, ?, 300) as ts
            )
            SELECT 
                t.ts,
                COALESCE(w.price, 0) as price,
                COALESCE(w.volume, 0) as volume,
                COALESCE(w.timestamp, 0) as wiki_ts
            FROM timestamps t
            LEFT JOIN (
                SELECT timestamp, price, volume,
                       MAX(timestamp) OVER (ORDER BY timestamp) as max_ts
                FROM wiki_prices
                WHERE item_id = ? AND timestamp BETWEEN ? AND ?
            ) w ON t.ts >= w.timestamp AND t.ts < w.max_ts
            ORDER BY t.ts
        """, (start_ts, end_ts, item_id, start_ts, end_ts)).fetchall()

    def _get_trading_data(self, item_id: int, start_ts: int, end_ts: int) -> List[tuple]:
        """Get trading data using efficient SQL"""
        return self.source_conn.execute("""
            WITH timestamps AS (
                SELECT generate_series(?, ?, 300) as ts
            )
            SELECT 
                t.ts,
                avg(CASE WHEN is_buy THEN price END) as buy_price,
                sum(CASE WHEN is_buy THEN volume END) as buy_volume,
                avg(CASE WHEN NOT is_buy THEN price END) as sell_price,
                sum(CASE WHEN NOT is_buy THEN volume END) as sell_volume,
                count(*) as trade_count
            FROM timestamps t
            LEFT JOIN trades tr ON tr.timestamp BETWEEN t.ts AND t.ts + 299
                AND tr.item_id = ?
            GROUP BY t.ts
            ORDER BY t.ts
        """, (start_ts, end_ts, item_id)).fetchall()

    def _merge_and_insert_data(self, item_id: int, wiki_data: List[tuple], trading_data: List[tuple]):
        """Merge and insert data using efficient batched operations"""
        insert_sql = """
        INSERT INTO npy_data (
            item_id, timestamp, minute, hour, day, month, year, weekday,
            hour_timestamp, day_timestamp, week_timestamp,
            wiki_timestamp, wiki_price, wiki_volume,
            buy_price, buy_volume, sell_price, sell_volume,
            rt_avg_price, rt_min_price, rt_max_price, rt_count,
            volume_coefficient
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (item_id, timestamp) DO UPDATE SET
            wiki_timestamp = excluded.wiki_timestamp,
            wiki_price = excluded.wiki_price,
            wiki_volume = excluded.wiki_volume,
            buy_price = excluded.buy_price,
            buy_volume = excluded.buy_volume,
            sell_price = excluded.sell_price,
            sell_volume = excluded.sell_volume
        """
        
        batch = []
        for wiki, trade in zip(wiki_data, trading_data):
            timestamp = wiki[0]
            dt = datetime.fromtimestamp(timestamp)
            
            row = (
                item_id, timestamp,
                dt.minute, dt.hour, dt.day, dt.month, dt.year, dt.weekday(),
                timestamp // 3600 * 3600,
                timestamp // 86400 * 86400,
                timestamp // 604800 * 604800,
                wiki[3], wiki[1], wiki[2],  # wiki data
                trade[1], trade[2], trade[3], trade[4],  # trading data
                0, 0, 0, trade[5],  # realtime data (placeholder)
                min(1.0, wiki[2] / max(1, item.buy_limit))  # volume coefficient
            )
            batch.append(row)
            
            if len(batch) >= self.config.batch_size:
                self.target_conn.executemany(insert_sql, batch)
                batch = []
        
        if batch:
            self.target_conn.executemany(insert_sql, batch)
        self.target_conn.commit() 