from dataclasses import dataclass, field
from typing import Optional, Tuple
from .base_raw_transaction import BaseRawTransaction

@dataclass(slots=True, match_args=False)
class RuneliteExportTransaction(BaseRawTransaction):
    """
    Represents a transaction from Runelite's GE export.
    Simplified version with core transaction data.
    """
    
    @property
    def sqlite_table(self) -> str:
        return "raw_runelite_export_transaction"

    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            item_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            is_buy BOOLEAN NOT NULL,
            quantity INTEGER NOT NULL,
            price INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            update_timestamp INTEGER NOT NULL,
            FOREIGN KEY (item_id) REFERENCES item(item_id),
            UNIQUE (item_id, timestamp, is_buy, account_name)
        )"""

    @property
    def sqlite_view(self) -> str:
        """View for simplified transaction querying"""
        return f"""
        CREATE VIEW IF NOT EXISTS v_{self.sqlite_table}_summary AS
        SELECT 
            item_id,
            date(timestamp, 'unixepoch') as trade_date,
            account_name,
            SUM(CASE WHEN is_buy THEN quantity ELSE 0 END) as total_bought,
            SUM(CASE WHEN NOT is_buy THEN quantity ELSE 0 END) as total_sold,
            AVG(CASE WHEN is_buy THEN price END) as avg_buy_price,
            AVG(CASE WHEN NOT is_buy THEN price END) as avg_sell_price
        FROM "{self.sqlite_table}"
        GROUP BY item_id, trade_date, account_name
        """ 