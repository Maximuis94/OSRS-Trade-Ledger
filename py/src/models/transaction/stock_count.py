from dataclasses import dataclass, field
from typing import Optional, Tuple, Callable
import sqlite3
import global_variables.path as gp
from interfaces.db_entity import DbEntity

@dataclass(slots=True, match_args=False)
class StockCount(DbEntity):
    """
    Representeert een voorraadtelling in de database.
    Houdt bij hoeveel van een item aanwezig is op een bepaald moment.
    """
    id: int = field(compare=True)
    stock_id: int = field(compare=False)
    item_id: int = field(compare=False)
    timestamp: int = field(compare=False)
    quantity: int = field(compare=False)
    account_name: str = field(compare=False)
    update_ts: int = field(compare=False)
    source: str = field(compare=False)  # 'manual', 'auto', 'snapshot'
    verified: bool = field(default=False, compare=False)

    @property
    def sqlite_path(self) -> str:
        return str(gp.f_db_local)
    
    @property
    def sqlite_table(self) -> str:
        return "stock_count"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], any]]:
        return lambda c, row: StockCount(*row)
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        return ("id", "stock_id", "item_id", "timestamp", "quantity", 
                "account_name", "update_ts", "source", "verified")
    
    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            stock_id INTEGER NOT NULL UNIQUE,
            item_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            update_ts INTEGER NOT NULL,
            source TEXT NOT NULL,
            verified BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (item_id) REFERENCES item(item_id)
        )"""

    @property
    def sqlite_trigger(self) -> str:
        return f"""
        CREATE TRIGGER IF NOT EXISTS update_ts_{self.sqlite_table}
        AFTER UPDATE ON "{self.sqlite_table}"
        BEGIN
            UPDATE "{self.sqlite_table}" 
            SET update_ts = unixepoch()
            WHERE stock_id = NEW.stock_id;
        END
        """

    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        return (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_stock_id 
                ON "{self.sqlite_table}"(stock_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_item_timestamp 
                ON "{self.sqlite_table}"(item_id, timestamp)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_account_timestamp 
                ON "{self.sqlite_table}"(account_name, timestamp)"""
        ) 