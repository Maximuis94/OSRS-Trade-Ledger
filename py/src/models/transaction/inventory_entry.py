from dataclasses import dataclass, field
from typing import Optional, Tuple, Callable
import sqlite3
import global_variables.path as gp
from interfaces.db_entity import DbEntity

@dataclass(slots=True, match_args=False)
class InventoryEntry(DbEntity):
    """
    Representeert een inventaris entry in de database.
    """
    id: int = field(compare=True)
    inventory_id: int = field(compare=False)
    item_id: int = field(compare=False)
    quantity: int = field(compare=False)
    avg_buy_price: float = field(compare=False)
    last_update: int = field(compare=False)
    account_name: str = field(compare=False)
    update_ts: int = field(compare=False)

    @property
    def sqlite_path(self) -> str:
        return str(gp.f_db_local)
    
    @property
    def sqlite_table(self) -> str:
        return "inventory_entry"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], any]]:
        return lambda c, row: InventoryEntry(*row)
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        return ("id", "inventory_id", "item_id", "quantity", "avg_buy_price",
                "last_update", "account_name", "update_ts")
    
    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            inventory_id INTEGER NOT NULL UNIQUE,
            item_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            avg_buy_price REAL NOT NULL,
            last_update INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            update_ts INTEGER NOT NULL,
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
            WHERE inventory_id = NEW.inventory_id;
        END
        """

    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        return (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_inventory_id 
                ON "{self.sqlite_table}"(inventory_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_item_account 
                ON "{self.sqlite_table}"(item_id, account_name)"""
        ) 