from dataclasses import dataclass, field
from typing import Optional, Tuple, Callable
import sqlite3
import global_variables.path as gp
from interfaces.db_entity import DbEntity

@dataclass(slots=True, match_args=False)
class ItemProduction(DbEntity):
    """
    Representeert een productie-event van items.
    Registreert wanneer items zijn geproduceerd volgens een productieregel.
    """
    id: int = field(compare=True)
    production_id: int = field(compare=False)
    rule_id: int = field(compare=False)
    timestamp: int = field(compare=False)
    quantity: int = field(compare=False)
    account_name: str = field(compare=False)
    update_ts: int = field(compare=False)

    @property
    def sqlite_path(self) -> str:
        return str(gp.f_db_local)
    
    @property
    def sqlite_table(self) -> str:
        return "item_production"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], any]]:
        return lambda c, row: ItemProduction(*row)
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        return ("id", "production_id", "rule_id", "timestamp", 
                "quantity", "account_name", "update_ts")
    
    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            production_id INTEGER NOT NULL UNIQUE,
            rule_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            update_ts INTEGER NOT NULL,
            FOREIGN KEY (rule_id) REFERENCES item_production_rule(rule_id)
        )"""

    @property
    def sqlite_trigger(self) -> str:
        return f"""
        CREATE TRIGGER IF NOT EXISTS update_ts_{self.sqlite_table}
        AFTER UPDATE ON "{self.sqlite_table}"
        BEGIN
            UPDATE "{self.sqlite_table}" 
            SET update_ts = unixepoch()
            WHERE production_id = NEW.production_id;
        END
        """

    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        return (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_production_id 
                ON "{self.sqlite_table}"(production_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_rule_timestamp 
                ON "{self.sqlite_table}"(rule_id, timestamp)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_account_timestamp 
                ON "{self.sqlite_table}"(account_name, timestamp)"""
        ) 