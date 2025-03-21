from dataclasses import dataclass, field
from typing import Optional, Tuple, Callable
import sqlite3
from datetime import datetime
import global_variables.path as gp
from interfaces.db_entity import DbEntity

@dataclass(slots=True, match_args=False)
class TransactionEntry(DbEntity):
    """
    Representeert een enkele transactie in de database.
    """
    id: int = field(compare=True)
    transaction_id: int = field(compare=False)
    item_id: int = field(compare=False)
    timestamp: int = field(compare=False)
    quantity: int = field(compare=False)
    price: int = field(compare=False)
    is_buy: bool = field(compare=False)
    account_name: str = field(compare=False)
    total_price: int = field(compare=False)
    update_ts: int = field(compare=False)

    @property
    def sqlite_path(self) -> str:
        return str(gp.f_db_local)
    
    @property
    def sqlite_table(self) -> str:
        return "transaction_entry"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], any]]:
        return lambda c, row: TransactionEntry(*row)
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        return ("id", "transaction_id", "item_id", "timestamp", "quantity", 
                "price", "is_buy", "account_name", "total_price", "update_ts")
    
    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL UNIQUE,
            item_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price INTEGER NOT NULL,
            is_buy BOOLEAN NOT NULL,
            account_name TEXT NOT NULL,
            total_price INTEGER NOT NULL,
            update_ts INTEGER NOT NULL,
            FOREIGN KEY (item_id) REFERENCES item(item_id)
        )"""
    
    @property
    def sqlite_trigger(self) -> Optional[str]:
        return f"""
        CREATE TRIGGER IF NOT EXISTS update_ts_{self.sqlite_table}
        AFTER UPDATE ON "{self.sqlite_table}"
        BEGIN
            UPDATE "{self.sqlite_table}" 
            SET update_ts = unixepoch()
            WHERE transaction_id = NEW.transaction_id;
        END
        """
    
    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        return (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_transaction_id 
                ON "{self.sqlite_table}"(transaction_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_item_id 
                ON "{self.sqlite_table}"(item_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_account 
                ON "{self.sqlite_table}"(account_name)"""
        )

    @staticmethod
    def create(transaction_id: int, c: Optional[sqlite3.Cursor] = None) -> Optional['TransactionEntry']:
        """Creëert een instantie van TransactionEntry met transaction_id=`transaction_id`"""
        if c is None:
            conn = sqlite3.connect(f"file:{TransactionEntry.sqlite_path.fget(TransactionEntry)}?mode=ro", uri=True)
            c = conn.cursor()
            c.row_factory = lambda cursor, row: TransactionEntry(*row)
        
        query = f"""SELECT {", ".join(TransactionEntry.sqlite_attributes.fget(TransactionEntry))} 
                   FROM transaction_entry WHERE transaction_id=?"""
        return c.execute(query, (transaction_id,)).fetchone() 