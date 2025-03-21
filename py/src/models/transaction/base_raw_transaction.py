from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, Any
import sqlite3
from datetime import datetime
from interfaces.db_entity import DbEntity
import global_variables.path as gp

@dataclass(slots=True, match_args=False)
class BaseRawTransaction(DbEntity):
    """
    Base class for raw transaction entries from various sources.
    Implements common transaction attributes and database functionality.
    """
    id: int = field(compare=True)
    item_id: int = field(compare=False)
    timestamp: int = field(compare=False)
    is_buy: bool = field(compare=False)
    quantity: int = field(compare=False)
    price: int = field(compare=False)
    update_timestamp: int = field(compare=False)
    account_name: Optional[str] = field(default=None, compare=False)

    @property
    def sqlite_path(self) -> str:
        return str(gp.f_db_local)

    @property
    def sqlite_table(self) -> str:
        raise NotImplementedError("Subclasses must implement table name")

    @property
    def sqlite_row_factory(self) -> Optional[sqlite3.Row]:
        return sqlite3.Row

    @property
    def value(self) -> int:
        """Total value of the transaction"""
        return self.quantity * self.price

    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction to dictionary format"""
        return {
            "item_id": self.item_id,
            "timestamp": self.timestamp,
            "is_buy": self.is_buy,
            "quantity": self.quantity,
            "price": self.price,
            "update_timestamp": self.update_timestamp,
            "account_name": self.account_name
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseRawTransaction':
        """Create transaction from dictionary data"""
        return cls(**data)

    @property
    def sqlite_trigger(self) -> str:
        """Trigger to automatically update timestamp on changes"""
        return f"""
        CREATE TRIGGER IF NOT EXISTS update_ts_{self.sqlite_table}
        AFTER UPDATE ON "{self.sqlite_table}"
        BEGIN
            UPDATE "{self.sqlite_table}" 
            SET update_timestamp = unixepoch()
            WHERE id = NEW.id;
        END
        """

    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        """Common indices for all transaction tables"""
        return (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_item_timestamp 
                ON "{self.sqlite_table}"(item_id, timestamp)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_account 
                ON "{self.sqlite_table}"(account_name)"""
        ) 