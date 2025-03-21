from dataclasses import dataclass, field
from typing import Optional, Tuple
from .base_raw_transaction import BaseRawTransaction

@dataclass(slots=True, match_args=False)
class FlippingUtilitiesTransaction(BaseRawTransaction):
    """
    Represents a transaction from Flipping Utilities plugin.
    Includes additional trading metadata.
    """
    timestamp_created: Optional[int] = field(default=None, compare=False)
    max_quantity: Optional[int] = field(default=None, compare=False)
    ge_slot: Optional[int] = field(default=None, compare=False)
    uuid: str = field(default="", compare=False)
    status: str = field(default="", compare=False)

    @property
    def sqlite_table(self) -> str:
        return "raw_flipping_utilities_transaction"

    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            uuid TEXT NOT NULL UNIQUE,
            item_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            timestamp_created INTEGER,
            is_buy BOOLEAN NOT NULL,
            quantity INTEGER NOT NULL,
            max_quantity INTEGER,
            price INTEGER NOT NULL,
            ge_slot INTEGER CHECK (ge_slot BETWEEN 0 AND 7),
            account_name TEXT NOT NULL,
            status TEXT,
            update_timestamp INTEGER NOT NULL,
            FOREIGN KEY (item_id) REFERENCES item(item_id)
        )"""

    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        indices = super().sqlite_index
        return indices + (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_uuid 
                ON "{self.sqlite_table}"(uuid)""",
        ) 