from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, Any
from .base_raw_transaction import BaseRawTransaction
from .sql_manager import sql

@dataclass(slots=True, match_args=False)
class ExchangeLoggerTransaction(BaseRawTransaction):
    """
    Represents a transaction from Exchange Logger.
    Includes GE-specific metadata.
    """
    state: str = field(compare=False)
    ge_slot: int = field(compare=False)
    max_quantity: int = field(compare=False)
    offered_price: int = field(compare=False)
    value: int = field(compare=False)

    @property
    def sqlite_table(self) -> str:
        return "raw_exchange_logger_transaction"

    @property
    def sqlite_create(self) -> str:
        return sql.get_statement('table', 'create_exchange_logger_table')

    def to_sqlite(self) -> Tuple[str, Dict[str, Any]]:
        """Convert to SQLite insert statement and parameters"""
        stmt = sql.get_statement('insert', 'insert_exchange_logger_transaction')
        params = {
            'item_id': self.item_id,
            'timestamp': self.timestamp,
            'is_buy': self.is_buy,
            'quantity': self.quantity,
            'price': self.price,
            'state': self.state,
            'ge_slot': self.ge_slot,
            'max_quantity': self.max_quantity,
            'offered_price': self.offered_price,
            'value': self.value,
            'update_timestamp': self.update_timestamp
        }
        return stmt, params

    @classmethod
    def find_duplicates(cls, conn, transaction_id: int) -> Tuple[str, Dict[str, Any]]:
        """Find potential duplicate transactions"""
        stmt = sql.get_statement('select', 'find_exchange_logger_duplicates')
        return stmt, {'transaction_id': transaction_id} 