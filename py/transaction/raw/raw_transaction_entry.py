import sqlite3

from dataclasses import dataclass
from typing import Optional

import global_variables.path as gp
from transaction.raw.raw_runelite_export_entry import RuneliteExportEntry

_tags = {
    "efr": "A",
    "ef": "B",
    "er": "C",
    "fr": "D"
}


@dataclass(slots=True)
class RawTransactionEntry:
    """Represents a row from the 'raw_transaction' SQLite table."""
    
    item_id: int
    """OSRS item id"""
    
    timestamp: int
    """UNIX timestamp of the transaction"""
    
    quantity: int
    """Quantity traded"""
    
    price: int
    """Price per item. Computed as floor(value / quantity)"""
    
    update_timestamp: int
    """Timestamp when the transaction was updated"""
    
    transaction_id: Optional[int] = None
    """Unique transaction ID (primary key, auto-incremented)"""
    
    timestamp_created: Optional[int] = None
    """Additional UNIX timestamp that indicates the time of creation"""
    
    timestamp_runelite_export: Optional[int] = None
    """UNIX timestamp that indicates the time of creation of the runelite export"""
    
    is_buy: int = 0
    """Flag indicating if this is a buy (1) or a sell (0)"""
    
    max_quantity: Optional[int] = None
    """Maximum amount of items in this GE offer"""
    
    offered_price: Optional[int] = None
    """Price per item"""
    
    value: Optional[int] = None
    """Total value of the transaction"""
    
    account_name: Optional[str] = None
    """Name of the associated account"""
    
    ge_slot: Optional[int] = None
    """GE slot number (if provided; must be between 0 and 7 if not NULL)"""
    
    status: Optional[int] = None
    """Transaction status. Used mostly for imported legacy transactions to preserve the value in the raw_transaction"""
    
    tag: Optional[str] = None
    """Transaction status. Used mostly for imported legacy transactions to preserve the value in the raw_transaction"""
    
    exchange_logger_id: Optional[int] = None
    """Foreign key: reference to a raw_exchange_logger_transaction row"""
    
    flipping_utilities_id: Optional[int] = None
    """Foreign key: reference to a raw_flipping_utilities_transaction row"""
    
    runelite_export_id: Optional[int] = None
    """Foreign key: reference to a raw_runelite_export_transaction row"""
    
    @staticmethod
    def raw_entry(item_id: int, timestamp: int, is_buy: int, quantity: int, price: int, update_timestamp: int, value: Optional[int] = None, timestamp_created: Optional[int] = None, account_name: Optional[str] = None, ge_slot: Optional[int] = None, status: Optional[int] = 1, tag: Optional[str] = None, exchange_logger_id: Optional[int] = None, flipping_utilities_id: Optional[int] = None, runelite_export_id: Optional[int] = None, transaction_id: Optional[int] = None):
        """
        Converts raw values into a RawTransaction instance.

        Parameters
        ----------
        item_id : int
            OSRS item id.
        timestamp : int
            UNIX timestamp of the transaction.
        is_buy : int
            Flag indicating if this is a buy (1) or sell (0).
        quantity : int
            Quantity traded.
        price : int
            Price per item.
        update_timestamp : int
            Timestamp when the transaction was updated.
        value : Optional[int], optional
            Total value of the transaction; defaults to price * quantity if not provided.
        timestamp_created : Optional[int], optional
            Additional transaction timestamp or contextual value.
        account_name : Optional[int], optional
            ID of the associated account.
        ge_slot : Optional[int], optional
            GE slot number (if provided, should be between 0 and 7).
        status : Optional[int], optional, 1 by default
            Transaction status. Can be omitted, unless a specific value is required
        tag : Optional[str], optional
            The tag to assign to the transaction. Unless a specific tag is required, it should be managed automatically.
        exchange_logger_id : Optional[int], optional
            Reference to a raw_exchange_logger_transaction row.
        flipping_utilities_id : Optional[int], optional
            Reference to a raw_flipping_utilities_transaction row.
        runelite_export_id : Optional[int], optional
            Reference to a raw_runelite_export_transaction row.
        transaction_id : Optional[int], optional
            Primary key; auto-assigned by the database if not provided.

        Returns
        -------
        RawTransaction
            An instance representing a raw transaction.
        """
        computed_value = value if value is not None else price * quantity
        
        return RawTransactionEntry(
            transaction_id=transaction_id,
            item_id=item_id,
            timestamp=timestamp,
            timestamp_runelite_export=None,
            is_buy=is_buy,
            quantity=quantity,
            max_quantity=None,
            price=price,
            offered_price=None,
            value=computed_value,
            timestamp_created=timestamp_created,
            account_name=account_name,
            ge_slot=ge_slot,
            status=status,
            tag=tag,
            update_timestamp=update_timestamp,
            exchange_logger_id=exchange_logger_id,
            flipping_utilities_id=flipping_utilities_id,
            runelite_export_id=runelite_export_id
        )
    
    @staticmethod
    def from_runelite_export_entry(entry: RuneliteExportEntry, update_timestamp: int,
                            account_id: Optional[int] = None) -> "RawTransactionEntry":
        """
        Constructs a CombinedTransaction from a RuneliteTradeEntry.

        Parameters
        ----------
        entry : RuneliteTradeEntry
            A Runelite trade entry containing fields like item_id, timestamp, is_buy, price,
            and quantities (e.g. current_quantity or total_quantity).
        update_timestamp : int
            The timestamp when the transaction was updated.
        account_id : Optional[int], optional
            An account identifier if available.

        Returns
        -------
        RawTransactionEntry
        """
        # Choose the best available quantity; prefer total_quantity if it exists.
        
        return RawTransactionEntry(
            transaction_id=None,
            item_id=entry.item_id,
            timestamp=entry.timestamp,
            timestamp_runelite_export=entry.timestamp,
            is_buy=1 if entry.is_buy else 0,  # ensuring an integer flag
            quantity=entry.quantity,
            max_quantity=None,
            price=entry.price,
            offered_price=None,
            value=None,
            account_name=entry.account_name,
            ge_slot=None,
            status=1,
            tag=None,
            update_timestamp=update_timestamp,
            exchange_logger_id=entry.transaction_id,
            flipping_utilities_id=None,
            runelite_export_id=None
        )
    
    @property
    def table(self) -> str:
        return "raw_transaction"
    
    @property
    def path(self) -> str:
        return str(gp.f_db_transaction_new)
    
    @property
    def _tag(self) -> str:
        if self.tag is not None:
            return self.tag
        chars = ""
        if self.exchange_logger_id is not None:
            chars += "E"
        if self.flipping_utilities_id is not None:
            chars += "F"
        if self.runelite_export_id is not None:
            chars += "R"
        
        # Legacy transaction
        if len(chars) == 0:
            return "L"
        return chars if len(chars) == 1 else _tags[chars.lower()]


def factory_raw_transaction(c: sqlite3.Cursor, row: tuple) -> RawTransactionEntry:
    return RawTransactionEntry(**{c[0]: row[i] for i, c in enumerate(c.description)})
