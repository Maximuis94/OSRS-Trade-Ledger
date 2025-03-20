"""
Model class for a raw Exchange Logger entry
"""
import sqlite3

from typing import Tuple, Dict, Optional
from math import floor
from dataclasses import dataclass
import datetime

import global_variables.path as gp
from transaction.interface.transaction_database_entry import ITransactionDatabaseEntry
from transaction.constants import update_timestamp


@dataclass(slots=True)
class ExchangeLoggerEntry(ITransactionDatabaseEntry):
    """An entry extracted from a raw Runelite GE export"""
    
    timestamp: int
    """UNIX timestamp derived from the 'date' and 'time' fields."""
    
    state: str
    """The current state of the offer (e.g., 'BUYING')."""
    
    ge_slot: int
    """The slot number in the GE interface."""
    
    item_id: int
    """OSRS item id (from the 'item' field)."""
    
    quantity: int
    """Quantity of items (from the 'qty' field)."""
    
    price: int
    """Price per item traded so far (manually computed via floor(value / quantity))."""
    
    value: int
    """Total value of the transaction (from the 'worth' field)."""
    
    max_quantity: int
    """Maximum allowed quantity (from the 'max' field)."""
    
    offered_price: int
    """Offer price (from the 'offer' field)."""
    
    update_timestamp: int = update_timestamp
    """Timestamp of the start of the session in which the transaction was generated from raw data"""
    
    
    def to_sqlite(self, con: Optional[sqlite3.Connection] = None) -> Tuple[str, Tuple[str, ...] | Dict[str, any]]:
        ...
    
    @property
    def sql_columns(self) -> Tuple[str, ...]:
        """Columns found in the SQLite table"""
        return "transaction_id", "item_id", "timestamp", "is_buy", "quantity", "price", "value", "ge_slot", "max_quantity", "offered_price", "update_timestamp"
    
    @property
    def sql_insert(self) -> str:
        """SQL insert statement"""
        columns = self.sql_columns[1:]
        return (f'INSERT INTO "{self.table}" ({", ".join(columns)}) VALUES '
                f'({", ".join(["?" for _ in range(len(["?" for _ in range(len(columns))]))])})')
    
    @property
    def sql_params(self):
        return self.item_id, self.timestamp, self.is_buy, self.quantity, self.price, self.value, self.ge_slot, self.max_quantity, self.offered_price, self.update_timestamp
    
    @property
    def table(self) -> str:
        """Name of the database table"""
        return "raw_exchange_logger_transaction"
    
    @property
    def merged_select(self):
        return f"""SELECT transaction_id FROM "raw_transaction" WHERE exchange_logger_id=?"""
    
    @property
    def path(self) -> str:
        """Name of the database table"""
        return gp.f_db_transaction_new
    
    @property
    def transaction_id(self) -> Optional[int]:
        """Check if the transaction id is uploaded and if so, return its ID"""
        con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        _id = con.execute(f"SELECT transaction_id FROM '{self.table}' WHERE item_id=? AND timestamp=? AND is_buy=? AND ge_slot=?",
                           (self.item_id, self.timestamp, self.is_buy, self.ge_slot)).fetchone()
        return None if _id is None else _id[0]
    
    @property
    def is_buy(self) -> int:
        """0 if this is a sale, 1 if it is a purchase. Determined using state."""
        s = self.state.lower()
        return int(s.endswith(("buy", "bought")))
    
    @property
    def sql_count(self) -> str:
        """SQL statement that can be used to check if there is a row in the database of this entry"""
        columns = self.sql_columns[1:-1]
        return f"""SELECT COUNT(*) FROM "{self.table}" WHERE {" = ? AND ".join(columns)} = ?"""
    
    # @property
    # def price(self) -> int:
    #     """The price per item paid/received within the transaction. Value is floored, based on various comparisons"""
    #     return int(floor(self.value / self.quantity))
    
    @staticmethod
    def raw_entry(state: str, slot: int, item: int, qty: int, worth: int, max_quantity: int, offer: int, **kwargs):
        """
        Converts a raw entry into a RuneliteOfferEntry object.

        Parameters
        ----------
        state : str
            The current state of the offer (e.g., "BUYING").
        slot : int
            The slot number in the GE interface.
        item : int
            OSRS item id.
        qty : int
            Quantity of items.
        worth : int
            Total worth of the transaction.
        max_quantity : int
            Maximum allowed quantity.
        offer : int
            Offer price.
            
        Other Parameters
        ----------------
        timestamp : int, Optional
            UNIX timestamp. If passed, date and time are no longer needed to generate the timestamp
        date : str
            Date in the format YYYY-MM-DD.
        time : str
            Time in the format HH:MM:SS.

        Returns
        -------
        RuneliteOfferEntry
            The offer entry converted to a structured object.
        """
        return ExchangeLoggerEntry(
            timestamp=kwargs.get('timestamp', int(datetime.datetime.strptime(f"{kwargs['date']} {kwargs['time']}", "%Y-%m-%d %H:%M:%S").timestamp())),
            state=state,
            ge_slot=slot,
            item_id=item,
            quantity=qty,
            price=int(floor(worth / qty)),
            value=worth,
            max_quantity=max_quantity,
            offered_price=offer
        )
    
    @property
    def key(self) -> Tuple[int, int, int]:
        """The key that is used in the dict of entries to identify """
        return int(self.item_id), int(self.is_buy), int(self.timestamp)
    
    @property
    def dict(self) -> Dict[str, int | str]:
        """The key that """
        return {k: self.__getattribute__(k) for k in self.__match_args__}
