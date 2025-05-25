"""
Model class for a raw Runelite Profile trade entry
"""
import datetime

from math import floor

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import global_variables.path as gp
from transaction._util import convert_account_name
from transaction.constants import update_timestamp
from transaction.interface.transaction_database_entry import ITransactionDatabaseEntry


_attributes = ""


@dataclass(slots=True)
class RunelitePluginTransaction(ITransactionDatabaseEntry):
    """Combined Transaction for runelite JSON export and runelite profile data transactions"""
    item_id: int
    """OSRS item id"""
    
    timestamp: int
    """UNIX timestamp of this trade"""
    
    is_buy: int
    """Flag that indicates if this a purchase"""
    
    price: int
    """Price per item in this transaction"""
    
    quantity: int
    """Quantity traded within this transaction"""
    
    # This field was omitted because this computation will produce a rounding error
    # value: int
    # """Total value of this transaction; price * quantity"""
    
    account_name: Optional[str] = None
    """Account that executed this trade"""
    
    update_timestamp: int = field(default=update_timestamp, init=False)
    """Timestamp of the start of the session in which the transaction was generated from raw data"""
    
    _hash: int = field(default=None, init=False)
    """Hash value of the tuple of all unique column values in a specific order"""
    
    _date: int = field(default=None, init=False)
    """Hash value of the tuple of all unique column values in a specific order"""
    
    _time: int = field(default=None, init=False)
    """Representation of the time"""
    
    # def __post_init__(self):
    #     self.timestamp = int(floor(self.timestamp / 1000))
    
    def to_sqlite(self, con: Optional[sqlite3.Connection] = None) -> Tuple[str, Tuple[str, ...] | Dict[str, any]]:
        return self.sql_insert, self.sql_params
    
    @property
    def table(self) -> str:
        """Name of the database table"""
        return "raw_runelite_profile_transaction"
    
    @property
    def merged_select(self):
        return f"""SELECT transaction_id FROM "raw_transaction" WHERE runelite_export_id=?"""
    
    @property
    def path(self) -> str:
        """Name of the database table"""
        return gp.f_db_transaction_new
    
    @property
    def sql_columns(self) -> Tuple[str, ...]:
        """Tuple of column names that are used in an SQL insert statement"""
        return "item_id", "timestamp", "is_buy", "price", "quantity", "account_name", "update_timestamp"
    
    @property
    def sql_count(self) -> str:
        """SQL statement that can be used to check if there is a row in the database of this entry"""
        columns = self.sql_columns[:-1]
        return f"""SELECT COUNT(*) FROM "{self.table}" WHERE {" = ? AND ".join(columns)} = ?"""
    
    @property
    def sql_insert(self) -> str:
        """SQL insert or ignore statement for inserting RuneliteProfileTransactions."""
        return f"""INSERT OR IGNORE INTO "{self.table}" (
            item_id, timestamp, is_buy, price, quantity, account_name, update_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"""
    
    @property
    def sql_params(self) -> Tuple[int | str, ...]:
        """Parameters that can be passed along with the sql_insert statement"""
        return self.item_id, self.timestamp, self.is_buy, self.price, self.quantity, self.account_name, update_timestamp
    
    @property
    def transaction_id(self) -> Optional[int]:
        """Check if the transaction id is uploaded and if so, return its ID"""
        con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        _id = con.execute(
            f"SELECT transaction_id FROM {self.table} WHERE item_id=? AND timestamp=? AND is_buy=? AND price=? AND quantity=?",
            (self.item_id, self.timestamp, self.is_buy, self.price, self.quantity)).fetchone()
        return None if _id is None else _id[0]
    
    @property
    def account_id(self) -> int:
        return convert_account_name(self.account_name)
    
    @staticmethod
    def raw_entry(buy: bool, itemId: int, quantity: int, price: int, time: int, account_name: str):
        """
        Converts a raw entry into a JsonEntry object

        Parameters
        ----------
        buy : bool
            If True, this is a purchase. If False, it is not.
        itemId : int
            The item_id of the item that is traded.
        quantity : int
            The quantity of the item that is traded.
        price : int
            The price of the item that is traded.
        time : int
            UNIX timestamp (ms)
        account_name : str
            The name of the account that made this trade

        Returns
        -------
        RuneliteExportEntry
            The Transaction converted to a JSONEntry object
        """
        return RuneliteProfileTransaction(
            item_id=itemId,
            timestamp=time,
            is_buy=int(buy),
            price=int(price),
            quantity=int(quantity),
            # value=int(price*quantity),
            account_name=account_name
        )
    
    def __eq__(self, e) -> bool:
        """True if `entry` describes the same entry as this JsonEntry"""
        return (e.item_id == self.item_id and
                e.timestamp == self.timestamp and
                e.is_buy == self.is_buy and
                e.price == self.price and
                e.quantity == self.quantity)
    
    def __ne__(self, e) -> bool:
        """True if `entry` describes the same entry as this JsonEntry"""
        return (e.item_id != self.item_id or
                e.timestamp != self.timestamp or
                e.is_buy != self.is_buy or
                e.quantity != self.quantity or
                e.price != self.price)
    
    @property
    def key(self) -> Tuple[int, int, int]:
        """The key that is used in the dict of entries to identify """
        return int(self.item_id), int(self.is_buy), int(self.timestamp)
    
    @property
    def dict(self) -> Dict[str, int | str]:
        """The key that """
        return {k: self.__getattribute__(k) for k in self.sql_columns}
    
    @property
    def unique_columns(self) -> Tuple[str, str, str, str, str]:
        """Columns of which each combination of values is unique per row"""
        return "item_id", "timestamp", "is_buy", "quantity", "price"
    
    @property
    def hash(self) -> int:
        """The combination of unique column values"""
        if self._hash is None:
            self._hash = (self.item_id, self.timestamp, self.is_buy, self.quantity, self.price).__hash__()
        return self._hash
    
    @staticmethod
    def from_exchange_log_line(line: Dict[str, str | int]):
        """Constructor that parses a json line as RunelitePluginTransaction"""
        for a, v in zip(__match_args__, ):
    
    @staticmethod
    def convert_exchange_log_line(log_file) -> Dict[str, int | str]:
        """
        Example line;
{"date":"2025-05-23","time":"02:12:09","state":"CANCELLED_SELL","slot":0,"item":22810,"qty":340,"worth":2714220,"max":7480,"offer":7983}
        datetime is *local*
        Parameters
        ----------
        date
        time
        state
        slot
        item
        qty
        worth
        max_quantity
        offer

        Returns
        -------

        """
        
        if RunelitePluginTransaction.state in ("BUYING", "SELLING"):
        
        ts = datetime.datetime.strptime("_".join([date, time]), "%Y-%m-%d_%H:%M:%S").timestamp()
        is_buy = state in ("BOUGHT", "CANCELLED_BUY", "BUYING")
        ge_slot = slot
        quantity = qty
        value = worth
        price
        offered_price = offer
