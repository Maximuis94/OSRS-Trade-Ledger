"""
Model class for a raw flipping utilities export entry
"""
import sqlite3
from dataclasses import dataclass
from typing import Tuple, Dict, Optional

import global_variables.path as gp
from transaction._util import convert_account_name
from transaction.constants import update_timestamp
from transaction.interface.transaction_database_entry import ITransactionDatabaseEntry
from ts_util import convert_unix_time


@dataclass(slots=True)
class RuneliteExportEntry(ITransactionDatabaseEntry):
    """An entry extracted from a raw Runelite GE export"""
    
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
    
    account_name: str
    """Account that executed this trade"""
    
    update_timestamp: int = update_timestamp
    """Timestamp of the start of the session in which the transaction was generated from raw data"""
    
    def to_sqlite(self, con: Optional[sqlite3.Connection] = None) -> Tuple[str, Tuple[str, ...] | Dict[str, any]]:
        return self.sql_insert, self.sql_params
    
    @property
    def table(self) -> str:
        """Name of the database table"""
        return "raw_runelite_export_transaction"
    
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
        """SQL insert statement"""
        return (f'INSERT INTO "{self.table}" ({", ".join(self.sql_columns)}) VALUES '
                f'({", ".join(["?" for _ in range(len(["?" for _ in range(len(self.sql_columns))]))])})')
    
    @property
    def sql_params(self) -> Tuple[int | str, ...]:
        """Parameters that can be passed along with the sql_insert statement"""
        return self.item_id,  self.timestamp, self.is_buy, self.price, self.quantity, self.account_name, update_timestamp

    @property
    def transaction_id(self) -> Optional[int]:
        """Check if the transaction id is uploaded and if so, return its ID"""
        con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        _id = con.execute(
            f"SELECT transaction_id FROM {self.table} WHERE item_id=? AND timestamp=? AND is_buy=? AND account_name=?",
            (self.item_id, self.timestamp, self.is_buy, self.account_name)).fetchone()
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
        return RuneliteExportEntry(
            item_id=itemId,
            timestamp=convert_unix_time(time),
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
                e.account_name == self.account_name)
        
    def __ne__(self, e) -> bool:
        """True if `entry` describes the same entry as this JsonEntry"""
        return (e.item_id != self.item_id or
                e.timestamp != self.timestamp or
                e.is_buy != self.is_buy or
                e.account_name != self.account_name)
    
    @property
    def key(self) -> Tuple[int, int, int]:
        """The key that is used in the dict of entries to identify """
        return int(self.item_id), int(self.is_buy), int(self.timestamp)
    
    @property
    def dict(self) -> Dict[str, int | str]:
        """The key that """
        return {k: self.__getattribute__(k) for k in self.__match_args__}
