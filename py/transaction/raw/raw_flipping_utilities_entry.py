"""
Model class for a raw flipping utilities export entry

"""
import sqlite3
from dataclasses import dataclass
from typing import Tuple, Dict, Optional

import global_variables.path as gp

import transaction.sql.select as sql_select
import transaction.sql.insert as sql_insert
import transaction.sql.update as sql_update
from transaction._util import convert_unix_ms
from transaction.constants import update_timestamp
from transaction.interface.transaction_database_entry import ITransactionDatabaseEntry
from transaction.sql.insert import insert_raw_flipping_utilities_transaction


@dataclass(slots=True)
class FlippingUtilitiesEntry(ITransactionDatabaseEntry):
    """An entry extracted from a raw Flipping Utilities JSON entry export"""
    
    item_id: int
    """OSRS item id (id in json)"""
    
    timestamp_created: Optional[int]
    """Timestamp when the offer was created (from 'tradeStartedAt', converted from ms to s)"""
    
    timestamp: int
    """UNIX timestamp of this trade completion time (t in json))"""
    
    is_buy: bool
    """Flag indicating if this is a purchase (from 'b')"""
    
    before_login: bool
    """Indicates if the trade occurred before login (from 'beforeLogin')"""
    
    quantity: int
    """Current quantity in trade (from 'cQIT')"""
    
    max_quantity: int
    """Total quantity in trade (from 'tQIT')"""
    
    price: int
    """Price per item in this transaction (from 'p')"""
    
    ge_slot: Optional[int]
    """Field 's' (its meaning is unspecified in the export)"""
    
    status: str
    """Status of the trade (from 'st', e.g. 'BOUGHT')"""
    
    tAA: int
    """Additional field tAA (unspecified meaning)"""
    
    tSFO: int
    """Additional field tSFO (unspecified meaning)"""
    
    uuid: str
    """Unique identifier for this trade (from 'uuid')"""
    
    account_name: str
    """Name of the account that made the trade"""
    
    update_timestamp: int
    """Timestamp of the start of the session in which the transaction was generated from raw data"""
    
    def to_sqlite(self, con: Optional[sqlite3.Connection] = None) -> Tuple[str, Tuple[str, ...] | Dict[str, any]]:
        pass
    
    @property
    def table(self) -> str:
        """Name of the database table"""
        return "raw_flipping_utilities_transaction"
    
    @property
    def merged_select(self):
        return f"""SELECT transaction_id FROM "raw_transaction" WHERE flipping_utilities_id=?"""
    
    @property
    def path(self) -> str:
        """Name of the database table"""
        return gp.f_db_transaction_new
    
    @property
    def sql_columns(self) -> Tuple[str, ...]:
        """Tuple of column names that are used in an SQL insert statement"""
        return "uuid", "item_id", "timestamp_created", "timestamp", "is_buy", "quantity", "max_quantity", "price", "account_name", "ge_slot", "update_timestamp"

    @property
    def sql_count(self) -> str:
        """SQL statement that can be used to check if there is a row in the database of this entry"""
        columns = self.sql_columns[:-1]
        return f"""SELECT COUNT(*) FROM "{self.table}" WHERE {" = ? AND ".join(columns)} = ?"""

    @property
    def sql_insert(self) -> str:
        """SQL insert statement"""
        return sql_insert.insert_raw_flipping_utilities_transaction
    
    @property
    def sql_params(self) -> Tuple[int | str, ...]:
        """Parameters that can be passed along with the sql_insert statement"""
        return self.uuid, self.item_id, self.timestamp_created, self.timestamp, int(self.is_buy), self.quantity, self.max_quantity, self.price, self.account_name, self.ge_slot if self.ge_slot != -1 else None, self.update_timestamp
    
    @property
    def transaction_id(self) -> Optional[int]:
        """Check if the transaction id is uploaded and if so, return its ID"""
        con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        t_id = con.execute(f"SELECT transaction_id FROM '{self.table}' WHERE uuid=?", (self.uuid,)).fetchone()
        return None if t_id is None else t_id[0]
    
    @staticmethod
    def raw_entry(b: bool, beforeLogin: bool, cQIT: int, item_id: int, p: int, s: int, st: str, t: int, tAA: int, tQIT: int,
                  tSFO: int, tradeStartedAt: int, uuid: str, account_name: str, **kwargs):
        """
        Converts a raw entry into a FlippingUtilitiesEntry object.

        Parameters
        ----------
        b : bool
            Indicates if this is a purchase.
        beforeLogin : bool
            Indicates if the trade occurred before login.
        cQIT : int
            Current quantity in trade.
        item_id : int
            The OSRS item id.
        p : int
            The price per item in this transaction.
        s : int
            Field 's' (unspecified meaning).
        st : str
            Status of the trade.
        t : int
            UNIX timestamp (ms) of the trade.
        tAA : int
            Additional field tAA (unspecified meaning).
        tQIT : int
            Total quantity in trade.
        tSFO : int
            Additional field tSFO (unspecified meaning).
        tradeStartedAt : int
            Timestamp (ms) when the trade started.
        uuid : str
            Unique identifier for this trade.
        account_name : str
            Name of the account that made the trade

        Returns
        -------
        RuneliteTradeEntry
            The trade entry converted to a structured object.
        """
        return FlippingUtilitiesEntry(
            item_id=item_id,
            timestamp=convert_unix_ms(t),
            is_buy=b,
            before_login=beforeLogin,
            quantity=cQIT,
            price=p,
            ge_slot=None if s == -1 else s,
            status=st,
            tAA=tAA,
            max_quantity=tQIT,
            tSFO=tSFO,
            timestamp_created=convert_unix_ms(tradeStartedAt),
            uuid=uuid,
            account_name=account_name,
            update_timestamp=update_timestamp
        )
    
    def __eq__(self, e) -> bool:
        """True if `entry` describes the same entry as this JsonEntry"""
        return e.uuid == self.uuid
    
    def __ne__(self, e) -> bool:
        """True if `entry` describes the same entry as this JsonEntry"""
        return e.item_id != self.item_id or e.timestamp != self.timestamp or e.is_buy != self.is_buy
    
    @property
    def key(self) -> str:
        """The key that """
        return self.uuid
    
    @property
    def dict(self) -> Dict[str, int | str]:
        """The key that """
        return {k: self.__getattribute__(k) for k in self.__match_args__}
    
    @property
    def sql_id_merged_transaction(self):
        return {
            """SELECT """
        }
