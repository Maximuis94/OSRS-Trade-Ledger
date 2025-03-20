"""
Module with basic functionalities of the TransactionDatabase
"""
import sqlite3
from dataclasses import dataclass, field
from typing import Literal, Dict, List

import transaction.sql.select as sql_select
from transaction.transaction_model.transaction_entry import factory_transaction
from transaction.transaction_model.transaction_entry import Transaction


@dataclass(slots=True)
class BasicTransactionDatabase:
    """Class with the basic TransactionDatabase utilities"""
    
    path: str
    """Path to the database"""
    
    triggers: List[str] = field(default_factory=list, kw_only=True)
    """List of triggers associated with this database"""
    
    pragma_journal_mode: Literal['Delete', 'Truncate', 'Persist', 'Memory', 'WAL', 'Off'] = field(default='WAL',
                                                                                                  kw_only=True)
    """Journal mode PRAGMA. See https://www.sqlite.org/pragma.html#pragma_journal_mode"""
    
    pragma_synchronous: Literal['Off', 'Normal', 'Full'] = field(default='Full', kw_only=True)
    """Journal mode PRAGMA. See https://www.sqlite.org/pragma.html#pragma_synchronous"""
    
    pragma_temp_store: Literal['Default', 'File', 'Memory'] = field(default='Memory', kw_only=True)
    """Temp store PRAGMA. See https://www.sqlite.org/pragma.html#pragma_temp_store"""
    
    pragma_cache_size: int = field(default=-4000000, kw_only=True)
    """Cache size PRAGMA. See https://www.sqlite.org/pragma.html#pragma_cache_size"""
    
    pragma_foreign_keys: bool = field(default=True, kw_only=True)
    """Foreign keys PRAGMA. See https://www.sqlite.org/pragma.html#pragma_foreign_keys"""
    
    @classmethod
    def _connect(cls) -> sqlite3.Connection:
        """Read-only connection that is not further configurable"""
        return sqlite3.connect(f"file:{cls.path}?mode=ro", uri=True)
    
    def connect(self, read_only: bool = False, **kwargs) -> sqlite3.Connection:
        """
        Connect to the database
        
        Parameters
        ----------
        read_only : bool, optional, False by default
            If True, return a read-only connection

        Returns
        -------
        sqlite3.Connection
            Connection to the database, following specified parameters

        """
        verbose = kwargs.get('verbose', False)
        
        if read_only:
            # Use the URI method to open in read-only mode
            uri = f'file:{self.path}?mode=ro'
            conn = sqlite3.connect(uri, uri=True, **kwargs)
            
            if verbose:
                print(f"Establishing read-only connection to {self.path}...")
        else:
            conn = sqlite3.connect(self.path, **kwargs)
            if verbose:
                print(f"Establishing writable connection to {self.path}...")
        
        cursor = conn.cursor()
        for pragma in self.__dir__():
            if not pragma.startswith('pragma_'):
                continue
            
            if read_only and pragma in ("pragma_journal_mode",):
                continue
            
            if verbose:
                print('\t', pragma, kwargs.get(pragma.replace('pragma_', ''), getattr(self, pragma)))
            cursor.execute(self._pragma(pragma.replace('pragma_', '')))
        
        conn.commit()  # Commit the PRAGMA settings if necessary
        # cursor.close()
        return conn
    
    def _pragma(self, pragma: Literal['journal_mode', 'synchronous', 'temp_store', 'cache_size', 'foreign_keys'],
                **kwargs):
        """Returns an executable SQL statement for setting a particular PRAGMA"""
        attribute = "_".join(["pragma", pragma])
        return " ".join(["PRAGMA", pragma, "=", str(kwargs.get(attribute, self.__getattribute__(attribute)))])
    
    def load_transactions(self, **kwargs) -> Dict[int, List[Transaction]]:
        """Load all transactions and group them per item_id"""
        conn = self.connect(read_only=True)
        c = conn.cursor()
        c.row_factory = lambda cur, row: row[0]
        
        item_ids = c.execute(sql_select.unique_items).fetchall()
        
        c.row_factory = factory_transaction
        return {item_id: c.execute(sql_select.sql_select_transaction_by_item, (item_id,)).fetchall() for item_id in item_ids}
    
    def add_triggers(self, triggers: List[str]):
        ...

