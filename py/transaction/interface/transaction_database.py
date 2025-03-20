"""
Transaction Database interface

"""
import sqlite3

from abc import ABC, abstractmethod


class ITransactionDatabase(ABC):
    """
    Interface of the TransactionDatabase
    
    """
    
    @abstractmethod
    def connect(self, read_only: bool = False, **kwargs) -> sqlite3.Connection:
        """
        Connect to the SQLite database applying the preferred PRAGMA settings.
        
        Parameters
        ----------
        read_only : bool, optional, False by default
            If True, return a read-only connection
        
        Other Parameters
        ----------------
        journal_mode: Literal['Delete', 'Truncate', 'Persist', 'Memory', 'WAL', 'Off'], optional, 'WAL' by default
            Value to pass to PRAGMA journal_mode
        synchronous: Literal['Off', 'Normal', 'Full'], optional, 'Full' by default
            Value to pass to PRAGMA synchronous
        temp_store: Literal['Default', 'File', 'Memory'], optional, 'Memory' by default
            Value to pass to PRAGMA synchronous
        cache_size : int, optional, -4000000 by default
            Allocated cache size for this database in Kb
        verbose : bool, optional, False by default
            If set, print invoking custom settings

        Returns
        -------
        sqlite3.Connection
            A connection to the transaction database with specific configurations set
        """
        raise NotImplementedError
    