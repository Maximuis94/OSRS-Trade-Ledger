"""
Module with an interface for TransactionDatabaseEntries. Raw transactional data to be specific.
Raw transactional data is also stored as a json file. The database entries typically contain preprocessed values. For
instance, account_names are converted to account_ids, and some values are includes, while others are not.

Each transactional data source has its own table. Beyond that, there is a merged raw transaction table, which combines
these transactions. The merged table also contains a reference of the raw transaction it was composed from to help
assess whether transactions from various sources refer to the same transaction.

"""
import os.path
from collections import namedtuple

import sqlite3
from abc import ABC, abstractmethod
from typing import Tuple, Dict, Callable, Optional
from warnings import warn

from file.file import File
from transaction._util import convert_account_name


class ITransactionDatabaseEntry(ABC):
    """Interface for Transactions that have a table in the TransactionDatabase"""
    # table: str
    # """Name of the SQLite table"""
    #
    # path: str | File
    # """Path to the SQLite database"""
    #
    # sql_insert: str
    # """Executable INSERT statement for this entry"""
    #
    # sql_columns: Tuple[str, ...]
    # """Parameters that are to be passed along with sql_insert"""
        
    @abstractmethod
    def to_sqlite(self, con: Optional[sqlite3.Connection] = None) -> Tuple[str, Tuple[str, ...] | Dict[str, any]]:
        """
        Converts the attribute values so they can be uploaded to the TransactionDatabase.
        
        Parameters
        ----------
        con

        Returns
        -------

        """
        raise NotImplementedError
    
    @property
    def row_factory(self) -> Callable[[sqlite3.Cursor, tuple], type(tuple)]:
        """
        Row factory that can be set to a cursor to get the rows as NamedTuple instances that resemble the class
        
        Returns
        -------
        Callable[[sqlite3.Cursor, tuple], NamedTuple]
            An anonymous function that accepts a sqlite.Cursor and a tuple, which will return a namedtuple instance.
        
        """
        named_tuple = namedtuple(f"{self.__class__.__name__}Row", self.sql_columns)
        return lambda c, row: named_tuple(*row)
    
    @property
    def account_id(self) -> Optional[int]:
        """The account_id that associated with `account_name`, if there is an account_name"""
        return convert_account_name(self.account_name) if hasattr(self, "account_name") else None
    
    def __init_subclass__(cls, **kwargs):
        """Subclass hook. Used to verify class attributes."""
        e, msg, name = None, [], cls.__class__.__name__
        
        # Check if class attributes exist and if their types match annotations
        for a in ('path', 'table', 'sql_insert'):
            if not hasattr(cls, a):
                msg.append(f"'{a}' attribute is not defined in class {name}")
            
            # if not isinstance(getattr(cls, a), cls.__annotations__[a]):
            #     msg.append(f"Attribute '{a}' is not of type '{cls.__annotations__[a].__name__}', but of type "
            #                f"'{type(getattr(cls, a)).__name__}'")
        
        # if len(msg) > 0:
        #     err = "\n".join([f"One or more issues were found with class attribute configurations for {name};"] + msg)
        #     raise AttributeError(err)
        
        path = cls.path.fget(cls.path) if isinstance(cls.path, property) else cls.path
        if not os.path.exists(path):
            w = f"Database path was set to {path} for class {name}, but it does not refer to an existing file"
            warn(w)
        
        