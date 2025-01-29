"""Module with an interface for a DB entry"""
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Tuple, Optional, Dict


class IDbEntity(ABC):
    """
    Interface for a Db Entity. A Db Entity is an entity that has its own table in the database.
    
    Some methods are not abstract as not all methods may be required in most cases.
    The methods that generate an SQLite statement should also return the args, if these are mentioned in the statement
    """
    sqlite_path: str
    sqlite_table: str
    sqlite_attributes: Tuple[str, ...]
    sqlite_row_factory: Callable[[sqlite3.Cursor, tuple], any]
    
    @property
    def sqlite_row(self) -> Tuple[any, ...] | Dict[str, any]:
        """Tuple/dict representation of the class that can be inserted into the database; Complements sqlite_insert"""
        return tuple([self.__getattribute__(a) for a in self.sqlite_attributes])
    
    @property
    def sqlite_select(self) -> str:
        """Returns an executable SQLite SELECT statement. By default, select all rows in the table."""
        return f"""SELECT {self.sqlite_attributes} FROM "{self.sqlite_table}" """

    @property
    def sqlite_insert(self) -> str:
        """Returns an executable SQLite INSERT statement"""
        return f""""INSERT OR REPLACE INTO "{self.sqlite_table}" {self.sqlite_attributes} VALUES
                            {str(tuple(["?" for _ in self.sqlite_attributes]))}"""

    @property
    def sqlite_update(self) -> str:
        """Returns an executable SQLite UPDATE statement"""
        raise NotImplementedError("Implement an override for methods of the IDBEntry interface you wish to use")

    @property
    def sqlite_delete(self) -> str:
        """Returns an executable SQLite DELETE statement"""
        raise NotImplementedError("Implement an override for methods of the IDBEntry interface you wish to use")
    