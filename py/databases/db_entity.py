"""Module with an interface for a DB entry"""
import os.path
from collections import namedtuple
from dataclasses import dataclass, field, is_dataclass

import warnings

import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import final, Tuple, Optional, Dict

from common.row_factories import factory_idx0

RowFactories = namedtuple("RowFactories", ["idx0"])


_column_dict = {}


@dataclass(slots=True)
class DbEntity(ABC):
    """
    Abstract class for an Entity that has a table in an SQLite database. Resembles an interface (ish), although
    It provides methods and properties for defining the database path, table name, column attributes,
    and generating SQL statements for common database operations (SELECT, INSERT, UPDATE, DELETE).

    Methods
    -------
    sqlite_path : str
        The path to the SQLite database file.
    sqlite_table : str
        The name of the table in the database.
    sqlite_row_factory : Optional[Callable[[sqlite3.Cursor, tuple], any]]
        A custom row factory for converting database rows into Python objects.
    sqlite_attributes : Tuple[str, ...]
        The names of the columns in the database table, in the order they appear.
    sqlite_row : Tuple[any, ...] | Dict[str, any]
        A tuple or dictionary representing the entity's data, suitable for insertion into the database.
    sqlite_select : str
        An SQL SELECT statement for retrieving data from the table.
    sqlite_insert : str
        An SQL INSERT or REPLACE statement for adding or updating data in the table.
    sqlite_update : str
        An SQL UPDATE statement for modifying existing data in the table.
    sqlite_delete : str
        An SQL DELETE statement for removing data from the table.
    sqlite_connect : sqlite3.Connection
        A connection object for interacting with the database in read-write mode.
    sqlite_connect_ro : sqlite3.Connection
        A connection object for interacting with the database in read-only mode.
    sqlite_cursor_ro_factory : sqlite3.Cursor
        A cursor object with a custom row factory, for read-only database interaction.
    """
    _columns: Tuple[str, ...] = field(init=False, repr=False)
    
    @property
    def sqlite_path(self) -> str:
        """The path to the database associated with this Entity"""
        raise NotImplementedError("Override this property if you wish to use it")
    
    @property
    def sqlite_table(self) -> str:
        """The name of the table in the sqlite database"""
        raise NotImplementedError("Override this property if you wish to use it")
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], any]]:
        """The row factory that is specifically designed for this DbEntity"""
        return None
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        """Attributes in the order they are defined in the associated dataclass and database"""
        return self._columns
    
    @property
    def sqlite_row(self) -> Tuple[any, ...] | Dict[str, any]:
        """Tuple/dict representation of the class that can be inserted into the database; Complements sqlite_insert"""
        return tuple([self.__getattribute__(a) for a in self.sqlite_attributes])
    
    @property
    def sqlite_select(self) -> str:
        """Executable SQLite SELECT statement. By default, select `sqlite_attributes` for all rows in the table."""
        return f"""SELECT {self.sqlite_attributes} FROM "{self.sqlite_table}" """
    
    @property
    def sqlite_insert(self) -> str:
        """Returns an executable SQLite INSERT statement"""
        return f""""INSERT OR REPLACE INTO "{self.sqlite_table}" {self.sqlite_attributes} VALUES
                            {str(tuple(["?" for _ in self.sqlite_attributes]))}"""
    
    @property
    def sqlite_update(self) -> str:
        """Returns an executable SQLite UPDATE statement"""
        raise NotImplementedError("Override this property if you wish to use it")
    
    @property
    def sqlite_delete(self) -> str:
        """Returns an executable SQLite DELETE statement"""
        raise NotImplementedError("Override this property if you wish to use it")
    
    @property
    def sqlite_connect(self) -> sqlite3.Connection:
        """Establish and return a writable connection to the database"""
        return sqlite3.connect(self.sqlite_path)
    
    @property
    def sqlite_connect_ro(self) -> sqlite3.Connection:
        """Establish and return a read-only connection to the database"""
        return sqlite3.connect(f"file:{self.sqlite_path}?mode=ro", uri=True)
    
    @property
    def sqlite_cursor_ro_factory(self) -> sqlite3.Cursor:
        """Establish a read-only connection with the db, extract a cursor, set the row factory and return the cursor"""
        conn = self.sqlite_connect_ro
        if self.sqlite_row_factory:
            cursor = conn.cursor()
            cursor.row_factory = self.sqlite_row_factory
            return cursor
        else:
            return conn.cursor()
    
    @property
    def sqlite_create(self) -> Optional[str]:
        """CREATE TABLE statement for this entity"""
        return None
    
    @property
    def sqlite_trigger(self) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N triggers related to this entity to the database"""
        return None
    
    @property
    def sqlite_index(self) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N indices related to this entity to the database"""
        return None
    
    @property
    def sqlite_view(self) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N views used to display information about this entity to the database"""
        return None
    
    def sqlite_add_to_database(self, con: Optional[sqlite3.Connection | sqlite3.Cursor] = None):
        """
        Method that extends a database with a table that represents this Entity. Furthermore, it may also add indices,
        triggers, views, and extend the database in other ways in order to accomodate this entity, depending on the
        implementation.
        
        Parameters
        ----------
        con

        Returns
        -------

        """
        if con is None:
            con = self.sqlite_connect
        try:
            for _next in ("sqlite_create", "sqlite_trigger", "sqlite_index", "sqlite_view"):
                sql = self.__getattribute__(_next)
                if sql is None:
                    continue
                elif ";" in sql:
                    con = con.executescript(sql)
                else:
                    con = con.execute(sql)
        finally:
            con.close()
            
    def post_init(self):
        """Override this method instead of __post_init__"""
        
        ...
    
    @final
    def __post_init__(self):
        """
        Connect to the database to determine the names of the columns of the associated table. This method should not
        be overridden. override `post_init()` instead.
        """
        cls_name = self.__class__.__name__
        
        global _column_dict
        _columns = _column_dict.get(cls_name)
        if _columns is None:
            conn = self.sqlite_connect_ro
            conn.row_factory = factory_idx0
            _column_dict[cls_name] = tuple(conn.execute(f"PRAGMA table_info({self.sqlite_table})").fetchall())
        self._columns = _columns
        
        self.post_init()
    
    def __init_subclass__(cls, **kwargs):
        if "__post_init__" in cls.__dict__:
            raise RuntimeError("A class that inherits from IDbEntity is not allowed to override __post_init__. "
                               "Override post_init() instead-- a call to this method will be made in the super "
                               "class __post_init__")
        if not __debug__:
            return
        
        path = cls.sqlite_path.fget(cls)
        table = cls.sqlite_table.fget(cls)
        if not os.path.exists(path):
            raise FileNotFoundError(f"""No database file exists at "{path}" """)
        
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        c = conn.cursor()
        c.row_factory = lambda _c, _r: _r[0]
        
        if not c.execute("SELECT COUNT(*) FROM main.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            msg = f"""No table named "{table}" was found in the database located at "{path}" """
            warnings.warn(msg)
        
        
        for attribute, prefix in zip(("sqlite_create", "sqlite_trigger", "sqlite_index", "sqlite_view"),
                                     ("CREATE TABLE ", "CREATE TRIGGER ", "CREATE INDEX ", "CREATE VIEW ")):
            
            
            if getattr(cls, attribute) is None or getattr(cls, attribute).fget(cls) is None:
                continue
                
            sql = getattr(cls, attribute).fget(cls)
            
            if not sql.strip().startswith(prefix):
                msg = f"Property '{attribute}' does not meet the expected format as it does not start with {prefix}"
                warnings.warn(msg)
                
    @staticmethod
    @final
    def factory_el0(c: sqlite3.Cursor, row: tuple) -> int | str | float:
        """Row factory that returns the first element"""
        return row[0]
        
    @staticmethod
    @final
    def factory_dict(c: sqlite3.Cursor, row: tuple) -> Dict[str, int | str | float]:
        """Row factory that returns the first element"""
        return {c[0]: row[i] for i, c in enumerate(c.description)}
        
        
        
        
        