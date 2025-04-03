"""
Abstract Local Database Entity class

"""
import sqlite3
from dataclasses import field
from typing import Tuple

import global_variables.path as gp
from entity._entity import DbEntity


class LocalDbEntity(DbEntity):
    _columns: Tuple[str, ...] = field(init=False, repr=False)
    
    @property
    def sqlite_path(self) -> str:
        """The path to the database associated with this Entity"""
        return str(gp.f_db_local)
    
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
