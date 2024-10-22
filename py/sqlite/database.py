import sqlite3
from collections import namedtuple
from typing import Callable, Dict, Tuple

from sqlite.column import Column
from sqlite.pragmas import IPragma
from sqlite.row_factories import factory_list, factory_dict, factory_idx0, factory_named_tuple, \
    factory_db_content_parser


class Database(IPragma):
    """
    Database class that is directly interacted with outside of this module. Serves as an interface to interact with the
    database without having to compose various SQLite statements.
    
    
    """
    def __init__(self, path: str, TableTuple: Callable = None):
        self.path = path
        # Establish a read-only connection
        self._c = sqlite3.connect(database=f"file:{path}?mode=ro", uri=True)
        
        self.TableTuple = TableTuple
        
        # self.RowTuple = named_tuple
        # if named_tuple is not None:
        #     self.set_default_factory()
        
        # Initialize read-only cursors for a specific set of row factories
        self._c_list = self._c.cursor()
        self._c_list.row_factory = factory_list
        self._c_dict = self._c.cursor()
        self._c_list.row_factory = factory_dict
        self._c_idx0 = self._c.cursor()
        self._c_idx0.row_factory = factory_idx0
        
        self._c_table_row: Dict[str, sqlite3.Cursor] = {}
    
    def set_table_factory(self, table_name: str, ):
        """ Set/update the row_factory assigned to cursor `_c` to `factory` """
        self._c.row_factory = (lambda c, row: factory_named_tuple(c, row, self.RowTuple) if factory is None else factory)
    
    def parse_tables(self, db: sqlite3.Connection):
        self._c.row_factory = factory_db_content_parser
        
        if self.TableTuple is None:
            self.TableTuple = namedtuple('TableTuple', self._c_list.execute("SELECT tbl_name FROM main.sqlite_master WHERE type='table'"))
        
        for table in self._c.execute("""SELECT * FROM main.sqlite_master WHERE type='table'""").fetchall():
            columns: Tuple[Column]
            
            
            
            
            _c = self._c.cursor()
            
            
            
            _c.row_factory = lambda c, row: factory_named_tuple(c, row, ...)
