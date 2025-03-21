from typing import Dict, Any
from pathlib import Path
import importlib

class SQLManager:
    """
    Manages SQL statements by loading them from the sql directory
    and providing easy access to them.
    """
    def __init__(self):
        self.select = importlib.import_module('transaction.sql.select')
        self.insert = importlib.import_module('transaction.sql.insert')
        self.update = importlib.import_module('transaction.sql.update')
        self.view = importlib.import_module('transaction.sql.view')
        self.trigger = importlib.import_module('transaction.sql.trigger')
        self.table = importlib.import_module('transaction.sql.table')
        self.index = importlib.import_module('transaction.sql.index')

    def get_statement(self, module: str, name: str) -> str:
        """Get a SQL statement by module and name"""
        return getattr(getattr(self, module), name)

sql = SQLManager() 