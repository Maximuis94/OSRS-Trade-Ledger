import sqlite3
from dataclasses import dataclass

from file.file import File
from inventory.constants import empty_tuple, transaction_db
import global_variables.path as gp
import os


@dataclass(frozen=True, slots=True, match_args=True)
class SQL:
    query: str
    parameters: tuple or dict = empty_tuple
    read_only: None or bool = False
    db: File = transaction_db
    # db: File = File(os.path.join(gp.dir_data, "local_.db"))
    
    @property
    def con(self) -> sqlite3.Connection:
        """Connection to the database; read-only if read_only==True"""
        # print(self.db)
        return sqlite3.connect(self.db if self.read_only is None or not self.read_only else f"file:{self.db}?mode=ro", uri=True)
    
    def __str__(self):
        if self.query.startswith("INSERT INTO"):
            return self.query.split("VALUES")[0] + "VALUES " + str(self.parameters)
        elif self.query.startswith("UPDATE"):
            return self.query + " " + str(self.parameters)
        return self.query