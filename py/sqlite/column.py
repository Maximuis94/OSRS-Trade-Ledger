"""
Module in which a Column representation is defined.
Additionally, this module contains logic to automatically derive a set of columns using an SQLite database connection.


"""
import sqlite3
from collections.abc import Container
from typing import NamedTuple, List

from sqlite._util import convert_dtype
from sqlite.models import ForeignKey
from sqlite.row_factories import factory_idx0


class Column(NamedTuple):
    """ CLass that represents a Column of a Table within a SQLite Database """
    id: int
    name: str
    table: str
    sql_dtype: str
    
    is_primary_key: bool = False
    is_nullable: bool = False and not is_primary_key
    is_unique: bool = False and not is_nullable
    
    check: Container = None
    foreign_key: ForeignKey = None
    default: any = None
    
    def __repr__(self):
        return self.name
    
    def create(self):
        """ Return a partial sqlite create table statement for this column """
        return f"""'{self.name}' {self.sql_dtype}
            {"" if self.is_nullable else "NOT NULL "}
            {"UNIQUE " if self.is_unique else ""}
            {"" if self.default is None else f"DEFAULT {self.default} "}
            {"" if self.check is None else f"CHECK ({str(self.check)[1:-1].replace(', ', ' AND ')})"}"""
    
    @staticmethod
    def parse_columns(c: sqlite3.Cursor, table: str) -> list:
        """ Given an SQLite3 cursor, extract and return a list of Columns from the CREATE TABLE sql of sqlite_master """
        c.row_factory = factory_idx0
        output = []
        sql = c.execute("""SELECT sql FROM sqlite_master WHERE tbl_name=? """, (table,)).fetchone().replace("\t", " ")
        if sql is None:
            raise ValueError(f"Unable to find table '{table}' in sqlite_master")
        pk_idx = sql.index("PRIMARY KEY(") + 12
        pks = [pk.strip(""""'""") for pk in sql[pk_idx:sql.index(")", pk_idx)].split(")")[0].split(",")]
        for col in sql[sql.index("(") + 1:].split(","):
            cur = col.strip().split(" ")
            
            # print(col)
            # print(cur)
            try:
                py_dtype = convert_dtype(cur[1])
                name = cur[0].strip(""""'""")
            except ValueError:
                break
            except IndexError:
                break
            
            if name.strip(""""'""") in pks:
                kws = {"is_primary_key": True}
            else:
                kws = {"is_nullable": " NOT NULL " not in col, "is_unique": " UNIQUE " in col}
            
            try:
                kws["default"] = py_dtype(cur[cur.index("DEFAULT") + 1])
            except ValueError:
                ...
            
            try:
                check = col.split('CHECK')[1]
                kws["check"] = check[check.index("(") + 1:check.index(")")]
            except IndexError:
                ...
            
            output.append(Column(id=len(output), name=name, table=table, sql_dtype=cur[1], **kws))
        
        if len(output) > 0:
            return output
        else:
            raise ValueError(f"""Unable to extract columns from SQLite statement '{sql}'""")
