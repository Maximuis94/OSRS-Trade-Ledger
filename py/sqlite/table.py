"""
Module in which a Table representation within an SQLite database is defined.


"""
import sqlite3
from abc import abstractmethod
from collections import namedtuple
from dataclasses import dataclass
from typing import Sequence, Callable, Iterable

from sqlite.column import Column


@dataclass(match_args=True, init=False)
class Table:
    """
    CLass that represents a Table within a SQLite Database. A Table is defined as an entity with a name and one or more
    Columns it is computed of. A table is treated as a static entity, therefore its core attributes cannot be modified.
    Tables are used by Databases to fetch SQLite statements applicable to that specific table
    
    A Table has various methods for generating corresponding executable SQLite statements. Naming is defined as a set of
    two words that describe intent unambiguously. E.g. select_from; delete_from, insert_into. From/into always refers to
    the instance of the Table.
    
    Attributes
    ----------
    id : int
        A numerical identifier of the table
    name : str
        The name of the table
    columns : Sequence[Column]
        One or more Column instances this Table is composed of
    row_factory : Callable, optional, None by default
        The method to invoke for generating a single row from the SQLite database.
        
    """
    id: int
    name: str
    columns: Sequence[Column]
    
    row_factory: Callable = None
    
    def __init__(self, c: sqlite3.Connection, name: str, row_factory: Callable = None, **kwargs):
        self.name = name
        
        # c.row_factory = self.factory_column
        self.columns = Column.parse_columns(c.cursor(), table=name)
        
        self.Row = namedtuple(f'{name}Row', [c.name for c in self.columns])
        self.row_factory = lambda _, row: self.Row(*row) if row_factory is None else row_factory
        
        # Set CRUD methods that were passed as keyword args
        for key in ['create', 'read', 'update', 'delete']:
            try:
                override_method = kwargs[key]
                if isinstance(override_method, Callable):
                    self.__setattr__(key, override_method)
                else:
                    raise TypeError(f"Override method for {key} is not Callable")
            except KeyError:
                ...
        
    def create_table(self, check_exists: bool = False) -> str:
        """ Generate an executable SQLite statement for this table, given the columns it's composed of. """
        sql = f""" CREATE TABLE {'IF NOT EXISTS ' if check_exists else ''} "{self.name}" ("""
        pk = "PRIMARY KEY("
        columns = {column.id: column for column in self.columns}
        
        ids = list(columns.keys())
        ids.sort()
        
        for column_id in ids:
            column = columns[column_id]
            sql += f"{column}, "
            if column.is_primary_key:
                pk += f"{column.name}, "
        
        return sql.rstrip(", ") + pk.rstrip(", ") + ") )"
    
    @abstractmethod
    def create(self, **kwargs) -> str:
        """ Return an SQLite statement for inserting rows into the database """
        columns = tuple(self.columns)
        return f"""INSERT INTO "{self.name}" {columns} VALUES {tuple(["?" for _ in range(len(columns))])}"""

    @abstractmethod
    def read(self, **kwargs) -> str:
        """ Return an SQLite statement for reading rows from the database """
        return f"""SELECT * FROM "{self.name}" """

    @abstractmethod
    def update(self, columns: str or Iterable[str or Column], where_clause: str, **kwargs) -> str:
        """ Return an SQLite statement for updating rows in the database """
        if not isinstance(columns, str):
            columns = str([f"{col}=?" for col in columns])[1:-1]
        
        return f"""UPDATE "{self.name}" SET {columns} WHERE {where_clause.lstrip("WHERE ")} """

    @abstractmethod
    def delete(self, where_clause: str, **kwargs) -> str:
        """ Return an SQLite statement for deleting rows from the database """
        return f"""DELETE FROM "{self.name}" WHERE {where_clause.lstrip("WHERE ")} """.rstrip("WHERE ")
        
    # @staticmethod
    # def factory_columns(c, row) -> Tuple[Column, ...]:
    #     return tuple([Column(idx, col[0], Table.name, ) for idx, col in enumerate(c.description)])
    
    def __repr__(self):
        return f"""Table {self.name} \n{str([c.create() for c in self.columns]).replace(", ", "\n\t")[1:-1]}"""
