"""
This module contains implementations for generic classes

"""
import abc
import sqlite3
from collections.abc import Iterable


class SingletonMeta(type):
    """
    Singleton class that LocalFiles classes will be derived from. Ensures that all instances of the classes are
    identical. Since LocalFile classes serve as an interface with an actual local file of which there is only one
    instance, this seems fitting.
    
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    
    Source: https://refactoring.guru/design-patterns/singleton/python/example
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class AggregateFunction(abc.ABC):
    """
    Wrapper class that can be set as an aggregation function in a Database.
    
    """
    def __init__(self, name: str, table: str, cursor: sqlite3.Cursor, agg_class, columns: str or Iterable, **kwargs):
        self.name = name
        self.cursor = cursor
        self.n_args = -1
        self.columns = ''
        self.agg = agg_class
        self.table = table
        
        self.set_columns(columns)
        
        for kw in frozenset(self.__dict__.keys()).intersection(list(kwargs.keys())):
            self.__dict__[kw] = kwargs.get(kw)
            print(kw, kwargs.get(kw))
        ...
    
    def set_cursor(self, c: sqlite3.Cursor):
        self.cursor = c
        
    def set_columns(self, columns):
        if isinstance(columns, str):
            self.columns = [columns]
        self.columns = str(tuple(columns))[1:-1].replace("'", "")
        self.columns = self.columns.strip(",")
    
    def aggregate_select(self, suffix: str = '', columns: str or Iterable = None):
        """
         Apply the aggregation method through SELECT and return the result

        Parameters
        ----------
        table : str
            Name of the table the method is applied to
        columns: str or Iterable, optional, '*' by default
            Columns the
        suffix : str
            String to append to 'SELECT self.name(columns) FROM table' sql exe

        Returns
        -------

        """
        if not isinstance(self.cursor, sqlite3.Cursor):
            raise TypeError
        
        if columns is not None:
            self.set_columns(columns)
        return self.cursor.execute(f"SELECT {self.name}({self.columns}) FROM main.{self.table} {suffix}")
