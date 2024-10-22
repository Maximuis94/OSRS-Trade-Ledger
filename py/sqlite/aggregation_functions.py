"""
This module contains logic for defining custom aggregated functions.
SQL aggregate classes operate on a number of SQL rows and can be used to compute specific values from these rows.

Aggregate functions can be set to a sqlite3.Connection object by passing the aggregate class to
sqlite3.Connection.create_aggregate()

While it is possible to define custom aggregate classes, native aggregate functions exist as well.
Examples of native aggregate functions are;
- COUNT(...) -> Returns a row count of the arg that was passed. E.g. COUNT(DISTINCT COLUMN_NAME) returns the amount of
    values COLUMN_NAME assumes within the associated database. COUNT(*) returns a row-count of rows that match specified
    criteria.
- AVG(...) -> Returns the average value of the column passed as arg across all rows that match specified criteria.
- MIN/MAX (...) -> Returns the lowest and highest value of the column passed as arg, respectively.
As mentioned earlier, these functions capture a specific value from a set of rows found within the database
For a more detailed explanation, see the official docs on aggregated functions (See References)

TODO: Aggregate Functions
    - Define abstract base class
        > What should this look like?
    - Implement a working example


References
----------
https://www.sqlite.org/lang_aggfunc.html
    Official SQLite documentation of aggregate functions
"""
import abc
import sqlite3
from collections.abc import Callable, Iterable

from import_parent_folder import recursive_import


class AggregateFunction(abc.ABC):
    """
    Wrapper class that can be set as an aggregation function in a Database.
    
    TODO: Implement abstract AggregateFunction class

    """
    
    def __init__(self, name: str, table: str, cursor: sqlite3.Cursor, agg_class, columns: str or Iterable, **kwargs):
        raise NotImplemented("TODO: Implement abstract AggregateFunction class")
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
        raise NotImplementedError
        if not isinstance(self.cursor, sqlite3.Cursor):
            raise TypeError
        
        if columns is not None:
            self.set_columns(columns)
        return self.cursor.execute(f"SELECT {self.name}({self.columns}) FROM main.{self.table} {suffix}")


class MySum(AggregateFunction):
    """
    Aggregation function that sums a series of values and returns the sm
    """
    
    def __init__(self, table: str, columns: str or Iterable = ''):
        raise NotImplementedError
        self.count = 0

    def step(self, value):
        self.count += value

    def finalize(self):
        return self.count
    
    def aggregate_select(self, table: str, columns: str or Iterable = '*', suffix: str = ''):
        ...

################################################################################
# Aggregation functions for Transactions
################################################################################


class CountQuantities():
    def __init__(self):
        self.n_traded = 0
    
    def finalize(self):
        print(self.n_traded, 'traded')
        return self.n_traded
    
    def step(self, *args):
        self.n_traded += args[4]
    