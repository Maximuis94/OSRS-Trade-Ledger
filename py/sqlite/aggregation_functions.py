"""
This module contains SQL aggregate classes.
SQL aggregate classes operate on a number of SQL rows and can be used to compute specific values from these rows.

Aggregate functions can be set to a sqlite3.Connection object by passing the aggregate class to
sqlite3.Connection.create_aggregate()

Aggregate classes can be tested on the sandbox_db, which is a smaller database with the same tables and columns.

"""
import sqlite3
from collections.abc import Callable, Iterable

import global_variables.classes as _cls


class MySum(_cls.AggregateFunction):
    """
    Aggregation function that sums a series of values and returns the sm
    """
    
    def __init__(self, table: str, columns: str or Iterable = ''):
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

if __name__ == '__main__':
    import global_variables.path as gp
    db = sqlite3.connect(f'file:{gp.f_db_sandbox}?mode=ro', uri=True)
    
    
    a = A('count_q', db.cursor(), 1, ['quantity'])
    
    
    
    
    