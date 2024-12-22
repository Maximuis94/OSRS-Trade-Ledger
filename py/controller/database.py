"""
Controller for a sqlite database

"""
import os
import time
from collections.abc import Iterable

from venv_auto_loader.active_venv import *
import global_variables.classes as _cls
from global_variables.importer import *
from model.database import Database

__t0__ = time.perf_counter()

class DatabaseController(Database):
    """
    Controller class for sqlite3 databases
    
    """
    
    def __init__(self, path: str, **kwargs):
        """"""
        super().__init__(path, **kwargs)
    
    def count_rows_for_item(self, item_id: int, srcs: Iterable = None) -> int:
        sql = f"SELECT COUNT(*) FROM item{item_id:0>5}"
        if srcs is not None:
            sql += f" WHERE src IN {str(tuple(srcs))}"
        return self.execute(sql, factory=0).fetchone()
    
    def set_aggregation_function(self, agg, name: str, table: str, columns: str, Iterable = None):
        if columns is None:
            columns = list(self.tables.get(table).column_list.keys())
        self.create_aggregate(name, 1 if isinstance(columns, str) else len(columns), aggregate_class=agg)
        self.aggregation_protocols[name] = _cls.AggregateFunction(name=name, cursor=self.cursor(), agg_class=agg, columns=columns, table=table)
        # self.aggregation_protocols[a.name] = a
    
    def aggregate(self, name: str, exe_type='select', **kwargs):
        a = self.aggregation_protocols.get(name)
        if not isinstance(a, _cls.AggregateFunction):
            raise ValueError
        
        if exe_type == 'select':
            return a.aggregate_select(**kwargs).fetchall()
    
    # def aggregate_select(self, table: str, suffix: str = ''):
    #     (f"SELECT {self.name}({str(self.columns)}) FROM '{table}' "+suffix)
    #     self.cursor.execute(f'SELECT {self.name}({str(self.columns)}) FROM "{table}" '+suffix).fetchone()
    


if __name__ == '__main__':
    import global_variables.variables as var
    ...
    