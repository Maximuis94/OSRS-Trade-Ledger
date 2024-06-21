"""
This module contains the model for an NpyArray of an OSRS item.
An NpyArray captures timeseries data for an item within a specified scope. Most of its attributes are derived by
combining timeseries data from various sources.

Specific attributes are described in the Attributes section of the NpyArray class docstring.
"""
from collections.abc import Iterable
from typing import Tuple, List

import numpy as np

import global_variables.configurations as cfg
import global_variables.path as gp
import global_variables.variables as var
from controller.item import create_item
from global_variables.data_classes import NpyArray as _NpyArray, NpyDatapoint, TimeseriesDatapoint
from model.database import Database
from model.item import Item
from sqlite.row_factories import factory_npy_row

ts_volume_sub = 86400*(cfg.n_days_volume_avg+1)


class NpyArray(_NpyArray):
    """
    NpyArray class. The base data class is defined in global_variables.data_classes to prevent an excessive amount of
    attributes being listed here.
    
    Note that the arrays have been integrated almost entirely into the sqlite db, due to better performance.
    
    """
    columns: Tuple[str] = NpyDatapoint.__match_args__
    attributes = _NpyArray.__match_args__
    attributes_fz = frozenset(attributes)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.item_id = self.item_id[0]
    
    @staticmethod
    def get_col(np_ar: np.ndarray, column_name: str) -> np.ndarray:
        """ Extract the values corresponding to `column_name` col from 2d array `np_ar` using NpyArray.columns idx """
        return np_ar[:, NpyArray.columns.index(column_name)]
    
    @staticmethod
    def npy_array_file(item_id: int) -> str:
        """ Return the path the npy arrays for `item_id` will be saved at """
        return gp.dir_npy_arrays + f'{item_id:0>5}.npy'
    
    @staticmethod
    def save_npy_arrays(item_id: int, column_names: Iterable[str], array_values: np.ndarray) -> bool:
        """
        Save the npy arrays `np_ar` with column index `np_ar_columns` in the npy file for `item_id`
        Parameters
        ----------
        item_id : int
            The item_id affiliated with the arrays
        column_names : Iterable
            List of the column names that can be used to access array_values in `array_values`
        array_values : np.ndarray
        
        Returns
        -------

        """
        raise NotImplementedError

    @staticmethod
    def load_npy_array_sql(item_id: int):
        """ Load columns and arrays for this item and return them as a tuple. """
        return NpyArray.columns, Database(gp.f_db_npy, read_only=True).execute(f"SELECT * FROM item{item_id:0>5}", factory=dict).fetchall()
        # return NpyArray.columns, npy_db.execute(f"SELECT * FROM item{item_id:0>5}", factory=tuple).fetchall()

    @staticmethod
    def load(item_id: int):
        """ Load columns and arrays for this item and return them as a tuple. """
        item = create_item(item_id).__dict__
        cols, np_ars = NpyArray.load_npy_array(item_id)
        item.update({col: np_ars[NpyArray.columns.index(col)] for col in cols})
        item['path'] = NpyArray.npy_array_file(item_id)
        return NpyArray(**{key: item.get(key) for key in NpyArray.attributes_fz.intersection(tuple(item.keys()))})
        
    @staticmethod
    def load_npy_array(item_id: int):
        """ Load columns and arrays for this item and return them as a tuple. """
        # print(len(dt_keys))
        cols = NpyArray.columns
        values = [[] for _ in range(len(cols))]
        for row in NpyArray.load_npy_array_sql(item_id)[1]:
            for col, el in row.items():
                values[cols.index(col)].append(el)
        ar = []
        err = None
        for c, v in zip(cols, values):
            try:
                ar.append(np.array(v, dtype=var.np_array_dtypes.get(c)))
            except OverflowError as e:
                err = e
                import global_variables.osrs as go
                
                print(item_id, go.id_name[item_id], c, max(v), '\n')
        
        if err is not None:
            raise err
            
        return cols, ar


class NpyDb(Database):
    """
    Class for interacting with the NpyDb. This is not the same class as used while updating the database; this instance
    is read-only and tailored for fetching data. It can be used, among others, to supply plots with data.
    
    Rows from this database combine a large amount of data from various sources. Returned rows are dataclass instances.
    """
    
    def __init__(self, path: str = gp.f_db_npy):
        super().__init__(path, row_factory=factory_npy_row, parse_tables=False, read_only=True)
        self.add_cursor(key=TimeseriesDatapoint, rf=lambda c, row: TimeseriesDatapoint(*row))
        
    def fetch_rows(self, item_id: int, t0: int = None, t1: int = None) -> List[NpyDatapoint]:
        """ Fetch augmented rows for item `item_id` within the specified timestamp(s) `t0` and `t1` """
        if t1 is not None and t0 is not None:
            return self.execute(f"SELECT * FROM item{item_id:0>5} WHERE timestamp BETWEEN ? AND ?", (t0, t1)).fetchall()
        if t0 is not None:
            return self.execute(f"SELECT * FROM item{item_id:0>5} WHERE timestamp >= ? ", (t0,)).fetchall()
        if t1 is not None:
            return self.execute(f"SELECT * FROM item{item_id:0>5} WHERE timestamp <= ? ", (t1,)).fetchall()
        return self.execute(f"SELECT * FROM item{item_id:0>5}").fetchall()
    
    def fetch_plot_rows(self, item_id: int, y_value: str, t0: int = None, t1: int = None):
        """ Fetch timestamps and `y_value` rows for item `item_id` within the specified timestamp(s) `t0` and `t1` """
        if t1 is not None and t0 is not None:
            return self.execute(f"SELECT timestamp, {y_value} FROM item{item_id:0>5} WHERE timestamp BETWEEN ? AND ?",
                                (t0, t1), factory=TimeseriesDatapoint).fetchall()
        if t0 is not None:
            return self.execute(f"SELECT timestamp, {y_value} FROM item{item_id:0>5} WHERE timestamp >= ? ",
                                (t0,), factory=TimeseriesDatapoint).fetchall()
        if t1 is not None:
            return self.execute(f"SELECT timestamp, {y_value} FROM item{item_id:0>5} WHERE timestamp <= ? ",
                                (t1,), factory=TimeseriesDatapoint).fetchall()
        return self.execute(f"SELECT timestamp, {y_value} FROM item{item_id:0>5}",
                            factory=TimeseriesDatapoint).fetchall()
    
    def get_columns(self) -> Tuple[str]:
        """ Return the header of the npydb tables as a tuple """
        return NpyDatapoint.__match_args__




if __name__ == '__main__':
    npy_db = NpyDb()
    rows = npy_db.fetch_plot_rows(item_id=2, y_value='buy_price')
    row = rows[0]
    print(row)
    