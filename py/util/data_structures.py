"""
Module with various methods related to data structures like sqlite databases, but also pandas dataframes.
Methods in this module are extensively commented

"""
import sqlite3
from collections.abc import Iterable
from typing import Callable, Dict
from venv_auto_loader.active_venv import *

import global_variables.variables as var
from global_variables.datapoint import Avg5mDatapoint, RealtimeDatapoint, WikiDatapoint

__t0__ = time.perf_counter()

def get_sorted_tuple(_list, reverse_sort: bool = False) -> tuple:
    """ Convert `_list` to a list, sort it and return it as a tuple. """
    _list = list(_list)
    _list.sort(reverse=reverse_sort)
    return tuple(_list)


def dict_factory(sqlite_cursor: sqlite3.Cursor, row) -> dict:
    """ Method that can be set as row_factory of a sqlite3.Connection so it will return dicts """
    return {col[0]: row[idx] for idx, col in enumerate(sqlite_cursor.description)}
    

def get_df_dtype(column_name: str) -> str:
    """ Return the pandas.DataFrame dtype of the given `column_name`. """
    output = var.df_dtypes.get(column_name)
    if output is None:
        raise ValueError(f"Unable to convert `column_name` {column_name} to a dtype.")
    else:
        return output


def get_sqlite_dtype(value=None, column_name: str = None) -> str:
    """ Return the sqlite dtype of the given `value` or `column_name` """
    if isinstance(column_name, str):
        return convert_dtype_df_sqlite(get_df_dtype(column_name))
    if isinstance(value, bool):
        return 'INTEGER'
    elif isinstance(value, str):
        return 'TEXT'
    elif isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'REAL'
    else:
        return 'NULL'


def get_dtypes(t: Callable and NamedTuple) -> (str, Tuple[str], Dict[str, str], Callable):
    """ Given a NamedTuple class that represents a timeseries datapoint, return variables for setting up its class """
    return t.__name__[:-5].lower(), t._fields, {c: var.types.get(c).df for c in t._fields}, lambda c, row: t(*row)


def convert_dtype_df_sqlite(df_dtype: str) -> str:
    """ Convert the given `df_dtype` to its corresponding sqlite dtype (e.g. UInt32 -> INTEGER) """
    var.get_dtype(df_dtype)
    return var.dtypes_by_df.get(df_dtype).sql


def datapoint(row: dict) -> List[any]:
    """ Parse dict `row`, convert it to the appropriate LegacyDatapoint and then to a TimeseriesDatapoint """
    row = {k: row.get(k) for k in var.legacy_keys if row.get(k) is not None}
    keys = tuple(row.keys())
    
    for idx, _cls in enumerate((Avg5mDatapoint, RealtimeDatapoint, WikiDatapoint)):
        if keys == _cls.__match_args__:
            # return True
            return [_cls(**row).convert_datapoint(True), _cls(**row).convert_datapoint(False)] if idx == 0 else \
                [_cls(**row).convert_datapoint()]
    raise ValueError(f'Unable to determine LegacyRow typing for row {row}')


def update_existing_dict_values(to_update: dict, new_values: dict) -> dict:
    """ Update `to_update` with values from `new_values`, but only with keys found in both dicts """
    for k in frozenset(new_values).intersection(to_update):
        to_update[k] = new_values.get(k)
    return to_update


def remove_dict_entries(_dict: dict, keys: Iterable) -> dict:
    """ Removes entries from `_dict` under keys found in `keys` and returns the resulting dict """
    for k in frozenset(_dict).intersection(keys):
        del _dict[k]
    return _dict
