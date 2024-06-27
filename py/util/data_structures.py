"""
Module with various methods related to data structures like sqlite databases, but also pandas dataframes.
Methods in this module are extensively commented

"""
import os.path
import sqlite3
import time
from collections import namedtuple
from typing import List, Type, Dict, Callable, NamedTuple, Tuple

import global_variables.values as gv
import global_variables.variables as var
from file.file import File
from global_variables.data_classes import TimeseriesRow, Avg5mDatapoint, RealtimeDatapoint, WikiDatapoint
from util import str_formats as fmt


def get_sorted_tuple(_list, reverse_sort: bool = False) -> tuple:
    """ Convert `_list` to a list, sort it and return it as a tuple. """
    _list = list(_list)
    _list.sort(reverse=reverse_sort)
    return tuple(_list)


def dict_factory(sqlite_cursor: sqlite3.Cursor, row) -> dict:
    """ Method that can be set as row_factory of a sqlite3.Connection so it will return dicts """
    return {col[0]: row[idx] for idx, col in enumerate(sqlite_cursor.description)}


def connect(db_file: File, set_row_factory: bool = True, prt: bool = False) -> sqlite3.Connection:
    """
    Connect with the sqlite database specified by `db_file`. The database is assumed to exist; an error will be raised
    if it does not.
    
    Parameters
    ----------
    db_file : str
        Path to the database to connect to.
    set_row_factory : bool, optional, True by default
        If True, configure a row dict factory for this connection
    prt : bool, optional, False by default
        True to print updates on the established connection

    Returns
    -------
    sqlite3.Connection
        The established connection with the database
    
    Raises
    ------
    FileNotFoundError
        If `db_file` does not exist, a FileNotFoundError is raised.

    """
    if not db_file.exists():
        raise FileNotFoundError(f'Unable to connect to non-existent db file {db_file}')
    con = sqlite3.connect(db_file.path)
    if prt:
        print(f'Connected with sqlite database at {db_file}')
    if set_row_factory:
        con.row_factory = dict_factory
    return con
    

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


def exe_select_one(cursor: sqlite3.Cursor, sql: str, parameters: dict = None) -> any:
    """ Execute `sql` via `cursor` with `parameters` as parameters. Return as one row. """
    return cursor.execute(sql, parameters).fetchone()


def exe_select(cursor: sqlite3.Cursor, sql: str, *args, **kwargs) -> List[any]:
    """
    Execute sql select query `sql` via `cursor` with `parameters` as parameters. Return a specific amount of rows if
    `n_rows` is passed.
    
    Parameters
    ----------
    cursor : sqlite3.Cursor
        cursor object with a link to the db that is to be queried
    sql : str
        executable sql statement for querying rows
    
    Other Parameters
    ----------------
    

    Returns
    -------
    List[any]
        A list of rows that meet the requirements specified in `sql` is returned

    """
    n_rows = kwargs.get('n_rows')
    
    if n_rows is None:
        return cursor.execute(sql, args[0] if len(args) > 0 else kwargs.get('parameters')).fetchall()
    else:
        return cursor.execute(sql, args[0] if len(args) > 0 else kwargs.get('parameters')).fetchmany(n_rows)


def datapoint(row: dict) -> List[TimeseriesRow]:
    """ Parse dict `row`, convert it to the appropriate LegacyDatapoint and then to a TimeseriesDatapoint """
    row = {k: row.get(k) for k in var.legacy_keys if row.get(k) is not None}
    keys = tuple(row.keys())
    
    for idx, _cls in enumerate((Avg5mDatapoint, RealtimeDatapoint, WikiDatapoint)):
        if keys == _cls.__match_args__:
            # return True
            return [_cls(**row).convert_datapoint(True), _cls(**row).convert_datapoint(False)] if idx == 0 else \
                [_cls(**row).convert_datapoint()]
    raise ValueError(f'Unable to determine LegacyRow typing for row {row}')


if __name__ == "__main__":
    nt = namedtuple('Avg5mDatapoint', ['timestamp', 'buy_price', 'buy_volume', 'sell_price', 'sell_volume'])
    for el in get_dtypes(nt):
        print(el)
    # for k, v in nt.__dict__.items():
    #     print(k, v)
    exit(1233)
    get_dtypes(nt)