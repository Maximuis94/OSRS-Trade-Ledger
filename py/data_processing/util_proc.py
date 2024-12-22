"""
This module contains various util methods for analyzing npy db data.



"""
import datetime
import sqlite3
import time
import warnings
from collections.abc import Iterable, Sized
from typing import List

from multipledispatch import dispatch

from venv_auto_loader.active_venv import *
import util.verify as verify
import util.unix_time as ut
from global_variables import path as gp
from global_variables.data_classes import NpyDatapoint as Dp
__t0__ = time.perf_counter()

default_t1 = int(time.time())
dp_select_prefix = f"""SELECT {str(Dp.__match_args__)[1:-1]} FROM """.replace("'", "")


def npy_datapoint_factory(c: sqlite3.Cursor, row: tuple) -> Dp:
    """ Row factory that produces instances of the NpyDatapoint class """
    return Dp(*row)


# read-only sqlite3 Connection object with npy db
_db = sqlite3.connect(database=f"file:{gp.f_db_npy}?mode=ro", uri=True)
_db.row_factory = npy_datapoint_factory

if not verify.db_dataclass(tuple([str(el[0]) for el in _db.execute("SELECT * FROM item00002 LIMIT 1").description]),
                           dataclass=Dp):
    print(f'Mismatch between dataclass columns and database columns may result in unintended behaviour.'
          f'Consider updating the dataclass so its attributes match the database table.')


def floor_ts(timestamp: int, m: int = 300) -> int:
    """ Floor `timestamp` to the nearest value where `timestamp`%`m`==0 """
    return int(timestamp - timestamp % m)


@dispatch(int, int, None or bool)
def get_datapoints(item_id: int, timestamp: int, floor_timestamp: bool = False) -> Dp:
    if timestamp % 300 != 0:
        # Do not warn if I am explicitly told to floor it
        if not floor_timestamp:
            warnings.warn(f"timestamp should be passed as a unix timestamp rounded down per 5 minutes. Given timestamp "
                          f"{timestamp} was floored to {timestamp - timestamp % 300}. Timestamps can be floored by "
                          f"default without this warning with floor_timestamp=True or by passing a timestamp dividable "
                          f"by 300")
        
        timestamp = floor_ts(timestamp)
    return _db.execute(dp_select_prefix+f"""'item{item_id:0>5}' WHERE timestamp=?""", (timestamp,)).fetchone()


@dispatch(int, int, int)
def get_datapoints(item_id: int, t0: int, t1: int = default_t1) -> List[Dp]:
    return _db.execute(dp_select_prefix+f"""'item{item_id:0>5}' WHERE timestamp BETWEEN ? AND ?""", (t0, t1)).fetchall()


@dispatch(int, int, datetime.datetime)
def get_datapoints(item_id: int, t0: int, t1: datetime.datetime = datetime.datetime.now()) -> List[Dp]:
    return _db.execute(dp_select_prefix+f"""'item{item_id:0>5}' WHERE timestamp BETWEEN ? AND ?""", (t0, t1)).fetchall()


@dispatch(int, datetime.datetime, int)
def get_datapoints(item_id: int, t0: datetime.datetime, t1: int = default_t1) -> List[Dp]:
    return _db.execute(dp_select_prefix+f"""'item{item_id:0>5}' WHERE timestamp BETWEEN ? AND ?""", (t0, t1)).fetchall()


@dispatch(int, datetime.datetime, datetime.datetime)
def get_datapoints(item_id: int, t0: datetime.datetime or int, t1: datetime.datetime or int = default_t1) \
        -> List[Dp]:
    """
    Fetch all datapoints between timestamps / datetimes `t0` and `t1` for item `item_id`.
    
    Parameters
    ----------
    item_id : int
        item_id of the item
    t0 : int or datetime.datetime
        Lower bound timestamp / datetime (inclusive)
    t1 : int or datetime.datetime, optional, current time by default
        Upper bound timestamp / datetime (inclusive)

    Returns
    -------
    List[NpyDatapoint]
        List of NpyDatapoint instances for item `item_id` within interval `t0` - `t1`
    """
    return _db.execute(dp_select_prefix + f"""'item{item_id:0>5}' WHERE timestamp BETWEEN ? AND ?""",
                       (ut.loc_dt_unix(t0), ut.loc_dt_unix(t1))).fetchall()


def avg_dp_value(datapoints: List[Dp], attribute_name: str, exclude_zeros: bool = False, n_decimals: int = None):
    """ Compute the average of `attribute_name` across elements of `datapoints`. If exclude_zeros, ignore value=0 """
    result = None
    try:
        result = [el.__getattribute__(attribute_name) for el in datapoints if not exclude_zeros or exclude_zeros and el.__getattribute__(attribute_name) > 0]
        result = sum(result) / len(result) if isinstance(result, Iterable) else result
        if n_decimals is not None:
            result = round(result, n_decimals)
    finally:
        if result is None or isinstance(result, Sized) and len(result) == 0:
            _list = [el.__getattribute__(attribute_name) for el in datapoints if not exclude_zeros or \
                     exclude_zeros and el.__getattribute__(attribute_name) > 0]
            s = f"avg_dp_value() result is None or an empty list for parameters " \
                f"attribute_name={attribute_name}, exclude_zeros={exclude_zeros}, n_decimals={n_decimals}. " \
                f"The list causing this result is: {_list}"
            warnings.warn(s)
        return int(result) if n_decimals == 0 and result is not None else result


if __name__ == '__main__':
    t = time.time()
    for el in get_datapoints(2, int(t-t%86400)):
        print(el.sell_price)
    t -= 3600
    