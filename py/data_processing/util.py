"""
This module contains various util methods for analyzing npy db data.



"""
from enum import Enum

import datetime
import sqlite3
from collections.abc import Iterable, Sized
from typing import List, Optional, Sequence

import numpy as np
from multipledispatch import dispatch

from venv_auto_loader.active_venv import *
import util.unix_time as ut
from global_variables.datapoint import NpyDatapoint as Dp

__t0__ = time.perf_counter()

default_t1 = int(time.time())
dp_select_prefix = f"""SELECT {str(Dp.__match_args__)[1:-1]} FROM """.replace("'", "")


def npy_datapoint_factory(c: sqlite3.Cursor, row: tuple) -> Dp:
    """ Row factory that produces instances of the NpyDatapoint class """
    return Dp(*row)


# read-only sqlite3 Connection object with npy db
# _db = sqlite3.connect(database=f"file:{gp.f_db_npy}?mode=ro", uri=True)
# _db.row_factory = npy_datapoint_factory

# if not verify.db_dataclass(tuple([str(el[0]) for el in _db.execute("SELECT * FROM item00002 LIMIT 1").description]),
#                            dataclass=Dp):
#     print(f'Mismatch between dataclass columns and database columns may result in unintended behaviour.'
#           f'Consider updating the dataclass so its attributes match the database table.')


def floor_ts(timestamp: int, m: int = 300) -> int:
    """ Floor `timestamp` to the nearest value where `timestamp`%`m`==0 """
    return int(timestamp - timestamp % m)


def floor_timestamp_window_size(timestamp: int, window_size: int) -> int:
    """
    Floor a given timestamp to the nearest lower multiple of the window size.

    This function takes a timestamp and a window size, and returns the greatest multiple
    of the window size that is less than or equal to the given timestamp. The result can
    be useful for aligning timestamps to consistent time windows for purposes such as data
    aggregation or temporal analysis.

    Parameters
    ----------
    timestamp : int
        The timestamp to be floored. Typically, this is represented as an integer value
        (e.g., seconds or milliseconds since the epoch).
    window_size : int
        The window size by which the timestamp should be rounded down. This must be a positive
        integer representing the size of the window.

    Returns
    -------
    int
        The floored timestamp, which is the largest multiple of `window_size` that does not
        exceed the input `timestamp`.

    Raises
    ------
    ValueError
        If `window_size` is not a positive integer.

    Examples
    --------
    >>> floor_timestamp_window_size(12345, 1000)
    12000
    >>> floor_timestamp_window_size(9876, 500)
    9500
    """
    return int(timestamp / window_size) * window_size


def get_min_value(values: Sequence[int | float], decimal_places: int | None = None) -> Optional[int | float]:
    """
    Retrieve the minimum value from a sequence of numeric values and optionally constrain
    the result to a specific number of decimal places.

    This function iterates over a sequence of numbers and returns the smallest element.
    If `decimal_places` is provided, the minimum value is rounded to the specified number
    of decimals. This is useful for ensuring consistent precision in numerical computations.

    Parameters
    ----------
    values : Sequence[int | float]
        A non-empty sequence of numeric values (e.g., list, tuple) from which to
        determine the minimum.
    decimal_places : int | None, optional
        The number of decimal places to round the result to. If set to None (the default),
        the returned value will not be rounded.

    Returns
    -------
    int | float
        The smallest numerical value within the `values` sequence, rounded to the specified
        number of decimals if `decimal_places` is provided.
    None
        If the Sequence provided is empty, return None

    Raises
    ------
    TypeError
        If `decimal_places` is provided and is not a non-negative integer.

    Examples
    --------
    >>> get_min_value([0, -1, 3, 5, 2])
    -1
    >>> get_min_value([3.1415, 2.718, 1.414])
    1.414
    >>> get_min_value([3.1415, 2.718, 1.414], decimal_places=2)
    1.41
    """
    if len(values) == 0:
        return None
    
    min_val = min(values)
    
    if decimal_places is not None:
        if not isinstance(decimal_places, int) or decimal_places < 0:
            raise TypeError("decimal_places must be a non-negative integer.")
        min_val = round(min_val, decimal_places)
    
    return min_val


def get_max_value(values: Sequence[int | float], decimal_places: int | None = None) -> Optional[int | float]:
    """
    Retrieve the maximum value from a sequence of numeric values and optionally constrain
    the result to a specific number of decimal places.

    This function iterates over a sequence of numbers and returns the largest element.
    If `decimal_places` is provided, the maximum value is rounded to the specified number
    of decimals. This is useful for ensuring consistent precision in numerical computations.

    Parameters
    ----------
    values : Sequence[int | float]
        A non-empty sequence of numeric values (e.g., list, tuple) from which to
        determine the maximum.
    decimal_places : int | None, optional
        The number of decimal places to round the result to. If set to None (the default),
        the returned value will not be rounded.

    Returns
    -------
    int | float
        The largest numerical value within the `values` sequence, rounded to the specified
        number of decimals if `decimal_places` is provided.
    None
        If the Sequence provided is empty, return None

    Raises
    ------
    TypeError
        If `decimal_places` is provided and is not a non-negative integer.

    Examples
    --------
    >>> get_max_value([0, -1, 3, 5, 2])
    5
    >>> get_max_value([3.1415, 2.718, 1.414])
    3.1415
    >>> get_max_value([3.1415, 2.718, 1.414], decimal_places=2)
    3.14
    """
    if len(values) == 0:
        return None
    
    max_val = max(values)
    
    if decimal_places is not None:
        if not isinstance(decimal_places, int) or decimal_places < 0:
            raise TypeError("decimal_places must be a non-negative integer.")
        max_val = round(max_val, decimal_places)
    
    return max_val


def get_percentile_value(values: Sequence[int | float], percentile: float, decimal_places: int | None = 2) \
        -> Optional[float]:
    """
    Retrieve the value at a specific percentile from a sequence of numeric values
    and optionally constrain the result to a specific number of decimal places.

    This function computes the given percentile of the input sequence where the
    percentile is specified as a fraction between 0 and 1 (e.g., 0.25 for Q1, 0.5 for
    the median, 0.75 for Q3). The calculation is performed using NumPy's percentile
    functionality after converting the fractional percentile to a percentage. The result
    is then optionally rounded to the desired number of decimal places.

    Parameters
    ----------
    values : Sequence[int | float]
        A non-empty sequence of numeric values (e.g., list, tuple) from which to compute
        the percentile.
    percentile : float
        The desired percentile represented as a fraction between 0 and 1 (inclusive). For example,
        0.95 for the 95th percentile.
    decimal_places : int | None, optional
        The number of decimal places to which the result should be rounded. If set to None,
        no rounding will be applied. The default is 2.

    Returns
    -------
    float
        The value at the specified percentile of the `values` sequence, rounded to the specified
        number of decimal places if `decimal_places` is provided.
    None
        If the Sequence provided is empty, return None

    Raises
    ------
    ValueError
        If `percentile` is not between 0 and 1.
    TypeError
        If `decimal_places` is provided and is not a non-negative integer.

    Examples
    --------
    >>> get_percentile_value([1, 2, 3, 4, 5], 0.95)
    5.0
    >>> get_percentile_value([1, 2, 3, 4, 5], 0.95, decimal_places=1)
    5.0
    >>> get_percentile_value([1, 2, 3, 4, 5], 0.5)
    3.0
    """
    if len(values) == 0:
        return None
    
    if not (0 <= percentile <= 1):
        raise ValueError("percentile must be a fraction between 0 and 1 (inclusive).")
    
    if decimal_places is not None:
        if not isinstance(decimal_places, int) or decimal_places < 0:
            raise TypeError("decimal_places must be a non-negative integer or None.")
    
    # Convert fractional percentile to percentage for np.percentile.
    q = percentile * 100
    value = np.percentile(values, q)
    
    if decimal_places is not None:
        value = round(value, decimal_places)
    
    return float(value)


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


class WINDOW_SIZE(Enum):
    DAY = 86400
    WEEK = 604800
    HOUR = 3600
    
    def __int__(self) -> int:
        return self.value


def dt_utc_ts(timestamp: int) -> datetime.datetime:
    """Converts `timestamp` into a datetime.datetime object that aligns with the UTC timezone and returns it"""
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC)


def floor_ts_day(timestamp: int) -> int:
    """Floor `timestamp` to 12am UTC time and return it"""
    return timestamp - timestamp % WINDOW_SIZE.DAY.value


def floor_ts_week(timestamp: int) -> int:
    """Floor `timestamp` to monday 12am UTC time and return it"""
    return timestamp - timestamp % WINDOW_SIZE.WEEK.value - 259200
