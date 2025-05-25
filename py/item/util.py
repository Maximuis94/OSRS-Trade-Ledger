"""
Util functions for Items. By default, the values are extracted from the timeseries database.


"""
import time

import sqlite3
import datetime
from typing import Optional

import global_variables.path as gp
from common.row_factories import factory_idx0
from item.model import Item

# Default timeseries and npy databases that are used
_db_timeseries = sqlite3.connect(f"file:{gp.f_db_timeseries}?mode=ro", uri=True)
_db_timeseries.row_factory = factory_idx0
_db_npy = sqlite3.connect(f"file:{gp.f_db_npy}?mode=ro", uri=True)
_db_npy.row_factory = factory_idx0

_ct = int(time.time())


def price_nonzero_nth_percentile_realtime(item: int, n: float, c: Optional[sqlite3.Cursor | sqlite3.Connection] = None,
                                          guide_price_data: Optional[bool] = None,
                                          averaged_realtime_data  : Optional[bool] = None, **kwargs) -> int:
    """
    Returns the price of `item` at percentile `n` of non-zero realtime prices
    
    Parameters
    ----------
    item : int or Item
        The item_id of the item for which the data is requested. Can also be passed as an Item.
    n : float
        The percentile of non-zero realtime prices that is to be returned
    c : Optional[sqlite3.Cursor | sqlite3.Connection], optional, None by default
        The database the data is to be extracted from. If not provided, the default database is used
    guide_price_data : bool, optional, True by default
        If True (default), use less volatile guide price data, otherwise use realtime data
    averaged_realtime_data : bool, optional, False by default
        If True, use averaged realtime data instead. Averaged data is less accurate, less prone to outliers and takes
        less time to compute.
        
    Other Parameters
    ----------------
    timespan_hours : int, optional, 52*7*24 by default
        The temporal coverage of the data that is analysed in hours, defaults to approximately 1 year.
        Defaults to approximately 1 year.
    timespan_days : int, optional, 52*7 by default
        The temporal coverage of the data that is analysed in days, defaults to approximately 1 year.
    timespan_weeks : int, optional, 52 by default
        The temporal coverage of the data that is analysed in weeks, defaults to approximately 1 year.
        Defaults to approximately 1 year.
    floor_timespan : int, optional, 1 by default
        Do not floor the timespan (0), or floor it to the nearest day (1) or hour (2).
    t1 : datetime.datetime | int | float, optional, None by default
        The upper bound of the temporal coverage. Can be passed as a datetime.datetime object or as a number (UNIX
        timestamp)

    Returns
    -------
    
    
    Notes
    -----
    Additional keyword args passed mainly involve the timespan of the data that is requested. They are covered in the
    Other Parameters section, they The timespan can be defined as timespan_hours, timespan_days, timespan_weeks. It
    defaults to slightly less than 1 year, which is defined as 52*7 days. The timespan applied depends on the
    floor_timespan parameter, if this is True, which is the default, the timespan will be floored to the nearest hour

    """
    assert kwargs.get("timespan_weeks") or kwargs.get("timespan_days") or kwargs.get("timespan_hours") and \
           isinstance(kwargs.get("t1", _ct), (int, float, datetime.datetime)) and \
           0 <= n <= 1
    
    params = {"n": n}
    src = "src=0" if guide_price_data else \
        "src BETWEEN 1 AND 2 AND price>0" if averaged_realtime_data else "src>=3"
    
    # Default timespan to 52 (weeks in a year) * 7 (days in a week) * 24 (hours in a day) * 3600 (seconds in a day)
    timespan = 31449600
    for unit, m in zip(("timespan_weeks", "timespan_days", "timespan_hours"), (604800, 86400, 3600)):
        _timespan = kwargs.get(unit)
        if _timespan:
            timespan = m * _timespan
            break
    
    t1 = kwargs.get("t1", _ct)
    if not isinstance(t1, int):
        t1 = int(t1.timestamp() if isinstance(t1, datetime.datetime) else t1)
    
    floor_ts = kwargs.get("floor_timespan")
    if floor_ts:
        t1 = t1 - _ct % (3600 if floor_ts == 1 else 86400)
        params.update({"t1": t1, "t0": t1 - timespan})
        timestamp_where = "timestamp BETWEEN :t0 AND :t1"
    
    else:
        params['t0'] = t1 - timespan
        timestamp_where = "timestamp > :t0"
    
    table = f"{(item if isinstance(item, int) else item.item_id):0>5}"
    sql = f"""WITH nonzeros AS (
              SELECT
                price
              FROM {item:0>5}
              WHERE {src} AND {timestamp_where}
            ),
            ranked AS (
              SELECT
                price,
                CUME_DIST() OVER (ORDER BY price) AS cum_dist
              FROM nonzeros
            )
            SELECT
              price
            FROM ranked
            WHERE cum_dist >= {n}
            ORDER BY cum_dist
            LIMIT 1;
            """
    if c is None:
        return _db_timeseries.execute(sql).fetchone()
    else:
        value = c.execute(sql).fetchone()
        try:
            return value[0]
        except TypeError:
            if isinstance(value, int):
                return value
