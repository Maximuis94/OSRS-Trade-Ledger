"""
Module with an implementation to aggregate timeseries data on a per-week basis.
Temporal window sizes are equal to 604800 (i.e. amount of seconds in one week), and also cut off at this specific value.
Or, more specifically, at [t0=timestamp - timestamp % 604800, t1=timestamp - timestamp % 604800 + 604800].
If data is drawn from multiple weeks, the resulting timeframes should be non-overlapping slices of one week per frame.

Aside from a set of functions defined at the top of the module, each subsequent function follows a pattern of requesting
a specific set of values for a specific item within a specific timeframe and converting these values, as well as a label
into a DistributionStats object.

As specific combinations of data may be of interest, whereas other may not be, this module intends to capture the
collection of values that is. As such, the result is a collection of almost identical functions that request specific
pieces of data. The aim is to document the functions properly, such that it is easily recognizable what kind of data
will be returned by a specific function.

The common denominator is that all functions return data in slices of a week as a DistributionStats object, labelled
consistently.

TODO:
 - Add various methods for showing specific distributions
 - Add methods for displaying preprocessed values, much like the raw values
 
"""

import datetime
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Tuple

import global_variables.path as gp
from common.classes.database import Database
from data_processing.distribution_stats import DistributionStats
from data_processing.util_timestamp import dt_utc_ts, floor_ts_week, WINDOW_SIZE
from item.itemdb import itemdb, Item

timeseries = Database(gp.f_db_timeseries)

_window_size: int = 604800
"""The size of the window in which data is aggregated"""


@dataclass(frozen=True, slots=True)
class DistributionPerWeek(DistributionStats):
    """
    Basic distribution stats for a set of numbers that span a week
    """
    
    @property
    def window_size(self) -> int:
        return WINDOW_SIZE.WEEK.value
    
    @property
    def timespan(self) -> Tuple[int, int]:
        """Return a t0 and t1 tuple that cover a full week, anchored to a specific timestamp within the week"""
        t0 = floor_ts_week(self.timestamp)
        return t0, t0 + self.window_size
    
    @property
    def timestamp_label(self) -> str:
        """Return the label, defined as YYYY wWW, which Y being the year and W being the week number"""
        return dt_utc_ts(self.timestamp).strftime("%Y-w%V")


def get_distribution_metrics(values: Sequence[int | float], item: Item, column: str, timestamp: int) -> DistributionStats:
    """Generate a DistributionStats object with a specific ID format that is consistent across the module"""
    return DistributionStats(values, column, )
    

def extract_distribution_avg5m_week(db: Database, item: Item, timestamp: int, column: Literal['price', 'volume']) -> DistributionStats:
    """
    Floor timestamp `timestamp` to the week start, extract all values for column `column` of item `item` within that week,
    compute the distribution (min, .05, .25, .75, .95, max) and return it.
    
    Parameters
    ----------
    db : Database
        Connection to the timeseries database the values are to be extracted from
    item : Item
        The Item to extract the data of
    timestamp : int
        The UNIX timestamp used to determine which week the data is to be extracted from
    column : Literal['price', 'volume']
        The name of the column for which the values are to be extracted.

    Returns
    -------

    """
    values = db.execute(
        f"""SELECT {column} FROM "{item.sqlite_timeseries_table}" WHERE src BETWEEN 1 AND 2 AND timestamp BETWEEN ? AND ?""",
        get_timestamps(timestamp),
        factory=0).fetchall()
    
    return DistributionStats(values, generate_id_label(item, column, timestamp))
    

# extract_stats_timeseries(timeseries, itemdb[2])
_item = itemdb[2]

_t0 = timeseries.execute(f"SELECT MIN(timestamp) FROM {_item.sqlite_timeseries_table}", factory=0).fetchone()
DistributionPerWeek.connect_timeseries_database(gp.f_db_timeseries)
DistributionPerWeek.connect_preprocessed_database(gp.f_db_timeseries)
print(DistributionPerWeek(itemdb[2], "price", """SELECT price FROM item00002 WHERE timestamp BETWEEN ? AND ? AND src BETWEEN 1 AND 2""", int(time.time())))
t0, t1 = DistributionPerWeek(itemdb[2], "price", """SELECT price FROM item00002 WHERE timestamp BETWEEN ? AND ? AND src BETWEEN 1 AND 2""", int(time.time())).timespan

print(dt_utc_ts(t1), datetime.datetime.fromtimestamp(t1-1, datetime.UTC))

# print(extract_distribution_avg5m_week(timeseries, _item, _t0+604800, 'price'))


# for idx, table in enumerate(
#         npy_db.execute("""SELECT tbl_name FROM sqlite_master WHERE type="table";""", factory=0).fetchall()):
#     print(f"Current table: {table}", end='\r')
#     ...
    
