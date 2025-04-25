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
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Tuple

import global_variables.path as gp
from common.classes.database import Database
from data_processing.distribution_stats import DistributionStats
from data_processing.util_timestamp import WINDOW_SIZE
from item.itemdb import itemdb, Item

timeseries = Database(gp.f_db_timeseries)

_window_size: int = 604800
"""The size of the window in which data is aggregated"""


@dataclass(frozen=True, slots=True)
class DistributionPerDay(DistributionStats):
    """
    Basic distribution stats for a set of numbers that span a week
    """
    
    @property
    def window_size(self) -> int:
        return WINDOW_SIZE.DAY.value
    
    @property
    def timestamp_label(self) -> str:
        """Str label for the timestamp, formatted as DD-MM-YY"""
        return datetime.datetime.fromtimestamp(self.timestamp).strftime("%d-%m-%y")


def get_distribution_metrics(values: Sequence[int | float], item: Item, column: str, timestamp: int) -> DistributionStats:
    """Generate a DistributionStats object with a specific ID format that is consistent across the module"""
    return DistributionPerDay(values, column, )

# cur_ts = npy_db.execute("""SELECT MIN(timestamp) FROM "item00002" """, factory=0).fetchone()
# cur_ts -= cur_ts % window_size + window_size


# def get_t0(db: Database, table_name: str) -> int:
#     """Get the lowest Avg5m timestamp available in the dataset"""
#     t0 = db.execute(f"""SELECT MIN(timestamp) FROM "{table_name}" """, factory=0).fetchone()
#     return t0 - t0 % _window_size + _window_size
#
#
# def extract_stats_timeseries(timeseries_db: Database, item: Item, t0: Optional[int] = None, **kwargs) -> Dict[str, int | float]:
#     """
#
#
#     Parameters
#     ----------
#     tbl_name : str
#         The name of the table the data is to be extracted from
#
#     Returns
#     -------
#
#     """
#     if t0 is None:
#         t0 = timeseries_db.execute(f"""SELECT MIN(timestamp) FROM "{item.sqlite_timeseries_table}" """, factory=0).fetchone()
#
#     window_size = kwargs.get('window_size', _window_size)
#     # t0 = get_t0(npy_db, item.sqlite_timeseries_table)
#
#     t1 = timeseries_db.execute(f"""SELECT MAX(timestamp) FROM "{item.sqlite_timeseries_table}" """, factory=0).fetchone()
#     timeseries_data = timeseries_db.execute(f"""SELECT * FROM "{item.sqlite_timeseries_table}" WHERE timestamp BETWEEN ? AND ? """, (t0, t1)).fetchall()
#
#     price_npy = np.sort([el[2] for el in timeseries_data])
#     volume_npy = np.array([el[3] for el in timeseries_data])
#
#     print(datetime.datetime.fromtimestamp(t0), datetime.datetime.fromtimestamp(t0+window_size-1))
#     print(np.percentile(price_npy, .05), np.percentile(price_npy, .25), np.percentile(price_npy, .5), np.percentile(price_npy, .75), np.percentile(price_npy, .95), np.average(price_npy))
#     print(np.average(volume_npy))
#
#     stats = DistributionStats(price_npy, t0, t1)
#     print(stats)


# for idx, table in enumerate(
#         npy_db.execute("""SELECT tbl_name FROM sqlite_master WHERE type="table";""", factory=0).fetchall()):
#     print(f"Current table: {table}", end='\r')
#     ...
    
