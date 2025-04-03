"""
Module with an implementation to aggregate NPY data on a per-week basis.
"""
import numpy as np
import sqlite3

from typing import Dict

import os

from data_processing.distribution_stats import DistributionStats
from global_variables.variables import wiki_types
from item.itemdb import itemdb, Item
from common.classes.database import Database
import global_variables.path as gp
import datetime

timeseries = Database(gp.f_db_timeseries)
npy_db = Database(gp.f_db_npy)

window_size: int = 604800
"""The size of the window in which data is aggregated"""

cur_ts = npy_db.execute("""SELECT MIN(timestamp) FROM "item00002" """, factory=0).fetchone()
cur_ts -= cur_ts % window_size + window_size


def get_t0(db: Database, table_name: str) -> int:
    """Get the lowest Avg5m timestamp available in the dataset"""
    t0 = db.execute(f"""SELECT MIN(timestamp) FROM "{table_name}" """, factory=0).fetchone()
    return t0 - t0 % window_size + window_size


def extract_stats_timeseries(timeseries_db: Database, npy_db: Database, item: Item, **kwargs) -> Dict[str, int | float]:
    """
    
    
    Parameters
    ----------
    tbl_name : str
        The name of the table the data is to be extracted from

    Returns
    -------

    """
    t0 = get_t0(npy_db, item.sqlite_timeseries_table)
    
    timeseries_data = timeseries_db.execute(f"""SELECT * FROM "{item.sqlite_timeseries_table}" WHERE timestamp BETWEEN ? AND ? """, (t0-86400, t0+window_size)).fetchall()
    
    price_npy = np.sort([el[2] for el in timeseries_data])
    volume_npy = np.array([el[3] for el in timeseries_data])
    
    print(datetime.datetime.fromtimestamp(t0), datetime.datetime.fromtimestamp(t0+window_size-1))
    print(np.percentile(price_npy, .05), np.percentile(price_npy, .25), np.percentile(price_npy, .5), np.percentile(price_npy, .75), np.percentile(price_npy, .95), np.average(price_npy))
    print(np.average(volume_npy))
    
    stats = DistributionStats(price_npy)
    print(stats)

extract_stats_timeseries(timeseries, npy_db, itemdb[2])


for idx, table in enumerate(
        npy_db.execute("""SELECT tbl_name FROM sqlite_master WHERE type="table";""", factory=0).fetchall()):
    print(f"Current table: {table}", end='\r')
    ...
    
