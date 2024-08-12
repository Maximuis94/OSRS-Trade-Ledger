"""
This module contains the model of the DataSource class, as well as all specific instances of DataSource used throughout
the project. A DataSource class describes the origin of the data and further distinctions (if any). Each instance should
provide enough information to be able to uniquely identify the specific data source it corresponds to, both via the
unique ID and a unique combination of distinguishing properties that characterizes that data.
Note that all references to data in this class refer to timeseries data only.

A DataSource class instance contains information on an external source data is scraped from, it is used to standardize
available information. Legacy OsBuddy datasources are not included in this module, as they are considered obsolete.

Instances can be imported individually, or as a specifically ordered tuple (SRCS).
"""
import time
from collections import namedtuple
from typing import List

import global_variables.path as gp
from model.database import Database


# Timeseries database with data of all sources
timeseries_database = Database(path=gp.f_db_timeseries, read_only=True)


class DataSource:
    """
    Class definition for a source that data is scraped from. DataSource class instances can also be used to access data
    of the respective source via DataSource.fetch_data().
    
    Attributes
    ----------
    src_id : int
        The id assigned to this DataSource. This is the value that is also used for the src column in the databases.
    name : str
        The (full) name of this DataSource. The name should fully distinguish this source from other potentially similar
        sources and may have some of its attribute values in it.
    abbreviation : str
        The abbreviated name of this DataSource, which is mostly used when this DataSource is referred to console tasks.
    average_scrape_frequency : int
        The average frequency at which this data is scraped in seconds. Although this is typically the gap between
        datapoints, the gap may differ significantly in practice. Gap size may depend on frequency at which data is
        published/scraped, or on how frequently an item is traded.
    is_buy : bool
        Boolean that reflects whether this is purchase or sales data. The value of `is_buy` is determined using the
        final char of `abbreviation`, which indicates whether this source is a purchase, sale or neither.
    db : Database
        The SQLite database that data of this source originates from when making calls to fetch_data(). A read-only
        instance of the timeseries Database is assigned as this attribute.
    
    Methods
    -------
    fetch_data(item_id: int, t0: int, t1: int) -> List[any]
        Method used to fetch data of this source using the specified parameters.
    
    """
    db: Database = timeseries_database
    
    def __init__(self, src_id: int, name: str, abbreviation: str, average_scrape_frequency: int):
        self.src_id: int = src_id
        self.name: str = name
        self.abbreviation: str = abbreviation
        self.average_scrape_frequency: int = average_scrape_frequency
        
        self.is_buy: bool = None if abbreviation[-1] not in ('b', 's') else abbreviation == 'b'
        
    def fetch_data(self, item_id: int, t0: int, t1: int = int(time.time())) -> List[any]:
        """ Fetch data of this source for `item_id` that ranges from `t0` to `t1` (inclusive on both ends) """
        return self.db.execute(f"""SELECT * FROM item{item_id:0>5} WHERE src=? AND timestamp BETWEEN ? AND ?""",
                               (self.src_id, t0, t1)).fetchall()
    
    def __repr__(self):
        return self.src_id


# Official wiki data
src_w = DataSource(
    src_id=0,
    name='wiki',
    abbreviation='w',
    average_scrape_frequency=86400
)


# Runelite buy data averaged per 5 minutes
src_ab = DataSource(
    src_id=1,
    name='avg5m_buy',
    abbreviation='a_b',
    average_scrape_frequency=300
)


# Runelite sell data averaged per 5 minutes
src_as = DataSource(
    src_id=2,
    name='avg5m_sell',
    abbreviation='a_s',
    average_scrape_frequency=300
)


# Runelite realtime buy data
src_rtb = DataSource(
    src_id=3,
    name='realtime_buy',
    abbreviation='r_b',
    average_scrape_frequency=60
)


# Runelite realtime sell data
src_rts = DataSource(
    src_id=4,
    name='realtime_sell',
    abbreviation='r_s',
    average_scrape_frequency=60
)


# Hard-coded tuple that can be used for accessing specific DataSources via index or namedtuple label
_Srcs = namedtuple('Srcs', ('w', 'a_b', 'a_s', 'r_b', 'r_s'))
SRCS = _Srcs(
    w=src_w,
    a_b=src_ab,
    a_s=src_as,
    r_b=src_rtb,
    r_s=src_rts
)

if __name__ == '__main__':
    print(SRCS[0], SRCS.a_b)
    