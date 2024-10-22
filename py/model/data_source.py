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
from dataclasses import dataclass, field
from typing import List, Tuple

from import_parent_folder import recursive_import
import global_variables.path as gp
from global_variables.classes import SingletonMeta
from model.database import Database
del recursive_import

# Timeseries database with data of all sources
timeseries_database = Database(path=gp.f_db_timeseries, read_only=True)


@dataclass(init=False)
class DataSource:
    """
    Class definition for a source that data is scraped from. DataSource class instances can also be used to access data
    of the respective source via DataSource.fetch_data().
    
    Attributes
    ----------
    src_id : int
        The id assigned to this DataSource. This is the value that is also used for the src column in the databases.
    source : str
        The name of the source this data originates from. This value is derived from the `name` attribute
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
    min_ts : int
        Lowest UNIX timestamp possible for this source; this value is identical per source, not distinguished by
        buy/sell. For realtime data, this value is set to the minimum timestamp of cannonballs.
    db : Database
        The SQLite database that data of this source originates from when making calls to fetch_data(). A read-only
        instance of the timeseries Database is assigned as this attribute.
    
    Methods
    -------
    fetch_data(item_id: int, t0: int, t1: int) -> List[any]
        Method used to fetch data of this source using the specified parameters.
    
    """
    src_id: int = field(compare=True)
    source: str = field(compare=False)
    name: str = field(compare=False)
    abbreviation: str = field(compare=False)
    average_scrape_frequency: int = field(compare=False)
    min_ts: int = field(compare=False)
    is_buy: bool = field(compare=False)
    _sql_fetch: str = field(compare=False)
    _repr: str = field(compare=False)
    
    db: Database = field(default=timeseries_database, compare=False)
    
    def __init__(self, src_id: int, name: str, abbreviation: str, average_scrape_frequency: int, min_ts: int):
        self.src_id, self._repr = src_id, str(src_id)
        
        self.source = name.split('_')[0]
        self.name = name
        self.abbreviation = abbreviation
        self.average_scrape_frequency = average_scrape_frequency
        self.min_ts = min_ts
        
        self.is_buy = None if abbreviation[-1] not in ('b', 's') else abbreviation == 'b'
        self._sql_fetch = f"""SELECT * FROM item_____ WHERE src={self._repr} AND timestamp BETWEEN ? AND ?"""
    
    def __repr__(self) -> str:
        """ This is the value that represents this object if it is inserted into a string, which is `src_id` as str """
        return self._repr
        
    def fetch_data(self, item_id: int, t0: int, t1: int = int(time.time())) -> List[any]:
        """ Fetch data of this source for `item_id` that ranges from `t0` to `t1` (inclusive on both ends) """
        return self.db.execute(self._sql_fetch.replace('_____', f'{item_id:0>5}'), (t0, t1)).fetchall()


# Official wiki data
src_w = DataSource(
    src_id=0,
    name='wiki',
    abbreviation='w',
    min_ts=1427500800,
    average_scrape_frequency=86400
)


# Runelite buy data averaged per 5 minutes
src_ab = DataSource(
    src_id=1,
    name='avg5m_buy',
    abbreviation='a_b',
    min_ts=1616688900,
    average_scrape_frequency=300
)


# Runelite sell data averaged per 5 minutes
src_as = DataSource(
    src_id=2,
    name='avg5m_sell',
    abbreviation='a_s',
    min_ts=1616688900,
    average_scrape_frequency=300
)


# Runelite realtime buy data
src_rb = DataSource(
    src_id=3,
    name='realtime_buy',
    abbreviation='r_b',
    min_ts=1666203032,
    average_scrape_frequency=60
)


# Runelite realtime sell data
src_rs = DataSource(
    src_id=4,
    name='realtime_sell',
    abbreviation='r_s',
    min_ts=1666203032,
    average_scrape_frequency=60
)


class _Srcs(namedtuple('Srcs', ('w', 'a_b', 'a_s', 'r_b', 'r_s')), metaclass=SingletonMeta):
    """
    Singleton namedtuple class for existing DataSources. This class is used to access specific DataSource instances from
    the namedtuple instance, given specific properties.
    
    Methods starting with 'by' are semi hard-coded calls to _matching_values() that return DataSources with a specific
    value for the corresponding property of that method. Additionally, it can return a specific attribute by setting
    `return_attribute`
    
    """
    def __str__(self):
        """ String representation of this tuple as a tuple of src_ids per element. """
        # return f"({self.w}, {self.a_b}, {self.a_s}, {self.r_b}, {self.r_s})"
        return str(tuple([self.__getattribute__(el).src_id for el in self._fields]))
    
    def _matching_values(self, attribute_name: str, value: any, return_attribute: str = None) -> List[DataSource]:
        """ Return DataSource instances of which the value of attribute `attribute_name` matches `value` as a tuple. """
        if return_attribute is None:
            return [el for el in self if isinstance(el, DataSource) and el.__getattribute__(attribute_name) == value]
        else:
            return [el.__getattribute__(return_attribute) for el in self
                    if isinstance(el, DataSource) and el.__getattribute__(attribute_name) == value]
    
    def by_source(self, source: str, return_attribute: str = None) -> Tuple[DataSource]:
        """ Method for fetching all DataSources with source=`source` """
        return tuple(self._matching_values('source', source, return_attribute))
    
    def by_is_buy(self, is_buy: bool or None, return_attribute: str = None) -> Tuple[DataSource]:
        """ Method for fetching all DataSources with is_buy=`is_buy` """
        return tuple(self._matching_values('is_buy', is_buy, return_attribute))
    
    def by_scrape_frequency(self, average_scrape_frequency: int, return_attribute: str = None) -> Tuple[DataSource]:
        """ Method for fetching all DataSources with average_scrape_frequency=`average_scrape_frequency` """
        return tuple(self._matching_values('average_scrape_frequency', average_scrape_frequency, return_attribute))

    def from_id(self, attribute_name: str, value: any) -> DataSource:
        """ Returns the DataSource that corresponds to `src_id`, provided it exists. If not, raise ValueError. """
        for el in self:
            if el.__getattribute__(attribute_name) == value:
                return el
        raise ValueError(f"Unable to find a logged DataSource for {attribute_name}={value}")


SRC: _Srcs[DataSource] = _Srcs(
    w=src_w,
    a_b=src_ab,
    a_s=src_as,
    r_b=src_rb,
    r_s=src_rs
)

if __name__ == '__main__':
    ...
