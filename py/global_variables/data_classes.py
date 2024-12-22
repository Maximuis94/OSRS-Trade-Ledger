"""
Module with dataclass definitions used throughout the project


Data classes
Data Classes can be used to dynamically generate classes that represent a data element (e.g. sql table row), the full
extent as to how the class is defined, depends on the arguments passed to the dataclasses decorator.
Data classes can be used to quickly generate a class designed to carry a specific amount of values, while automatically
generating certain methods, e.g. for fetching attribute names, string representation or annotations.


NamedTuple
A namedtuple is a basic dataclass that works like a tuple, but its elements can be accessed via a label, e.g.
<NamedTuple>.<Label>.
<NamedTuple>.<Label>.
In terms of runtime it is slightly slower than a tuple, but faster than a dict, which makes it a compromise between
readability and performance.

namedtuple instances can be converted to a dict and they can still be used as if they were a tuple.
It can also be converted to a dict with relative ease and it can still be used as if it were a tuple.

In summary, namedtuples appear to be a better alternative to very simple model classes that do not have any methods.
See the class representation below on how to use some if its features.


References
----------
Data Classes:
    https://docs.python.org/3.10/library/dataclasses.html#module-dataclasses

collections.namedtuple:
    https://docs.python.org/3.10/library/collections.html#collections.namedtuple
"""
import sqlite3
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
from overrides import override
from venv_auto_loader.active_venv import *

__t0__ = time.perf_counter()

_item_table: str = "item"


#######################################################################################################################
# RowTuples
#######################################################################################################################


@dataclass(order=False)
class Timeseries:
    """
    Template class for a sqlite db row.
    Attributes are designed to be immutable, as if they are loaded from a read-only database.

    """
    x_labels: tuple
    x: tuple
    y_labels: tuple
    y: tuple
    n: int
    
    @abstractmethod
    def __init__(self):
        ...
    
    def __repr__(self) -> str:
        """ Print this datapoint as; DatapointName (labelx1=x1, ..., labelxn=xn | labely1=y1, labelyn=yn)"""
        return f"""{self.__str__()} {str(tuple([f'{k}={v}' for k, v in zip(self.x_labels, self.x)]))[:-1]} |
                {str(tuple([f'{k}={v}' for k, v in zip(self.y_labels, self.y)]))[1:]}"""
    
    def __eq__(self, other):
        try:
            return self.n == other.n and self.x == other.x and self.y == other.y
        except AttributeError:
            return False


NpyAvg5mTuple = namedtuple('NpyAvg5mTuple', ['item_id', 'timestamp', 'buy_price', 'buy_volume', 'sell_price',
                                             'sell_volume'])
NpyRealtimeTuple = namedtuple('NpyRealtimeTuple', ['item_id', 'timestamp', 'is_buy', 'price'])
NpyWikiTuple = namedtuple('NpyWikiTuple', ['item_id', 'timestamp', 'price', 'volume'])

IndexTuple = namedtuple('IndexTuple', ['name', 'columns'])

ExeLogEntry = namedtuple('ExeLogEntry', ['transaction_id', 'timestamp', 'price', 'balance', 'profit', 'value',
                                         'n_bought', 'n_purchases', 'n_sold', 'n_sales'])


@dataclass(order=True, match_args=True, slots=True)
class Transaction:
    """
    Basic representation of a Transaction.
    
    """
    transaction_id: int = field(compare=True)
    item_id: int = field(compare=False)
    timestamp: int = field(compare=False)
    is_buy: bool = field(compare=False)
    quantity: int = field(compare=False)
    price: int = field(compare=False)
    status: int = field(compare=False)
    tag: str = field(compare=False)
    update_ts: int = field(compare=False)
    
    # Post-transactional-values?
    average_buy: int = field(default=0, compare=False)
    balance: int = field(default=0, compare=False)
    profit: int = field(default=0, compare=False)
    value: int = field(default=0, compare=False)
    n_bought: int = field(default=0, compare=False)
    n_purchases: int = field(default=0, compare=False)
    n_sold: int = field(default=0, compare=False)
    n_sales: int = field(default=0, compare=False)
    tax: int = field(default=0, compare=False)
    
    @staticmethod
    def row_factory(c: sqlite3.Cursor, row: sqlite3.Row):
        """Row factory that can be set to an SQLite Cursor"""
        return Transaction(*row)
    
    def __str__(self):
        return f"Transaction({', '.join([f'{k}={self.__getattribute__(k)}' for k in self.__match_args__])})"
    
    
# Datapoint classes are datapoints of scraped data
_datapoint_kwargs = {
    'init': True,
    'eq': True,
    'order': True,
    'frozen': False,
    'match_args': True
}

_ytype = int or float

"""
A timeseries datapoint. A pair of values, of which the first value is a unix timestamp and the other value is the
corresponding y-value.
Datapoints are immutable and are typically used in plots, where the timestamp represents an x-coordinate, while the
other value represents the y-coordinate.
"""
TimeseriesDatapoint = namedtuple('TimeseriesDatapoint', ['timestamp', 'value'])


@dataclass(**_datapoint_kwargs)
class PlotStats:
    """
    Statistics for a plot
    """
    t0: int
    t1: int
    delta_t: int
    y_min: _ytype
    y_max: _ytype
    y_avg: float
    y_std: float
    y_distribution: Tuple[_ytype, _ytype, _ytype, _ytype, _ytype]
    n: int
    
    @staticmethod
    def get(x: Sequence, y: list):
        y.sort()
        n = len(y)
        
        return PlotStats(
            t0=x[0],
            t1=x[-1],
            delta_t=x[-1] - x[0],
            y_min=min(y),
            y_max=max(y),
            y_avg=sum(y) / len(y),
            y_std=float(np.std(y)),
            y_distribution=(y[int(.1 * n)], y[int(.25 * n)], y[int(.5 * n)], y[int(.75 * n)], y[int(.9 * n)]),
            n=n
        )


# Transferred batches are saved as lists of Rows.
Row = namedtuple('Row', ['item_id', 'src', 'timestamp', 'price', 'volume'])


@dataclass(**_datapoint_kwargs)
class TimeseriesRow:
    """
    Class representation of a timeseries entry as found in the Database.
    """
    item_id: int = field(compare=True)
    src: int = field(compare=True)
    timestamp: int = field(compare=True)
    price: int = field(compare=False)
    volume: int = field(compare=False)
    
    def tuple(self) -> Tuple[int, int, int, int, int]:
        """ Return this row as a tuple (src, timestamp, price, volume) for this row """
        return Row(self.item_id, self.src, self.timestamp, self.price, self.volume)


class LegacyDatapoint(ABC):
    """
    LegacyDatapoints represent elements of each table before the database was restructured. They are left here as class
    as they still provide information about the data sources, and they may be needed if backward compatibility is
    required, e.g. for transferring data from the raspberry pi
    
    
    """
    
    @abstractmethod
    def convert_datapoint(self) -> TimeseriesRow:
        """ Convert the datapoint to a TimeseriesRow """
        ...


@dataclass(**_datapoint_kwargs)
class Avg5mDatapoint(LegacyDatapoint):
    """
    Class representation of an Avg5m Datapoint.
    Mostly obsolete since the database was restructured, but left in here as reference.
    """
    item_id: int = field(compare=True)
    timestamp: int = field(compare=True)
    buy_price: int = field(compare=False)
    buy_volume: int = field(compare=False)
    sell_price: int = field(compare=False)
    sell_volume: int = field(compare=False)
    
    @override(check_signature=False)
    def convert_datapoint(self, buy: bool) -> TimeseriesRow:
        """ Return this Avg5mDatapoint as a TimeseriesRow """
        return TimeseriesRow(self.item_id, 1, self.timestamp, self.buy_price, self.buy_volume) if buy else \
            TimeseriesRow(self.item_id, 2, self.timestamp, self.sell_price, self.sell_volume)
        

@dataclass(**_datapoint_kwargs)
class RealtimeDatapoint(LegacyDatapoint):
    """
    Class representation of a Realtime Datapoint.
    Mostly obsolete since the database was restructured, but left in here as reference.
    """
    item_id: int = field(compare=True)
    timestamp: int = field(compare=True)
    is_buy: bool = field(compare=True)
    price: int = field(compare=False)

    @override
    def convert_datapoint(self) -> TimeseriesRow:
        """ Return this RealtimeDatapoint as a standard TimeseriesRow """
        return TimeseriesRow(self.item_id, 4-int(self.is_buy), self.timestamp, self.price, 0)


@dataclass(**_datapoint_kwargs)
class WikiDatapoint(LegacyDatapoint):
    """
    Class representation of a wiki datapoint
    """
    item_id: int = field(compare=True)
    timestamp: int = field(compare=True)
    price: int = field(compare=False)
    volume: int = field(compare=False)

    @override
    def convert_datapoint(self) -> TimeseriesRow:
        """ Return this WikiDatapoint as a standard TimeseriesRow """
        return TimeseriesRow(self.item_id, 0, self.timestamp, self.price, self.volume)


def keep_row_element(key: str):
    return key.split('_')[0] in ('item', 'timestamp', 'price', 'volume', 'buy', 'sell', 'is')


@dataclass(**_datapoint_kwargs)
class NpyDatapoint:
    """
    A representation for a single datapoint within a NpyArray
    """
    item_id: int = field(compare=True)
    timestamp: int = field(compare=True)
    minute: int = field(compare=False)
    hour: int = field(compare=False)
    day: int = field(compare=False)
    month: int = field(compare=False)
    year: int = field(compare=False)
    day_of_week: int = field(compare=False)
    hour_id: int = field(compare=False)
    day_id: int = field(compare=False)
    week_id: int = field(compare=False)
    wiki_ts: int = field(compare=False)
    wiki_price: int = field(compare=False)
    wiki_volume: int = field(compare=False)
    wiki_value: int = field(compare=False)
    wiki_volume_5m: int = field(compare=False)
    
    buy_price: int = field(compare=False)
    buy_volume: int = field(compare=False)
    buy_value: int = field(compare=False)
    
    sell_price: int = field(compare=False)
    sell_volume: int = field(compare=False)
    sell_value: int = field(compare=False)
    
    avg5m_price: int = field(compare=False)
    avg5m_volume: int = field(compare=False)
    avg5m_value: int = field(compare=False)
    avg5m_margin: int = field(compare=False)
    
    gap_bs: float = field(compare=False)
    gap_wb: float = field(compare=False)
    gap_ws: float = field(compare=False)
    
    rt_avg: int = field(compare=False)
    rt_min: int = field(compare=False)
    rt_max: int = field(compare=False)
    n_rt: int = field(compare=False)
    realtime_margin: int = field(compare=False)
    tax: int = field(compare=False)
    
    est_vol_per_char: int = field(compare=False)
    volume_coefficient: float = field(compare=False)


@dataclass(order=True, match_args=True)
class Item:
    """
    Representation of an OSRS Item. An Item has various types of variables;
    - Static: Properties that define the item ingame that are rarely/never updated.
    - Augmentations: Can be both manually or automatically defined. Update frequency may vary, although generally they
        are assigned and updated infrequently.
    - Scraped variables: Derived from scraped database. Restricted to numerical values like prices / volumes.

    Item attributes are available for all items and are therefore restricted to particular attributes.
    """
    id: int = field(compare=True)
    item_id: int = field(compare=False)
    item_name: str = field(compare=False)
    members: bool = field(compare=False)
    alch_value: int = field(compare=False)
    buy_limit: int = field(compare=False)
    stackable: bool = field(compare=False)
    release_date: int = field(compare=False)
    equipable: bool = field(compare=False)
    weight: float = field(compare=False)
    update_ts: int = field(compare=False)
    augment_data: int = field(default=0, compare=False)
    remap_to: int = field(default=0, compare=False)
    remap_price: float = field(default=0, compare=False)
    remap_quantity: float = field(default=0, compare=False)
    target_buy: int = field(default=0, compare=False)
    target_sell: int = field(default=0, compare=False)
    item_group: str = field(default='', compare=False)
    count_item: bool = field(default=1, compare=False)
    
    # Live trade data -- Not from local db item table
    current_ge: int = field(default=0, compare=False)
    current_buy: int = field(default=0, compare=False)
    current_sell: int = field(default=0, compare=False)
    current_avg: int = field(default=0, compare=False)
    avg_volume_day: int = field(default=0, compare=False)
    current_tax: int = field(default=0, compare=False)
    margin: int = field(default=0, compare=False)
    
    n_wiki: int = field(default=-1, compare=False)
    n_avg5m_b: int = field(default=-1, compare=False)
    n_avg5m_s: int = field(default=-1, compare=False)
    n_rt_b: int = field(default=-1, compare=False)
    n_rt_s: int = field(default=-1, compare=False)
    
    @staticmethod
    def sqlite_columns() -> Tuple[str, ...]:
        """ Return the columns of the Item as stored in the sqlite database """
        return Item.__match_args__[:19]
    
    @staticmethod
    def sql_select() -> str:
        """SQL select statement for fetching all item attributes for all items from an SQLite db"""
        return f"""SELECT ({", ".join(Item.sqlite_columns())}) FROM "{_item_table}" """
    
    @staticmethod
    def row_factory(c: sqlite3.Cursor, row: sqlite3.Row):
        """row factory method that can be set to an SQLite cursor"""
        return Item(*row)
    
    def cast(self, var: str) -> datetime | int | float | str | bool:
        """Convert attribute `var` to the correct type"""
        if var in ["release_date", "update_ts"]:
            return datetime.fromtimestamp(self.__getattribute__(var))
        return self.__annotations__.get(var)(self.__getattribute__(var))
    
    def __int__(self):
        return self.item_id
    
    def __str__(self):
        return self.item_name
    
    def __repr__(self):
        return "\n\t* ".join([f"OSRS Item {self.item_name} (id={self.item_id})"]+\
                             [f"{el.capitalize()}: {self.cast(el)}"
                              for el in self.__match_args__[2:11]])
    
    def __eq__(self, other):
        return self.item_id == other.item_id
    
    def __ne__(self, other):
        return self.item_id != other.item_id
    

@dataclass(eq=False, match_args=True)
class NpyArray:
    """
    NpyArray data class for an Item that is composed of both Item data and array data from the npy database.
    """
    item_id: int
    item_name: str
    members: bool
    alch_value: int
    buy_limit: int
    stackable: bool
    release_date: int
    equipable: bool
    weight: float
    augment_data: int
    remap_to: int
    remap_price: float
    remap_quantity: float
    target_buy: int
    target_sell: int
    item_group: str
    update_ts: int
    
    timestamp: np.ndarray
    day: np.ndarray
    month: np.ndarray
    year: np.ndarray
    hour_id: np.ndarray
    hour: np.ndarray
    day_id: np.ndarray
    day_of_week: np.ndarray
    week_id: np.ndarray
    wiki_ts: np.ndarray
    wiki_price: np.ndarray
    wiki_volume: np.ndarray
    buy_price: np.ndarray
    buy_volume: np.ndarray
    sell_price: np.ndarray
    sell_volume: np.ndarray
    rt_avg: np.ndarray
    rt_min: np.ndarray
    rt_max: np.ndarray
    n_rt: np.ndarray
    avg5m_margin: np.ndarray
    realtime_margin: np.ndarray
    tax: np.ndarray
