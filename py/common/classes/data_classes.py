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
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
from venv_auto_loader.active_venv import *

__t0__ = time.perf_counter()

_item_table: str = "item"


#######################################################################################################################
# RowTuples
#######################################################################################################################
SqliteSchema = namedtuple('SqliteSchema', ['type', 'name', 'tbl_name', 'rootpage', 'sql'], defaults=(None,))


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


def keep_row_element(key: str):
    return key.split('_')[0] in ('item', 'timestamp', 'price', 'volume', 'buy', 'sell', 'is')


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
