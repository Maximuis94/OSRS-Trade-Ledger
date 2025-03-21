from typing import Tuple

from abc import ABC, abstractmethod
from collections import namedtuple

import sqlite3

from dataclasses import dataclass, field
from overrides import override


# Datapoint classes are datapoints of scraped data
_datapoint_kwargs = {
    'init': True,
    'eq': True,
    'order': True,
    'frozen': False,
    'match_args': True
}


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


TimeseriesDatapoint = namedtuple('TimeseriesDatapoint', ['timestamp', 'value'])
