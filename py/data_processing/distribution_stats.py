"""
Module with the abstract DistributionStats class.
"""

from abc import ABC
from dataclasses import dataclass, field
from typing import Tuple

import numpy as np

from common.classes.database import Database
from item.db_entity import Item

_db: Database


@dataclass(frozen=True, slots=True)
class DistributionStats(ABC):
    """
    Basic distribution stats for a set of numbers
    
    
    """
    item: Item = field(repr=False, compare=True)
    column: str = field(repr=False, compare=True)
    sql: str = field(repr=False, compare=True)
    timestamp: int = field(repr=False, compare=False)
    
    v_min: int | float = field(default=None, init=False)
    v_05: int | float = field(default=None, init=False)
    v_q1: int | float = field(default=None, init=False)
    v_median: int | float = field(default=None, init=False)
    v_q3: int | float = field(default=None, init=False)
    v_95: int | float = field(default=None, init=False)
    v_max: int | float = field(default=None, init=False)
    
    @property
    def window_size(self) -> int:
        """The size of the window in seconds"""
        raise NotImplementedError("The subclass should have a custom window_size")
    
    @property
    def timespan(self) -> Tuple[int, int]:
        """Return a t0 and t1 tuple that cover a full week, anchored to a specific timestamp within the week"""
        t0 = self.timestamp - self.timestamp % self.window_size
        return t0, t0 + self.window_size
    
    @property
    def temporal_shift(self) -> Tuple[int, int]:
        """Return the temporal shift, which is added to t0 to shift the anchored value by this amount of seconds"""
        t0 = self.timestamp - self.timestamp % self.window_size
        return t0, t0 + self.window_size
    
    @property
    def timestamp_label(self) -> str:
        """Returns the timestamp label"""
        raise NotImplementedError("The timestamp_label should be overridden")
    
    @property
    def label(self):
        """The label that describes the class in a concise manner"""
        return f"""{self.item.item_name} (id={self.item.item_id}) {self.column} {self.timestamp_label}"""
    
    def __post_init__(self):
        try:
            values = np.array(_db.execute(self.sql, self.timespan, factory=0).fetchall())
        except NameError:
            raise RuntimeError("Unable to determine value distribution without having an affiliated database. Make sure"
                               "to establish a connection to an existing database via DistributionStatsWeek.connect() "
                               "before generating DistributionStatsWeek objects.")
        
        object.__setattr__(self, "v_min", float(round(min(values), 2)))
        object.__setattr__(self, "v_05", float(round(np.percentile(values, 0.05), 2)))
        object.__setattr__(self, "v_q1", float(round(np.percentile(values, 0.25), 2)))
        object.__setattr__(self, "v_median", float(round(np.percentile(values, 0.5), 2)))
        object.__setattr__(self, "v_q3", float(round(np.percentile(values, 0.75), 2)))
        object.__setattr__(self, "v_95", float(round(np.percentile(values, 0.95), 2)))
        object.__setattr__(self, "v_max", float(round(max(values), 2)))
    
    def __eq__(self, other):
        return self.item_id == other.item_id and \
            self.column == other.column and \
            self.window_size == other.window_size and \
            self.temporal_shift == other.temporal_shift
    
    def __ne__(self, other):
        return self.item_id != other.item_id or \
            self.column != other.column or \
            self.window_size != other.window_size or \
            self.temporal_shift != other.temporal_shift
    
    @staticmethod
    def connect_database(path: str):
        """Establish a connection with a database to draw values from"""
        global _db
        _db = Database(path, read_only=True)
    