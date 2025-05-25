"""
Module with the abstract DistributionStats class.
"""
from abc import ABC
from dataclasses import dataclass, field
from typing import Tuple

import numpy as np

from common.classes.database import Database
from data_processing.util import floor_timestamp_window_size, get_min_value, get_max_value, get_percentile_value
from item.db_entity import Item

_db: Database = None
"""Timeseries database"""

_db_preproc: Database = None
"""Preprocessed timeseries database"""


@dataclass(frozen=True, slots=True)
class DistributionStats(ABC):
    """
    Abstract base class for calculating and holding distribution statistics.

    This class computes various statistical measures (min, max, percentiles)
    for a specific data column associated with an `Item`, based on data
    retrieved via an SQL query over a defined time window. Instances are
    immutable after creation.

    Subclasses must implement the `window_size` and `timestamp_label`
    properties.

    Parameters
    ----------
    item : Item
        The item entity for which statistics are calculated. Used for
        identification and potentially within the SQL query. Comparison
        uses this field. Not included in `repr`.
    column : str
        The specific column name within the item's data source for which
        statistics are calculated. Comparison uses this field. Not included
        in `repr`.
    sql : str
        The SQL query string used to fetch the numerical data for calculating
        statistics. The query should expect timestamp bounds as parameters.
        Comparison uses this field. Not included in `repr`.
    timestamp : int
        A Unix timestamp (seconds) used as an anchor point to determine the
        relevant time window (`timespan`) for data retrieval. Comparison
        ignores this field. Not included in `repr`.

    Attributes
    ----------
    item : Item
        See Parameters.
    column : str
        See Parameters.
    sql : str
        See Parameters.
    timestamp : int
        The input timestamp, adjusted (floored) to the beginning of the
        calculated window based on `window_size`.
    v_min : float | None
        The minimum value found in the dataset within the calculated timespan.
        Initialized to None, calculated in `__post_init__`. Rounded to 2 decimal places.
    v_05 : float | None
        The 5th percentile value of the dataset. Initialized to None,
        calculated in `__post_init__`. Rounded to 2 decimal places.
    v_q1 : float | None
        The first quartile (25th percentile) value of the dataset. Initialized
        to None, calculated in `__post_init__`. Rounded to 2 decimal places.
    v_median : float | None
        The median (50th percentile) value of the dataset. Initialized to None,
        calculated in `__post_init__`. Rounded to 2 decimal places.
    v_q3 : float | None
        The third quartile (75th percentile) value of the dataset. Initialized
        to None, calculated in `__post_init__`. Rounded to 2 decimal places.
    v_95 : float | None
        The 95th percentile value of the dataset. Initialized to None,
        calculated in `__post_init__`. Rounded to 2 decimal places.
    v_max : float | None
        The maximum value found in the dataset within the calculated timespan.
        Initialized to None, calculated in `__post_init__`. Rounded to 2 decimal places.

    Raises
    ------
    RuntimeError
        If `__post_init__` is called before a database connection is established
        via the `timeseries_database` property setter.

    Notes
    -----
    - This is an abstract class and cannot be instantiated directly.
    - Subclasses MUST implement the `window_size` and `timestamp_label`
      properties.
    - Statistics (`v_*` attributes) are calculated automatically during
      initialization (`__post_init__`) by executing the `sql` query against
      the globally configured timeseries database (`_db`).
    - The timeseries database connection (`_db`) and preprocessed database
      connection (`_db_preproc`) are module-level variables managed via the
      `timeseries_database` and `preprocessed_database` properties respectively.
      A connection must be set before creating instances.
    - The class is defined as a frozen dataclass using slots for immutability
      and memory efficiency. `object.__setattr__` is used internally in
      `__post_init__` to set calculated attribute values on the frozen instance.
    - Equality comparison (`__eq__`, `__ne__`) is based on `item.item_id`,
      `column`, `window_size`, and `temporal_shift`.
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
        
        object.__setattr__(self, "timestamp", floor_timestamp_window_size(self.timestamp, self.window_size))
        object.__setattr__(self, "v_min", get_min_value(values, 2))
        object.__setattr__(self, "v_05", get_percentile_value(values, 0.05, 2))
        object.__setattr__(self, "v_q1", get_percentile_value(values, 0.25, 2))
        object.__setattr__(self, "v_median", get_percentile_value(values, 0.5, 2))
        object.__setattr__(self, "v_q3", get_percentile_value(values, 0.75, 2))
        object.__setattr__(self, "v_95", get_percentile_value(values, 0.95, 2))
        object.__setattr__(self, "v_max", get_max_value(values, 2))
    
    def __eq__(self, other):
        return self.item.item_id == other.item.item_id and \
            self.column == other.column and \
            self.window_size == other.window_size and \
            self.temporal_shift == other.temporal_shift
    
    def __ne__(self, other):
        return self.item.item_id != other.item.item_id or \
            self.column != other.column or \
            self.window_size != other.window_size or \
            self.temporal_shift != other.temporal_shift
    
    @property
    def timeseries_database(self):
        """Establish a connection with a database to draw values from"""
        global _db
        
        if _db is None:
            raise RuntimeError("Establish a connection to the timeseries database before fetching the connection")
        return _db
    
    @timeseries_database.setter
    def timeseries_database(self, path: str):
        """Establish a connection with a database to draw values from"""
        self.connect_timeseries_database(path)

    @property
    def preprocessed_database(self):
        """Preprocessed database connection."""
        global _db_preproc
        
        if _db_preproc is None:
            raise RuntimeError("Establish a connection to the preprocessed database before fetching the connection")
        
        return _db_preproc
    
    @preprocessed_database.setter
    def preprocessed_database(self, path: str):
        self.connect_preprocessed_database(path)
    
    @staticmethod
    def connect_timeseries_database(path: str):
        """Connect to the timeseries database"""
        global _db
        if __debug__:
            print(f"Timeseries database was set to {path}")
        _db = Database(path, read_only=True)
    
    @staticmethod
    def connect_preprocessed_database(path: str):
        """Connect to the preprocessed database"""
        global _db_preproc
        if __debug__:
            print(f"Preprocessed database was set to {path}")
        _db_preproc = Database(path, read_only=True)

    