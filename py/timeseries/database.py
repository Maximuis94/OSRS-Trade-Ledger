"""
Implementation of the Timeseries database.


Notes
-----
The table architecture  is the result of rigorous sharding after the time needed to execute a query took too long. As
to prevent this issue from recurring, the database was sharded based on common queries. Whatever data was requested, it
was almost always data for one specific item. As such, each item was given its own table.
"""
import time

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field
from multipledispatch import dispatch
from typing import Dict, List, Literal, NamedTuple, Optional, Tuple

import global_variables.path as gp
from timeseries.types import SrcLike, OrderBy, Orderable
from timeseries.view import TimeseriesView, sql_create_timeseries_extension_view

_tables_per_db = {}


class TimeseriesDatapoint(NamedTuple):
    """
    A single datapoint in the timeseries database. The item_id was omitted on purpose due to

    Attributes
    ----------
    src : int
        Identifier for the data source or instrument.
    timestamp : int
        The timestamp of the datapoint, typically represented as a Unix timestamp.
    price : int
        The price value at the given timestamp.
    volume : Optional[int]
        The volume associated with the datapoint. This field is None for Realtime data (src=3/4)
    """
    src: int
    timestamp: int
    price: int
    volume: Optional[int] = None


def _row_factory(cursor, row) -> TimeseriesDatapoint:
    return TimeseriesDatapoint(*row)



@dataclass(slots=True, frozen=True, match_args=False, eq=True)
class TimeseriesDatabase:
    """
    A sharded timeseries database for managing datapoints across multiple items.

    Attributes
    ----------
    path : str
        The absolute path to the database file.
    tables : Tuple[int, ...]
        A tuple of item_ids corresponding to tables available in the database.

    Methods
    -------
    load_data(item_id: int | Iterable[int], source: Optional[int | Iterable[int]], **kwargs)
        Retrieve datapoints for one or more item_ids, optionally filtering by source and timestamp range.
        
    Example usage;
    db = TimeseriesDatabase()
    t0 = time.perf_counter()
    rows = db.load_data(2)
    """
    
    path: str = field(default=str(gp.f_db_timeseries), init=False, repr=True, compare=True)
    """Path to the database."""
    
    tables: Tuple[int, ...] = field(default=None, init=False, repr=False, compare=False)
    """All item_ids the underlying Database has data on"""
    
    _order_by: str = field(default=" ORDER BY src ASC, timestamp ASC", kw_only=True, repr=False, compare=False)
    """ORDER BY clause to apply by default."""
    
    def __post_init__(self):
        """Post-initialization, used to alter attributes after initialization. After this method they will be frozen."""
        conn = sqlite3.Connection(self.path)
        c = conn.cursor()
        c.row_factory = lambda cur, row: row[0]

        global _tables_per_db
        if _tables_per_db.get(self.path):
            tables = _tables_per_db[self.path]
        else:
            tables = c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
            _tables_per_db[self.path] = tables
        object.__setattr__(self, "tables", tuple([int(i[4:]) for i in tables if i.startswith('item')]))
        
        if not self.order_by.strip().startswith("ORDER BY"):
            msg = f"The ORDER BY clause does not meet the expected format (_order_by={self._order_by})"
            raise RuntimeError(msg)
    
    def load_data(self, item_id: int | Iterable[int], *, source: Optional[int | Iterable[int]] = None, **kwargs) \
            -> Dict[int, Tuple[TimeseriesDatapoint, ...]]:
        """
        Load data from the database for one or more item_ids from one or more sources.
        
        
        Parameters
        ----------
        item_id : int | Iterable[int]
            One or more item_ids to query data for
        source : Optional[int | Iterable[int]], optional, None by default
            One or more sources to query data from. If undefined, include all sources.
        t0 : Optional[int], optional, None by default
            Lower bound timestamp (inclusive)
        t1 : Optional[int], optional, None by default
            Upper bound timestamp (exclusive)
        
        Other Parameters
        ----------------
        order_rows : bool, optional, True by default
            If True, append the ORDER BY clause that was defined during initialization to the query. This clause
            defaults to 'ORDER BY src ASC, timestamp ASC'
        row_factory : Callable, optional, None by default
            Dictates the format in which each row is returned. If passed as True, return as TimeseriesDatapoints.

        Returns
        -------
        Dict[int, Tuple[TimeseriesDatapoint, ...]]
            The queried data is returned as a dict that uses item_id as key and a tuple of TimeseriesDatapoints
            as the entry (i.e. the queried data). Note that data from various sources is merged in the output.

        """
        if not kwargs.get('cursor'):
            conn = self._connect()
            cursor = conn.cursor()
            row_factory = kwargs.get('row_factory')
            if row_factory:
                cursor.row_factory = _row_factory if isinstance(row_factory, bool) and row_factory else row_factory
                
            kwargs['cursor'] = cursor
        return self._load_data(item_id if isinstance(item_id, int) else tuple(item_id),
                               source=source if source is None or isinstance(source, int) else tuple(source), **kwargs)
    
    @dispatch(int)
    def _load_data(self, item_id: int, **kwargs) -> Dict[int, Tuple[TimeseriesDatapoint, ...]]:
        """Load data of one item_id from one source from the database"""
        return {item_id: self._get_rows(item_id, **kwargs)}
    
    @dispatch(tuple)
    def _load_data(self, item_ids: Tuple[int, ...], **kwargs) -> Dict[int, Tuple[TimeseriesDatapoint, ...]]:
        """Load data of one item_id from one source from the database"""
        return {i: self._get_rows(i, **kwargs) for i in item_ids}
        
    @property
    def connection(self) -> sqlite3.Connection:
        """Read-only connection to the database"""
        return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
    
    @property
    def cursor(self) -> sqlite3.Cursor:
        """Cursor from a read-only connection with a TimeseriesDatapoint row factory"""
        conn = self.connection
        cursor = conn.cursor()
        cursor.row_factory = _row_factory
        return cursor
    
    def _table(self, item_id: int) -> str:
        """Name of the table for item `item_id`"""
        return f'"item{item_id:0>5}"'
    
    def _where(self, source: Optional[int | Tuple[int, ...]] = None, t0: Optional[int] = None,
                  t1: Optional[int] = None) -> str:
        """Generate a WHERE clause for the inputs"""
        if all([a is None for a in (source, t0, t1)]):
            return ""
        where = []
        if source is not None:
            where.append("src=?" if isinstance(source, int) else
                         f"src IN ({', '.join(['?' for _ in range(len(source))])})")
        
        if t0 is None and t1 is None:
            ...
        elif t0 is None:
            where.append("timestamp <= ?")
        elif t1 is None:
            where.append("timestamp >= ?")
        else:
            where.append("timestamp BETWEEN ? AND ?")
        
        return " WHERE " + " AND ".join(where)
    
    @property
    def order_by(self) -> str:
        return self._order_by
        
    
    def _get_rows(self, item_id: int, source: Optional[int | Tuple[int, ...]] = None, t0: Optional[int] = None,
                  t1: Optional[int] = None, cursor: Optional[sqlite3.Cursor] = None, order_by: bool = True) -> Tuple[TimeseriesDatapoint, ...]:
        """
        Load the rows from the timeseries database for item with id `item_id`. Only query rows with src as described by
        `source`. Return rows within the imposed timespan `t0`, `t1`.
        
        Parameters
        ----------
        item_id : int
            The item for which to query rows
        source : Optional[int | Tuple[int, ...]], optional, None by default
            The source(s) from which the queried rows should originate. If undefined, include all sources.
        t0 : Optional[int], optional, None by default
            The lower bound timestamp (inclusive). If undefined, there is no lower bound.
        t1 : Optional[int], optional, None by default
            The upper bound timestamp (inclusive). If undefined, there is no upper bound.
        cursor : sqlite3.Cursor, optional, None by default
            If passed, use this cursor to access the data, else establish a new connection and use that
        order_by : bool, optional, True by default
            If True, apply the order by clause described by the _order_by class attribute.

        Returns
        -------
        Tuple[TimeseriesDatapoint, ...]
            A tuple of timeseries datapoints from the table that corresponds to `item_id`, with srcs described by
            `source`, within timespan `t0`, `t1`.

        """
        if cursor is None:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.row_factory = lambda c, r: TimeseriesDatapoint(*r)
        
        sql = f"""SELECT {", ".join(TimeseriesDatapoint.__match_args__)} FROM {self._table(item_id)} {self._where(source, t0, t1)}"""
        if order_by:
            sql += self._order_by
        parameters = tuple([el for el in (*((source,) if isinstance(source, int) or source is None else source), t0, t1) if el is not None])
        
        return tuple(cursor.execute(sql, parameters).fetchall())
    
    def get_rows(self, item: int, src: Optional[SrcLike] = None, t0: Optional[int] = None, t1: Optional[int] = None, **kwargs) -> List[TimeseriesDatapoint]:
        """Executes a SELECT query for getting a specific set of rows, based on input parameters and returns the fetched
        rows as TimeseriesDatapoints.
        
        Parameters
        ----------
        item : int
            item_id of the item for which the data is needed
        src : Optional[SrcLike], optional, None by default
            One or more src values to fetch rows for. If omitted, include all sources.
        t0 : Optional[int], optional, None by default
            The lower bound timestamp (inclusive). If undefined, there is no lower bound.
        t1 : Optional[int], optional, None by default
            The lower bound timestamp (inclusive). If undefined, there is no lower bound.
        
        Other Parameters
        ----------------
        order_by : Orderable, optional, None by default
            One or more OrderBy clause elements, or a pre-defined ORDER BY clause
        
        Returns
        -------
        List[TimeseriesDatapoint]
            The rows that meet the specifications of the SELECT query, as a list of TimeseriesDatapoints.
        """
        return self.cursor.execute(
        
        
        )
    
    def add_timeseries_view(self, ts_view: TimeseriesView, item_ids: Optional[int | Iterable[int]] = None):
        """Extend the tables related to `item_ids` with the """
        con = self.connection
        c = con.cursor()
        c.row_factory = lambda cursor, row: row[0]
        
        if item_ids is None:
            item_ids = [int(table.lstrip("item")) for table in
                            c.execute("""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()
                        if len(table) == 9 and table.startswith('item')]
        else:
            if isinstance(item_ids, int):
                item_ids = [item_ids]
        for item_id in item_ids:
            con.execute(sql_create_timeseries_extension_view(item_id, ts_view))
        con.commit()
        print(f"Added {len(item_ids)} {ts_view.row_timespan} timeseries views to the database at '{self.path}'")
        
        

# Example usage;
# db = TimeseriesDatabase()
# t0 = time.perf_counter()
# rows = db.load_data(2)
# delta_t = time.perf_counter() - t0
# print(f"Time taken: {delta_t:.2f} seconds to load {len(rows[2])} rows")
