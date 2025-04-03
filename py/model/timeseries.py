"""
Module for modelling timeseries data

Timeseries datapoints consists of an item_id, source (src), timestamp (ts), price, volume.
The item_id refers to an OSRS item_id
The source indicates the type of data. It is encoded with values ranging from 0-4; 0=wiki, avg5m (1=buy, 2=sell),
realtime (3=buy, 4=sell). It is typically abreviated as 'src'
The timestamp, price and volume are the UNIX timestamp, price and volume of the datapoint.

Realtime datapoints (src=3, 4) do not have a volume, which is set to 0 in data structures.
While src is a discrete value, the other values are continuous and non-negative. 0 is typically used for missing values,
although this should only occur in the volume column





In this module, various timeseries datapoints are modelled.
Timeseries data is characterized by its x-axis, which is chronologically ascending scale. Throughout this project, the
x-axis of timeseries data consists of unix timestamps.
The y-axis can assume all kinds of values that are captured over a period of time. The basic timeseries attributes
modelled here are is_buy, item prices and trading volumes.
Timeseries data is typically fetched as a sequence of datapoints, methods for fetching these are defined in the
controller.timeseries module.

Throughout this module abbreviations of attribute names are used.
In a nutshell; timestamp -> ts, buy -> b, sell -> s, price -> p, volume -> v

Or more specifically;
ts: timestamp -> unix timestamp (int, float; avg5m, realtime, wiki) [1424995200, time.time()]
b: is_buy -> flag indicating buy/sell (bool; realtime) [False, True]
p: price -> item price (int; realtime, wiki) [1, 2147483647]
v: volume -> trading volume (int; wiki) [0, 2147483647]
bp: buy_price -> item buy price (int; avg5m) [0, 2147483647]
bv: buy_volume -> item buy volume (int; avg5m) [0, 2147483647]
sp: sell_price -> item sell price (int; avg5m) [0, 2147483647]
sv: sell_volume -> item sell volume (int; avg5m) [0, 2147483647]

Note that item_id has been excluded as an attribute, as timeseries data is always queried and grouped by item_id in
practice.

Missing data is typically set to 0. For price data, this implies the data could not be provided, for volume data it can
also imply the trading volume was equal to 0. What a 0 value entails per attribute;

volume (wiki) -> daily trade volume data was added later, which means it is not present in early datapoints, the volume
    data could not be retrieved for another reason (If this is the case, it usually applies to all datapoints on that
    timestamp). It can also simply mean 0 items were traded. However, this is much less likely than it is for avg5m
    datapoints, as wiki volume data refers to daily trading volume. It does happen, though.
    
buy/sell_price/volume (avg5m) if a price attribute of an avg5m datapoint is 0, it implies the corresponding volume is 0
    as well. This simply means there haven't been any trades registered within the 5 minute time interval that datapoint
    captures. If both buy- and sell- data is missing, there is no entry for that timestamp.
"""
import sqlite3
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Iterable
from typing import List, Dict, Callable, Tuple

import pandas as pd
from multipledispatch import dispatch

from venv_auto_loader.active_venv import *
import global_variables.configurations as cfg
import global_variables.osrs as go
import global_variables.path as gp
import global_variables.variables as var
import util.str_formats as fmt
from common.item import create_item
from global_variables.datapoint import TimeseriesRow
from global_variables.datapoint import NpyDatapoint
from common.classes.database import Database
__t0__ = time.perf_counter()

from global_variables.values import empty_tuple

queried_item_id = None
queried_timestamp = None
item = create_item(2)
np_ar_start = [queried_item_id, queried_timestamp]
ts_start = int(time.time())



def sql_timeseries_insert(item_id: int, src: int = None, replace: bool = True) -> str:
    """ Return an INSERT / INSERT OR REPLACE statement for table item`item_id`  """
    return f"""INSERT {'OR REPLACE ' if replace else ''}INTO "item{item_id:0>5}"
            (src, timestamp, price, volume) VALUES ({'?' if src is None else src}, ?, ?, ?)"""


def sql_create_timeseries_item_table(item_id: int, check_exists: bool = True) -> str:
    """ Returns an executable SQL statement for creating a timeseries table for a specific item """
    return f"""CREATE TABLE {"IF NOT EXISTS " if check_exists else ""}"item{item_id:0>5}"(
            "src" INTEGER NOT NULL CHECK (src BETWEEN 0 AND 4),
            "timestamp" INTEGER NOT NULL,
            "price" INTEGER NOT NULL DEFAULT 0 CHECK (price>=0),
            "volume" INTEGER NOT NULL DEFAULT 0 CHECK (volume>=0),
            PRIMARY KEY(src, timestamp) )"""


"""
The methods below can be used to for execute calls to an sqlite db via *get_timeseries_select(...). The bottom method
has a more detailed explanation
"""


@dispatch(int, tuple or None)
def sql_timeseries_select(item_id: int, columns: Tuple[str] = None) -> Tuple[str, tuple]:
    """ Get a timeseries SELECT statement and the tuple needed to execute it """
    return f"""SELECT {str(columns) if isinstance(columns, tuple) else '*'} FROM 'item{item_id:0>5}'""", empty_tuple


@dispatch(int, int, tuple or None)
def sql_timeseries_select(item_id: int, src: int, columns: Tuple[str] = None) -> Tuple[str, tuple]:
    """ Get a timeseries SELECT statement and the tuple needed to execute it """
    return f"""SELECT {str(columns) if isinstance(columns, tuple) else '*'} FROM 'item{item_id:0>5}' WHERE src=?""", \
           (src,)


@dispatch(int, int, int or float, tuple or None)
def sql_timeseries_select(item_id: int, src: int, t0: int or float, columns: tuple = None) -> Tuple[str, tuple]:
    """ Get a timeseries SELECT statement and the tuple needed to execute it """
    return f"""SELECT {str(columns) if isinstance(columns, tuple) else '*'} FROM 'item{item_id:0>5}'
                WHERE src=? AND timestamp >= ?""", (src, t0)


@dispatch(int, int, int or float, int or float, tuple or None)
def sql_timeseries_select(item_id: int, src: int, t0: int or float, t1: int or float, columns: Tuple[str] = None) -> \
        Tuple[str, tuple]:
    """
    Fetch timeseries select statement + corresponding tuple, specifying an `item_id`, `src`, `t0` and `t1`. It is also
    possible to specify the following combinations of input args;
    item_id OR item_id, src OR item_id, src, t0 OR item_id, src, t0, t1
    
    Parameters
    ----------
    item_id : int
        item_id for which timeseries data is needed
    src : int, optional
        Source of the data that should be fetched
    t0 : int, optional
        Lower bound timestamp. If given, only return rows with a higher timestamp than `t0`
    t1 : int, optional
        Upper bound timestamp. If given, only return rows with a lower timestamp than `t1`
    columns: str or tuple, optional, None by default
        Columns to fetch from the timeseries database. If unspecified or not passed as tuple, '*' is set as columns

    Returns
    -------
    Tuple[str, tuple]
        A tuple with an executable SELECT statement, as well as the parameters needed to execute the statement. Can be
        used as Sqlite3.Connection.execute(*get_timeseries_select(...))
    """
    return f"""SELECT {str(columns) if isinstance(columns, tuple) else '*'} FROM 'item{item_id:0>5}' WHERE src=? AND timestamp BETWEEN ? AND ?""", (src, t0, t1)


class TimeseriesDB(Database):
    """
    Model class for timeseries database. Note that the triple underscores of sql statements should be replaced by an
    item_id. Item_ids in table names are formatted as 5-digit integers, preceded by 0s if the item_id is less than 5
    digits
    
    """
    create_table: str = f"""CREATE TABLE "item___"(
        "src" INTEGER NOT NULL CHECK (src BETWEEN 0 AND 4),
        "timestamp" INTEGER NOT NULL,
        "price" INTEGER NOT NULL DEFAULT 0 CHECK (price>=0),
        "volume" INTEGER NOT NULL DEFAULT 0 CHECK (volume>=0),
        PRIMARY KEY(src, timestamp) )"""
    
    insert: str = """INSERT INTO ___(src, timestamp, price, volume) VALUES (?, ?, ?, ?) """
    insert_replace: str = """INSERT OR REPLACE INTO ___(src, timestamp, price, volume) VALUES (?, ?, ?, ?) """
    item_ids = tuple(frozenset(go.item_ids).difference(go.timeseries_skip_ids))
    
    TimeseriesDatapoint = TimeseriesRow
    TimeseriesDatapoint_noid = namedtuple('TimeseriesDatapoint_noid', ['src', 'timestamp', 'price', 'volume'])
    NpyDatapoint = namedtuple('NpyDatapoint', NpyDatapoint.__match_args__)
    
    def __init__(self, db_file: str = gp.f_db_timeseries, autocreate_tables: bool = True):
        super().__init__(db_file, parse_tables=False, row_factory=lambda c, row: self.TimeseriesDatapoint_noid(*row))
        self.path = db_file
        self.create_missing_tables = autocreate_tables
        
        import sqlite.row_factories as fac
        
        self.ro_con, self.cursors = sqlite3.connect(database=f'file:{self.path}?mode=ro', uri=True), {}
        for key, factory in zip((0, tuple, dict, -1, 1),
                                (fac.factory_single_value, fac.factory_tuple, fac.factory_dict,
                                 self.factory_datapoint, self.factory_datapoint_item_id)):
            cursor = self.ro_con.cursor()
            cursor.row_factory = factory
            self.cursors[key] = cursor
            
    def insert_table_name(self, sql: str, item_id: int) -> str:
        """ Return the sql statement with a table name corresponding to `item_id` instead of 3 underscores """
        return sql.replace("___", f'"item{item_id:0>5}"')
    
    def get_sql_create(self, item_id: int) -> str:
        return self.insert_table_name(self.create_table, item_id)
    
    def auto_create_table(self, e: sqlite3.Error, item_id: int, con: sqlite3.Connection) -> bool:
        """ Create a table for `item_id` through database connection `con`, but only if `item_id` is a valid item_id """
        if "no such table" in str(e) and item_id in self.item_ids:
            con.execute(sql_create_timeseries_item_table(item_id, False))
            return True
        return False
    
    def insert_row(self, item_id: int, src: int, timestamp: int, price: int, volume: int,
                   con: sqlite3.Connection = None, commit_data: bool = False, replace: bool = True) -> bool:
        """
        Insert a single row into the table associated with `item_id`
        
        Parameters
        ----------
        item_id : int
            The item_id of the row that is to be inserted
        src : int
            The source of the data that is to be inserted (see global_variables.osrs.timeseries_srcs
        timestamp : int
            The unix timestamp of the row that is to be inserted
        price : int
            The price of the row that is to be inserted
        volume : int
            The volume of the row that is to be inserted. Is set to 0 for realtime sources (i.e. src=3/4)
        con : sqlite3.Connection, optional, None by default
            Connection to use to submit the row. If not passed, create one and commit upon completion
        commit_data : bool, optional, False by default
            If True, commit data upon completion. If `con` is None, `commit_data` is set to True.
        replace : bool, optional, True by default
            If True, execute INSERT OR REPLACE instead of INSERT, allowing for rows to be overwritten.

        Returns
        -------
        bool
            True upon successfully submitting the row

        """
        if con is None:
            con = sqlite3.connect(self.path)
            commit_data = True
            
        if src >= 3:
            volume = 0
        
        try:
            global queried_item_id
            queried_item_id = item_id
            con.execute(sql_timeseries_insert(item_id, replace), (src, timestamp, price, volume))
        except sqlite3.OperationalError as e:
            if self.create_missing_tables and self.auto_create_table(e, item_id, con):
                con.execute(sql_timeseries_insert(item_id, replace), (src, timestamp, price, volume))
                
        if commit_data:
            con.commit()
        return True
    
    def insert_rows(self, item_id: int, rows: Iterable, con: sqlite3.Connection = None, commit_data: bool = True,
                    replace: bool = True, non_zero_prices: bool = True):
        """
        Insert `rows` into the timeseries database to the corresponding table of `item_id`
        
        Parameters
        ----------
        item_id : int
            The item_id of the item
        rows : Iterable
            An Iterable consisting of one or multiple rows that are to be inserted
        con : sqlite3.Connection, optional, None by default
            db connection to use for inserting the data. If undefined, connect to the path associated with this db
        commit_data : bool, optional, True by default
            If True, commit data upon completion
        replace : bool, optional, True by default
            If True, execute INSERT OR REPLACE instead of INSERT, allowing for rows to be overwritten
        non_zero_prices : bool, optional, True by default
            If True, only submit rows that have a price that is not 0

        Returns
        -------
        bool
            True upon successfully submitting the rows
        """
        if con is None:
            con = sqlite3.connect(self.path)
            commit_data = True
        
        # Set volume to 0 if it is a realtime row; exclude rows with price=0 if non_zero_prices
        rows = [tuple(row if row[1] < 3 else row[:3]+[0]) for row in rows
                if not non_zero_prices or non_zero_prices and row[2] > 0]
        try:
            global queried_item_id
            queried_item_id = item_id
            con.executemany(sql_timeseries_insert(item_id, replace), rows)
        except sqlite3.OperationalError as e:
            if self.create_missing_tables and self.auto_create_table(e, item_id, con):
                con.executemany(sql_timeseries_insert(item_id, replace), rows)
    
        if commit_data:
            con.commit()
        return True
    
    def fetch_rows(self, item_id: int, src: int, t0: int = None, t1: int = None,
                   return_type: Callable = tuple, include_item_id: bool = None):
        """
        Fetch rows from the Timeseries database from source `src` and item `item_id`. Additionally, the time coverage
        can be narrowed down with `t0` and `t1` as timestamp lower- and upper- bound, respectively.
        Furthermore, the dict factory can be specified via `return_type` or `include_item_id`; the latter returns a
        labelled TimeseriesDatapoint tuple.
        
        Parameters
        ----------
        item_id : int
            Item_id of the requested data
        src : int
            Source of the data that is needed
        t0 : int, optional, None by default
            If passed, use this UNIX timestamp as a lower bound
        t1 : int, optional, None by default
            If passed, use this UNIX timestamp as an upper bound
        return_type : Callable, optional, tuple by default
            Return type for fetched datapoints. The default value, a tuple, is the fastest in terms of runtime.
        include_item_id : bool, optional, None by default
            If passed, rows are returned as TimeseriesDatapoint tuples and the truth value dictates whether to include
            the `item_id` or not

        Returns
        -------
        List[Any]
            List of the fetched rows that meets the requirements specified

        """
        if t0 is None and t1 is None:
            exe = f"""SELECT * FROM "item{item_id:0>5}" WHERE src=? """, (src,)
        elif t0 is None:
            exe = f"""SELECT * FROM "item{item_id:0>5}" WHERE src=? AND timestamp <= ?""", (src, t1)
        elif t1 is None:
            exe = f"""SELECT * FROM "item{item_id:0>5}" WHERE src=? AND timestamp >= ?""", (src, t0)
        else:
            exe = f"""SELECT * FROM "item{item_id:0>5}" WHERE src=? AND timestamp BETWEEN ? AND ?""", (src, t0, t1)
        
        if include_item_id is None:
            try:
                return self.cursors.get(return_type).execute(*exe).fetchall()
            except AttributeError:
                raise AttributeError(f'Invalid return_type {return_type} passed; it should be one of the following args;\n'
                      f'\t{str(tuple(self.cursors.keys()))}')
        else:
                return self.cursors.get(1 if include_item_id else -1).execute(*exe).fetchall()
    
    def npy_interval(self) -> Tuple[int, int]:
        """ Return the npy array timestamp lower- and upper- bound as a tuple """
        t1 = self.cursors.get(0).execute("""SELECT MAX(timestamp) FROM "item00002" WHERE src in (1, 2)""").fetchone()
        t1 = t1 - t1 % 14400
        return t1 - t1 % 86400 - cfg.npy_db_timespan_days * 86400, t1
        
    @staticmethod
    def factory_datapoint_item_id(c: sqlite3.Cursor, row: tuple) -> TimeseriesDatapoint:
        """ Parse timeseries rows as a labelled tuple, including the item_id """
        return TimeseriesDB.TimeseriesDatapoint(queried_item_id, *row)

    @staticmethod
    def factory_datapoint(c: sqlite3.Cursor, row: tuple) -> TimeseriesDatapoint_noid:
        """ Parse timeseries rows as a labelled tuple """
        return TimeseriesDB.TimeseriesDatapoint_noid(*row)
    

class Timeseries(ABC):
    """
    Template class for timeseries data structures and data points.
    
    Although the subclasses allow for defining individual datapoints as a Timeseries instance, the namedtuple associated
    with the subclass is likely to be a better alternative, as it provides a tuple that can be accessed through labels.
    
    The Timeseries class is used to define the structure of timeseries data, which is typically a sequence of datapoints.
    In this class, sqlite statements are defined for selecting data from the database, as well as datapoints.
    
    
    
    
    """
    db_file: str = gp.f_db_sandbox
    column_list: List[str]
    row_tuple: NamedTuple
    df_dtypes: Dict[str, str]
    name: str
    row_factory: Callable
    sql_select: str
    
    
    def __init__(self, table_name: str): 
        self.sql_select_by_item_id = f"""SELECT * FROM {table_name}  WHERE item_id = ?"""
        self.sql_select_by_item_id_t0 = f"""SELECT * FROM {table_name}  WHERE item_id = ? AND timestamp >= ?"""
        self.sql_select_by_item_id_t1 = f"""SELECT * FROM {table_name}  WHERE item_id = ? AND timestamp <= ?"""
        self.sql_select_by_item_id_t0_t1 = f"""SELECT * FROM {table_name}  WHERE item_id = ? AND timestamp BETWEEN ? AND ?"""
    
    @staticmethod
    @abstractmethod
    def dp(*args, **kwargs): ...
    
    def row_dict(self, **kwargs) -> dict:
        """ Return a subset of the kwargs that correspond to the columns of the timeseries data """
        return {c: v for c, v in kwargs.items() if c in self.column_list} 
    
    def row_tuple_(self, **kwargs) -> tuple:
        """ Return a subset of the kwargs as a specifically ordered tuple """
        return tuple([kwargs.get(c) for c in self.column_list])
    


if __name__ == "__main__":
    print(sql_timeseries_select(2))
    print(sql_timeseries_select(2, 0))
    print(sql_timeseries_select(2, 0, ts_start - 86400))
    print(sql_timeseries_select(2, 0, ts_start - 86400, ts_start))
    exit(12)
    
    print(create_item(2).__dict__)
    
    tsdb = TimeseriesDB()
    times = []
    times_plus1 = []
    for idx, i in enumerate(go.item_ids):
        t_ = time.perf_counter()
        _ = tsdb.get_numpy_rows(i)
        times.append(time.perf_counter()-t_)
        print(go.id_name[i], fmt.delta_t(times[-1]), end='\r')
        if times[-1] > 1:
            times_plus1.append({'id': i, 'name': go.id_name[i], 'time': fmt.delta_t(times[-1])})
        if idx % 50 == 49:
            pd.DataFrame(times_plus1).to_csv(gp.dir_data+'runtimes.csv', index=False)
    print('average', fmt.delta_t(sum(times)//len(times)))
    _ = input('')
    print('')
    exit(123)
    
    print(tsdb.get_numpy_rows(item_id=2, t0=int(time.time()-170000), t1=int(time.time()-160000)))
    for r in tsdb.get_numpy_rows(item_id=2):#, t0=int(time.time()-170000), t1=int(time.time()-160000)):
        print(r)
    
    exit(1)
    t_start = time.perf_counter()
    n = 0
    item_id = 2
    for src in range(5):
        t_ = time.perf_counter()
        rows = tsdb.fetch_rows(src, item_id, include_item_id=True)
        delta_t = time.perf_counter()-t_
        print(f"Source: {var.timeseries_srcs[src]} n_rows: {len(rows)} query_time: {fmt.delta_t(delta_t)}")
        print('\tExample row:', rows[0], '\n')
        n += len(rows)
    print(f'Total query time: {fmt.delta_t(time.perf_counter()-t_start)}\n'
          f'Total rows for item {go.id_name[item_id]}:', fmt.number(n))
    
