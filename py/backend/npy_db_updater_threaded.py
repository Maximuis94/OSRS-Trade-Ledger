"""
This executable module contains an implementation for generating the npy db

"""
import ctypes
import os
import shutil
import sqlite3
import threading
import time
import warnings
from collections import namedtuple
from collections.abc import Iterable, Sequence, Callable
from typing import Tuple, List

import numpy as np
import pandas as pd
from overrides import override

from venv_auto_loader.active_venv import *
import global_variables.configurations as cfg
import global_variables.osrs as go
import global_variables.path as gp
import util.array as u_ar
import util.file as uf
import util.str_formats as fmt
from util.logger import prt
import util.unix_time as ut
from controller.item import create_item, Item
from data_processing.npy_array_computations import avg_price_summed_volume
from file.file import File
from global_variables.data_classes import NpyDatapoint as NpyDp, NpyDatapoint
from model.data_source import DataSource, SRC
from model.database import Database
from tasks.async_task import AsyncTask
__t0__ = time.perf_counter()

t_start = int(time.perf_counter())
t_print = t_start
t_import = t_start
t_export = t_start
item = create_item(2)
est_vol_per_char = 0
ts = 0
min_timestamp, max_timestamp = 0, 0

cols, ar = [], np.ndarray([])

empty_tuple = ((),)[0]

active_threads: list = []
_n_active_threads = 0
_initialized, _n_to_do, _items_done, _item_ids, _n_threads = False, 0, 0, {}, -1
_rows_deleted, _rows_skipped, _rows_exported, _rows_processed, _tables_added = 0, 0, 0, 0, 0
new_ids, skip_ids = [], []
_files_imported = 0
_db_size_start, f_db = gp.f_db_npy.fsize(), gp.f_db_npy

_r = ("'", "")
_columns = str(NpyDatapoint.__match_args__).replace(*_r)
_values = str(tuple(['?' for _ in range(len(NpyDatapoint.__match_args__))])).replace(*_r)
sql_i_end = f"""{_columns} VALUES {_values}"""


class NpyDbUpdater(Database):
    """
    Class representation of the Npy Database. Used for managing Npy Array data.
    
    """
    NpyDatapoint = NpyDp
    npy_columns = NpyDatapoint.__match_args__
    array_directory = gp.dir_npy_arrays
    sql_i = f"""INSERT OR REPLACE INTO ___{str(NpyDatapoint.__match_args__)}
                            VALUES {str(tuple(['?' for _ in NpyDatapoint.__match_args__]))}""".replace("'", "")
    sql_del_rows: str = """DELETE FROM ___ WHERE timestamp < ?"""
    sql_count_del: str = f"""SELECT COUNT(*) FROM ___ WHERE timestamp < ?"""
    sql_fetch_src_wiki: str = f"""SELECT ?, price, volume, MAX(timestamp) FROM ___ WHERE src={SRC.w} AND timestamp<?"""
    sql_fetch_src_avg5m_buy: str = f"""SELECT price, volume, ? FROM ___ WHERE src={SRC.a_b} AND timestamp=?"""
    sql_fetch_src_avg5m_sell: str = f"""SELECT price, volume, ? FROM ___ WHERE src={SRC.a_s} AND timestamp=?"""
    sql_fetch_src_rt_buy: str = f"""SELECT price FROM ___ WHERE src={SRC.r_b} AND timestamp BETWEEN ? AND ?+299"""
    sql_fetch_src_rt_sell: str = f"""SELECT price FROM ___ WHERE src={SRC.r_s} AND timestamp BETWEEN ? AND ?+299"""
    sql_fetch_src_rt: str = f"""SELECT price FROM ___ WHERE src IN {SRC.by_source('realtime')} AND
                                timestamp BETWEEN ? AND ?+299"""
    sql_fetch_npy: str = """SELECT * FROM ___"""
    sql_ts_start: str = """SELECT MAX(timestamp) FROM ___ """
    vacuum_db: bool = False
    
    dir_export: str = gp.dir_npy_import
    print_frequency: int = 5
    updated_listbox: bool = False
    prices_listbox_path: File = gp.f_prices_listbox
    
    def __init__(self, thread_id: int, check_stop_execution: Callable = None, db_path: File = gp.f_db_npy,
                 source_db_path: str = gp.f_db_timeseries, item_ids: Sequence = go.npy_items,
                 execute_update: bool = True, add_arrays: bool = False, listbox_updater: bool = False,
                 update_counter: Callable = None, **kwargs):
        """
        
        Parameters
        ----------
        db_path : File, optional, global_variables.path.f_db_npy by default
            Database File
        source_db_path : File, optional, global_variables.path.f_db_timeseries by default
            Source database file
        item_ids : Sequence, optional global_variables.osrs.npy_items by default
            Sequence of item_ids that are included in the update
        execute_update : bool, optional, True by default
            If True, start updater protocol in the constructor.
        add_arrays : bool, optional, True by default
            If True, add numpy arrays (obsolete -- TODO: check if it can be removed)
        
        Other Parameters
        ----------------
        new_db : bool, optional, None by default
            Flag that indicates whether a new database should be created. If True, this will instantly delete the
            existing database.
        itemdb : sqlite3.Connection, optional, None by default
            If passed, use this connection for itemdb interactions instead
        
        """
        # if not _initialized:
        #     raise RuntimeError(f"Execute initialize() before starting any thread!")
        super().__init__(path=db_path, parse_tables=False)
        
        self.t_start = t_start if kwargs.get('start_time') is None else kwargs['start_time']
        self.thread_id = thread_id
        self.to_export = []
        self.check_stop_execution = check_stop_execution
        self.sql, self.sql_keys = {}, None
        
        if kwargs.get('new_db') is not None and kwargs.get('new_db') and db_path.exists():
            self.new_db()

        self.item_id_list = item_ids
        self.listbox_items = []
        
        # If True, only update the listbox
        if listbox_updater:
            self.prices_listbox = {}
            self.update_listbox()
        else:
            self.prices_listbox = None
            try:
                self.tuple_items = isinstance(item_ids[0], tuple)
            except IndexError:
                self.tuple_items = False
            
            self.i = self.item_id_list[0] if not self.tuple_items else self.item_id_list[0][0]
            
            self.new_ids = new_ids
            self.skip_ids = skip_ids
            self.update_counter = update_counter
            
            self.add_npy_array_files = add_arrays
            
            if kwargs.get('dp') is not None:
                self.NpyDatapoint = kwargs.get('dp')
                if len(self.NpyDatapoint.__match_args__) < len(self.npy_columns):
                    raise ValueError("dp can be passed to add additional columns, but this one seems to have less")
                self.npy_columns = self.NpyDatapoint.__match_args__
            
            self.sql_c = self.get_create_table_sql()
            
            self.item_id, self._r = 0, ''
            
            self.src_db = Database(source_db_path, read_only=True)
            self.itemdb: sqlite3.Connection or None = False
            self.est_vol_per_char = 0
            
            self.ts_con_avg5m = self.src_db.cursor()
            self.ts_con_avg5m.row_factory = self.timeseries_rows_factory
            self.ts_con_rt = self.src_db.cursor()
            self.ts_con_rt.row_factory = self.realtime_row_factory
            self.ts_con_wiki = self.src_db.cursor()
            self.ts_con_wiki.row_factory = self.timeseries_rows_factory_wiki
            
            self.con = self.write_con()
            self.con_npy = self.cursor()
            self.con_npy.row_factory = self.factory_extract_npy
            self.column_file = gp.f_npy_column
            # self.con_npy.row_factory = lambda c, row: self.NpyDatapoint(*row)
            self.con_by_src_id = [
                self.ts_con_wiki,
                self.ts_con_avg5m,
                self.ts_con_avg5m
            ]
            self.t0, self.t1, self.timestamps = 0, 0, []
            self.configure_default_timestamps()
            self.n_rows, self.n_deleted, self.n_created, self.n_skipped, self.n_processed = 0, 0, 0, 0, 0
            self.cur_id, self.item, self.active = 0, None, True
            # self.get_next_id()
            
            self.placeholder = [[0, 0]]
            self.prices_listbox_path, self.prices_listbox = None, {}
            
            if execute_update:
                self.generate_db()
    
    def configure_default_timestamps(self):
        """ Set up the timestamp array, given the configurations. """
        global min_timestamp, max_timestamp
        
        # If this is the case, a value was previously computed and should be re-used
        if max_timestamp > 0:
            self.t0, self.t1 = min_timestamp, max_timestamp
        
        # ... If not, determine lower and upper bound timestamp thresholds via most traded items
        t1 = 0
        for i in go.most_traded_items:
            try:
                t1 = max(t1, self.src_db.execute(
                    self.set_table_name(f"""SELECT MAX(timestamp) FROM ___
                    WHERE src in {SRC.by_source('avg5m')}""", i),
                    factory=0).fetchone())
            except TypeError:
                ...
        
        t1 = int(round(t1 / cfg.npy_round_t1, 0) * cfg.npy_round_t1)
        self.t0 = t1 - t1 % cfg.npy_round_t0 - cfg.npy_db_timespan_days * 86400
        self.t1 = t1 - t1 % cfg.npy_round_t1
        self.timestamps = range(self.t0, self.t1, 300)
        min_timestamp, max_timestamp = self.t0, self.t1
    
    def updater_print(self, exe_times_item):
        """ Old method for printing status update """
        global t_print
        t_print = int(time.perf_counter())
        try:
            print(f'\t[{fmt.passed_pc(self.t_start)}] Items: {_items_done + 1}/{_n_to_do}  '
                      f'Db size: +{fmt.fsize(self.fsize() - _db_size_start)}  '
                      f'Rows [+ {_rows_exported} / - {_rows_deleted}]  {_files_imported} files imported into db'
                      f'Avg/item: {fmt.delta_t(sum(exe_times_item) / len(exe_times_item))}', end='\r')
        except ZeroDivisionError:
            print(f'\t[{fmt.passed_pc(self.t_start)}] Items: {_items_done + 1}/{_n_to_do}  '
                  f'Db size: +{fmt.fsize(self.fsize() - _db_size_start)}  '
                  f'Rows [+ {_rows_exported} / - {_rows_deleted}]  {_files_imported} files imported into db', end='\r')
            
    def generate_db(self):
        """
        Generate rows for the Npy db on a per-item basis. Iterate over item_ids in `item_ids` and fetch row data
        spanning from `t0` to `t1`. Default timestamp interval is based on global_variables.configurations values.
        
        Parameters
        ----------
        item_ids : collections.abc.Sequence, optional, global_variables.osrs.npy_items by default
            The item_ids to collect data for
        t0 : int, optional, None by default
            The lower bound timestamp
        t1 : int, optional, None by default
            The upper bound timestamp
        
        Notes
        -----
        Time needed to complete execution depends on the amount of rows to generate, which in turn depends on the amount
        of item_ids included in the update. If a new item_id is added to the database, 288 rows are added for each day
        in the coverage. If the rows were previously added, the amount of to-be generated rows is drastically reduced.
        The time needed to generate a table and inserting it with rows is 5-10 seconds for a coverage of 60 days (17k
        rows)

        """
        # print(f'\tUpdating Npy array db...')
        t0, t1 = self.t0, self.t1
        self.con = self.write_con()
        exe_times_item = []
        self.n_processed, self.n_deleted, self.n_created, self.n_skipped = 0, 0, 0, 0
        last_item = self.item_id_list[-1]
        
        for item_id in self.item_id_list:
            if self.check_stop_execution is not None:
                self.check_stop_execution()
            if self.update_counter is not None:
                self.update_counter()
            if self.tuple_items:
                item_id, min_ts, _t0 = item_id
            else:
                _t0 = t1
                
            self.i = item_id
            self.item_id = item_id
            self.cur_id = item_id
            self.item = None
            self.update_item_id()
            item_start = time.perf_counter()
            if not isinstance(self.item, Item):
                print(f"\n\t* Unable to assign an Item to item_id={self.i}                           ")
                continue
            try:
                item_rows = 0
                for timestamp, w_p, w_v, w_ts, b_p, b_v, s_p, s_v, rt_p in self.generate_rows():
                    if w_p is None:
                        w_p = 0
                    if w_v is None:
                        w_v = 0
                    if b_p is None:
                        b_p = 0
                    if b_v is None:
                        b_v = 0
                    if s_p is None:
                        s_p = 0
                    if s_v is None:
                        s_v = 0
                    self.n_processed += 1
                    n_rt = len(rt_p)
                    if n_rt > 0:
                        rt_min, rt_max, rt_avg = rt_p[0], rt_p[-1], sum(rt_p) // len(rt_p)
                        tax = int(np.average(rt_p) * .01)
                        rt_margin = rt_p[-1]-rt_p[0]-tax
                    else:
                        rt_min, rt_max, rt_avg, rt_margin = 0, 0, 0, 0
                        try:
                            tax = int(max([s_p if s_p is not None else 0,
                                      b_p if b_p is not None else 0,
                                      w_p if w_p is not None else 0]) * .01)
                        except ValueError:
                            tax = 0
                    avg5m_margin = s_p - b_p-tax if min(b_p, s_p) != 0 else 0
                    self.est_vol_per_char = min(self.item.buy_limit*4, w_v // 10)
                    volume_coefficient = min(item.buy_limit, w_v) / max(item.buy_limit, 1)
                    
                    dt = ut.utc_unix_dt(timestamp)
                    avg5m_price, avg5m_volume = ((b_p+s_p)//2 if min(b_p, s_p) > 0 else max(b_p, s_p)), b_v+s_v
                    try:
                        self.to_export.append((item_id, timestamp, dt.minute, dt.hour, dt.day, dt.month, dt.year,
                                               dt.weekday(), timestamp//3600, timestamp // 86400, timestamp // 604800,
                                               w_ts, w_p, w_v, w_p * w_v, w_v // 288,
                                               b_p, b_v, b_p*b_v,
                                               s_p, s_v, s_p*s_v,
                                               avg5m_price, avg5m_volume, avg5m_price*avg5m_volume, avg5m_margin,
                                               (s_p-b_p)/w_p if w_p > 0 else 0, (b_p-w_p)/w_p if w_p > 0 else 0,
                                               (s_p-w_p)/w_p if w_p > 0 else 0,
                                               rt_avg, rt_min, rt_max, n_rt, rt_margin,
                                               tax, self.est_vol_per_char, volume_coefficient))
                    except OverflowError as e:
                        print(e)
                        raise e
                    except sqlite3.ProgrammingError as e:
                        print(e)
                        raise e
                if len(self.to_export) > 10:
                    self.export_rows()
                    if self.add_npy_array_files:
                        self.generate_npy_array(item_id)
                    exe_times_item.append(time.perf_counter()-item_start)
                    self.n_rows += item_rows
                else:
                    self.to_export = []
                    continue
            except ZeroDivisionError:
                if len(exe_times_item) % 10 > 1:
                    continue
            self.active = item_id == last_item
    
    def export_rows(self):
        """ Return the next path to dump an export for this updater """
        el = self.to_export[-1]
        # print(el)
        if self.execute(f"""SELECT COUNT(*) FROM "item{el[0]:0>5}" WHERE timestamp=?""", (el[1],), factory=0).fetchone() == 0:
            f = File(self.dir_export + f"{self.thread_id}_{self.cur_id:0>5}.dat")
            while f.exists():
                f = File(f.path.replace('.dat', '_.dat'))
            # if self.thread_id == 0:
            #     for el in self.to_export:
            #         print(el)
            self.check_stop_execution()
            f.save(self.to_export)
            self.listbox_items.append(self.cur_id if isinstance(self.cur_id, int) else self.cur_id[0])
            global t_export, _rows_exported
            _rows_exported += len(self.to_export)
            t_export = time.perf_counter()
        else:
            print(':(')
        self.to_export = []
        self.cur_id = self.item_id
        self.item_id = 0
        
    def update_item_id(self):
        """
        Load a new item_id; update sql statements by inserting its table. Each time this method is called, all sql
        statements are generated for `item_id`. If `item_id` is already loaded, do nothing.
        
        Parameters
        ----------
        item_id : int
            The to-be loaded item_id.
        
        Notes
        -----
        In order to minimize sql exe generations, execute operations with item_id as outermost loop.
        """
        if self.item is None or self.i != self.item.item_id:
            if len(self.to_export) > 0:
                self.export_rows()
            # print('cur_id is ', self.cur_id, self.item_id)
            self.item_id = self.i
            self.cur_id = self.i
            self.item = create_item(self.i, sqlite3.connect(database=f"file:{gp.f_db_local}?mode=ro", uri=True), True)
            self._r = ('___', f'"item{self.i:0>5}"')
            
            self.sql = {
                'fetch_wiki': self.sql_fetch_src_wiki.replace(*self._r),
                'fetch_src_avg5m_buy': self.sql_fetch_src_avg5m_buy.replace(*self._r),
                'fetch_avg5m_sell': self.sql_fetch_src_avg5m_sell.replace(*self._r),
                'fetch_rt_buy': self.sql_fetch_src_rt_buy.replace(*self._r),
                'fetch_rt_sell': self.sql_fetch_src_rt_sell.replace(*self._r),
                'fetch_rt': self.sql_fetch_src_rt.replace(*self._r),
                'insert': self.sql_i.replace(*self._r),
                'create': self.sql_c.replace(*self._r),
                'count_del': self.sql_count_del.replace(*self._r),
                'del_rows': self.sql_del_rows.replace(*self._r),
                'get_t0': self.sql_ts_start.replace(*self._r),
                'fetch_npy': self.sql_fetch_npy.replace(*self._r)
            }
            if self.sql_keys is None:
                self.sql_keys = tuple(self.sql.keys())
            # print(f"[{self.thread_id}] item_id was set to", self.item_id, self.cur_id, f'{len(_item_ids)} remaining')
    
    def generate_rows(self, t0: int = None):
        """ Generate rows for `item_id` with timestamps spanning from `t0` to `t1` """
        self.update_item_id()
        if t0 is None:
            t0 = self.execute(f"""SELECT MAX(timestamp) FROM "item{self.item_id:0>5}" """, factory=0).fetchone()
            if t0 is None:
                t0 = self.t0
                t1 = self.t1
        t0 = t0 - t0 % 300
        
        if t0 >= self.t1-300 or \
                self.execute(f"""SELECT COUNT(*) FROM "item{self.item_id:0>5}" WHERE timestamp=? """, (self.t1-300,), factory=0).fetchone() >= 1:
            return []
        return [list(self.get_src_data(SRC.w, timestamp)) +
                list(self.get_src_data(SRC.a_b, timestamp)) +
                list(self.get_src_data(SRC.a_s, timestamp)) +
                [self.get_src_data(5, timestamp)]
                for timestamp in range(t0, self.t1, 300)]
    
    def get_src_data(self, src: int or DataSource, timestamp: int):
        """
        Get data from source `src` from the currently configured item_id or set config to `item_id`, if specified.````
        The following sources have been mapped to `src`s;
        0: wiki, 1: avg5m_buy, 2: avg5m_sell, 3: realtime_buy, 4: realtime_sell, 5: realtime_merged.
        Src 0-2 yield a price and volume, while src 3-5 yield a list of prices applicable for `timestamp`
        
        Parameters
        ----------
        src : int or DataSource
            The source ID of the data to fetch. Can also be passed as an instance of DataSource.
        timestamp: int
            The timestamp of the data that is to be fetched
        item_id : int, optional, None by default
            If passed, set the configured item_id to `item_id`. If `item_id` is the active item_id, execute as if
            `item_id` was not passed.

        Returns
        -------
        List[int, int, int, int]
            src 0: A timestamp, wiki_timestamp, price, volume tuple. Note that the first wiki timestamp that is smaller
            than or equal to `timestamp` is returned.
        
        List[int, int]
            src 1, 2: A price, volume tuple. If no entry exists at timestamp=`timestamp`, return [0, 0]
        
        List[int, ...]
            src > 2: A list of realtime prices, in the range of `timestamp`-`timestamp+299`

        """
        self.update_item_id()
        
        if not isinstance(src, int):
            src = src.src_id
        
        sql = self.sql.get(self.sql_keys[src])
        # print(sql)
        try:
            # if src==0:
            #     print(self.con_by_src_id[src].execute(sql, (timestamp, timestamp)).fetchone())
            return self.con_by_src_id[src].execute(sql, (timestamp, timestamp)).fetchone() if src == 0 else \
                (self.con_by_src_id[src].execute(sql, (timestamp, timestamp)).fetchone()+[0, 0])[:2]
        except IndexError:
            return self.ts_con_rt.execute(sql, (timestamp, timestamp)).fetchall()
        except TypeError:
            return [0, 0]
    
    def generate_table(self, item_id):
        """ Generate a table for `item_id` """
        self.update_item_id()
        self.con.execute(self.sql.get('create'), (item_id,))
        
    
    def delete_rows(self, item_id: int = None, t0: int = None) -> int:
        """ Delete rows with a timestamp smaller than `t0` or self.t0. Return the amount of deleted rows. """
        if item_id is not None:
            self.i = item_id
            
        self.update_item_id()
        parameters = ((self.t0 if t0 is None else t0),)
        n_rows = self.execute(self.sql.get('count_del').replace(*self._r), parameters, factory=0).fetchone()
        self.con.execute(self.sql.get('del_rows').replace(*self._r), parameters)
        self.con.commit()
        return n_rows
    
    def generate_npy_array(self, item_id: int, overwrite: bool = True, generate_csv: bool = False):
        """ Generate arrays with data of `item_id` and export them to a npy file in the array directory. """
        if not os.path.exists(self.array_directory):
            raise FileNotFoundError(f"Unable to export array files to non-existent directory {self.array_directory}")
        if not overwrite and self.array_file(item_id).exists():
            return
        # self.update_item_id(item_id=item_id)
        global cols, ar
        
        # Disable warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            warnings.warn("deprecated", DeprecationWarning)
            
            cols = list(self.npy_columns)
            ar = np.array(self.con_npy.execute(self.sql.get('fetch_npy')).fetchall())
            ar, cols = avg_price_summed_volume(ar, cols, 12, '1h')
            ar, cols = avg_price_summed_volume(ar, cols, 48, '4h')
            # ar, cols = avg_price_summed_volume(ar, cols, 288, '1d')
            if generate_csv:
                pd.DataFrame(ar, columns=cols).to_csv(gp.dir_data+'test.csv')
            if not self.column_file.exists() or time.time()-self.column_file.mtime() > 3600:
                self.column_file.save(cols)
            self.save_arrays(ar, cols, item_id)
    
    def array_file(self, item_id) -> File:
        """ Return the path to the array file with data for `item_id` """
        return File(f'{self.array_directory}{item_id:0>5}.npy')
    
    def save_arrays(self, arrays: np.ndarray, column_names: Iterable[str], item_id: int):
        """ Save `arrays` with corresponding `column_names` for `item_id` under the appropriate file name """
        return uf.save((column_names, arrays), self.array_file(item_id))
    
    def load_arrays(self, item_id: int) -> Tuple[np.ndarray, Iterable[str]]:
        """ Load the array file with data for `item_id` """
        column_names, arrays = uf.load(self.array_file(item_id))
        return column_names, arrays
    
    def generate_remaining_arrays(self, item_ids):
        """ Generate the array files from `item_ids` that have not been generated with the current data. """
        f_size = os.path.getsize(uf.get_newest_file(list(uf.get_files(self.array_directory))))
        done = [int(f.split('.')[-2][-5:]) for f in uf.get_files(self.array_directory) if os.path.getsize(f) == f_size]
        # print(done)
        to_do = frozenset(item_ids).difference(done)
        n_to_do = len(to_do)
        t_ar_gen = time.perf_counter()
        print('\n')
        for idx, item_id in enumerate(u_ar.unique_values(to_do, return_type=tuple, sort_ascending=True)):
            print(f' Generating remaining {n_to_do-idx-1} arrays...    ', end='\r')
            try:
                self.generate_npy_array(item_id, True)
            except sqlite3.OperationalError:
                ...
        print(f'\nRemaining arrays were generated in {fmt.passed_pc(t_ar_gen)}')
    
    @staticmethod
    def get_table_name(item_id: int) -> str:
        """ Return the formatted table name corresponding to `item_id`, e.g. 'item00002' for item_id=2 """
        return f'"item{item_id:0>5}"'
    
    @staticmethod
    def set_table_name(sql: str, item_id: int) -> str:
        """ Replace the triple underscores in the sql statement with the table name corresponding to `item_id` """
        return sql.replace('___', NpyDbUpdater.get_table_name(item_id))
    
    @staticmethod
    def timeseries_rows_factory(c: sqlite3.Cursor, row: tuple):
        """ Row factory for timeseries price, volume queries. Return 0, 0 if data is not present for given inputs. """
        return list(row[:2])
    
    @staticmethod
    def realtime_row_factory(c: sqlite3.Cursor, row: tuple):
        """ Row factory for timeseries price, volume queries. Return 0, 0 if data is not present for given inputs. """
        return row[0]
    
    @staticmethod
    def timeseries_rows_factory_wiki(c: sqlite3.Cursor, row: tuple):
        """ Row factory for timeseries price, volume queries. Return 0, 0 if data is not present for given inputs. """
        return list(row)
    
    @staticmethod
    def print_progress(t_start: int, n_done: int, n_total: int, avg_runtime: int,
                       print_frequency: int = cfg.data_transfer_print_frequency):
        """ Print method for a progress update. Only print if the cooldown has passed. """
        global t_print
        if time.perf_counter() > t_print:
            print(f'[{fmt.passed_pc(t_start)}] {n_done}/{n_total} done '
                  f'(avg/item={fmt.delta_t(avg_runtime)})              ', end='\r')
            t_print = time.perf_counter() + print_frequency
    
    @staticmethod
    def factory_extract_npy(c: sqlite3.Cursor, row: tuple):
        """ Row factory that can be set to a sqlite3.Connection """
        try:
            return NpyDbUpdater.NpyDatapoint(*row)
        except TypeError:
            return NpyDbUpdater.NpyDatapoint(*row[:len(NpyDbUpdater.npy_columns)])
    
    @staticmethod
    def generate_template_db(path: File = File(gp.dir_template+'npy.db')):
        """ Generate a template db+npy file, which has one table, to get an impression of what the db looks like. """
        dp = namedtuple('NpyDatapoint', list(NpyDbUpdater.NpyDatapoint.__match_args__) + ['src', 'price', 'volume'])
        
        if path.exists() and path.fsize() > pow(10, 7):
            raise FileExistsError(f'File already exists at {path}. Are you sure you want to create a template db here?')
        db = NpyDbUpdater(path, new_db=False, item_ids=[2], execute_update=True, add_arrays=False, dp=dp)
        db.commit()
        db.close()
        db = NpyDbUpdater(path, execute_update=False, new_db=False)
        db.generate_npy_array(2)
        db.execute("ALTER TABLE item00002 RENAME TO ___")
        db.commit()
        db.close()
        shutil.copy2(db.array_directory+'00002.npy', os.path.dirname(path)+'/___.npy')
    
    @staticmethod
    def get_create_table_sql(item_id: int = None) -> str:
        """ Returns an executable sqlite statement for creating a table for item `item_id` """
        sql_c = f"""CREATE TABLE "item___"(""" if item_id is None else f"""CREATE TABLE "item{item_id:0>5}"("""
    
        for col in NpyDbUpdater.NpyDatapoint.__match_args__:
            if col[:3] == "gap" or "coefficient" in col:
                sql_c += f""""{col}" REAL NOT NULL DEFAULT 0.0, """
            else:
                sql_c += f""""{col}" INTEGER NOT NULL DEFAULT 0, """
        return sql_c + "PRIMARY KEY(timestamp) )"
    
    @override
    def vacuum(self, temp_file: str = None, verify_vacuumed_db: bool = True, remove_temp_file: bool = True):
        """
        VACUUM this database.
        First, execute VACUUM INTO using `temp_file` as target file. After successfully executing it, verify if `verify`
         is True, then replace the old db file with the vacuumed db file.
        
        Parameters
        ----------
        temp_file : str, optional, None by default
            Temporary file to use as a target to VACUUM the database into. By default, add a _ to the file name.
        verify_vacuumed_db : bool, optional, True by default
            If True, compare the rows per table in the vacuumed db with the rows per table in the source db. A failed
            verification will prevent the old db file to be overwritten.
        remove_temp_file : bool, optional, True by default
            Remove the vacuumed db file after overwriting the original db file.
            
        Returns
        -------
        True
            If operation fully completed.
        False
            If operation ended prematurely (insufficient disk space available / row verification failed)
        
        Notes
        -----
        Depending on the size of the database file, the VACUUM operation can be quite demanding in terms of disk space,
        as the database is temporarily duplicated.
        If disk space is an issue on the original disk, `temp_file` can be used to bypass this problem without having to
         move the db before executing VACUUM.
        
        References
        ----------
        https://sqlite.org/lang_vacuum.html
            For more information on VACUUM, consult this website.
        """
        try:
            self.con.close()
            self.con_npy.close()
            super().vacuum(temp_file=temp_file, verify_vacuumed_db=verify_vacuumed_db, remove_temp_file=remove_temp_file)
            return 1
        except PermissionError:
            return -1
    
    def update_listbox(self, force_rebuild: bool = True):
        """ Update all prices listbox entries and save the result """
        t_listbox = time.perf_counter()
        to_do = self.item_id_list
        if force_rebuild or not self.prices_listbox_path.exists():
            self.prices_listbox = {}
        else:
            self.prices_listbox = self.prices_listbox_path.load()
        to_do = [i[0] if isinstance(i, tuple) else i for i in to_do]
        n_to_do, n_failed = len(to_do), 0
        print('')
        print(f'\tUpdating Listbox. Processed 0/{n_to_do} items...      ', end='\r')
        for idx, item_id in enumerate([i for i in to_do if go.id_name[i] is not None]):
            print(f'\t[{fmt.passed_pc(self.t_start)}] Updating Listbox. Processed {idx+1}/{n_to_do} items...', end='\r')
            try:
                try:
                    self.update_prices_listbox_entry(item_id, cur_t1=self.prices_listbox.get(item_id)[0].get('t0'))
                except TypeError:
                    self.update_prices_listbox_entry(item_id)
            except AttributeError as e:
                n_failed += 1
                print(f"Failed to create listbox entry for item {go.id_name[item_id]} "
                      f"({n_failed} {'entry' if n_failed == 1 else 'entries'} failed so far)")
                
        self.prices_listbox_path.save(self.prices_listbox)
        print(f'\n\t[{fmt.passed_pc(self.t_start)}] Updated {n_to_do} listbox entries in {fmt.passed_pc(t_listbox)}')
    
    def update_prices_listbox_entry(self, item_id: int, _n_rows: int = cfg.prices_listbox_days, n_intervals: int = 6, cur_t1: dict = None):
        """
        Compute prices listbox entries for `item_id`. Each row shows the price development for an item throughout the
        day and summarizes this.
        Provide a sell price for past 24h and a buy price for each 4h interval for past 24h for the given item.
    
        The buy price is saved for each 4h interval encountered and corresponds to the index at 10% of the sorted, non-zero
        prices list. The sell price is determined by averaging the 3 highest sell prices across the prices at the top 90% of
        the sorted, non-zero prices list across all 4h intervals. The resulting rows cover 24h each, sequentially and non-
        overlapping.
    
    
        Parameters
        ----------
        item_id : int
            item_id of the item that is processed
        _n_rows : int, optional, global_variables.configurations.prices_listbox_days by default
            Amount of rows to produce as output, where each row corresponds to a 24 hour timespan.
        n_intervals : int, optional, 6 by default
            Amount of smaller, equally sized intervals to divide each 24-hour timespan in
    
        Notes
        -----
        The tags are encoded as b_X_Y, with b referring to buy price, X referring to the first hour of the interval and
        Y referring to the final hour of the interval (UTC times). In the GUI, they are converted to local time rather
        than UTC time. E.g. b_4_8 refers to the buy price in the interval of 4am-8am (UTC).
        Sell prices are shown as the last interval for that row and the average across 3/6 highest sell prices across
        all intervals.
        """
        if self.prices_listbox is None:
            raise TypeError
        try:
            interval_start = self.execute(f"SELECT MAX(timestamp) FROM item{item_id:0>5}", factory=0).fetchone()-86100
        except TypeError:
            interval_start = self.t0
        interval_size = int(24 / n_intervals)*3600
        
        # Fix for missing entries (would otherwise
        if interval_start % interval_size != 0:
            interval_start = interval_start + cfg.listbox_column_timespan - interval_start % cfg.listbox_column_timespan
        
        # Entry was computed earlier -- move on instead
        if cur_t1 is not None and cur_t1 == fmt.unix_(interval_start, fmt_str='%d-%m %H:%M'):
            return
        
        buy_tags = {0: 'b_0_4', 4: 'b_4_8', 8: 'b_8_12', 12: 'b_12_16', 16: 'b_16_20', 20: 'b_20_24'}
        rows = []
        interval_step = 288 // n_intervals
        
        for day_idx in range(_n_rows):
            min_buy = None
            cur = {'t0': fmt.unix_(interval_start, fmt_str='%d-%m %H:%M')}  # , 'ts': interval_start}
            sell_prices = []
            for i in range(0, n_intervals*interval_step, interval_step):
                
                params = (interval_start, interval_start+interval_size-1)
                a5m_p = np.array(self.execute(f"SELECT avg5m_price FROM item{item_id:0>5} WHERE timestamp "
                                              f"BETWEEN ? AND ? AND avg5m_price > 0 ORDER BY avg5m_price",
                                              params, factory=0).fetchall(), dtype=np.int32)
                
                tag = buy_tags.get(interval_start % 86400 // 3600)
                
                try:
                    ar_5p = int(len(a5m_p) * .05)
                    buy_price = int(a5m_p[ar_5p])
                    cur[tag] = buy_price
                    sell_prices.append(a5m_p[-ar_5p])
                    min_buy = min(buy_price, min_buy) if isinstance(min_buy, int) else buy_price
                except IndexError:
                    cur[tag] = 1
                cur['ts'] = interval_start
                interval_start += interval_size
            try:
                cur['s_24h_last'] = int(sell_prices[-1])
                s_h = int(np.average(np.sort(sell_prices)[-3:]))
                cur['s_24h_high'] = s_h
                cur['volume'] = int(self.execute(f"SELECT AVG(wiki_volume) FROM item{item_id:0>5} WHERE timestamp "
                                                 f"BETWEEN ? AND ? ORDER BY timestamp ",
                                                 (interval_start-86400, interval_start), factory=0).fetchone())
                cur['delta_s_b'] = s_h - min_buy
            
            except ValueError:
                print(f'ValueError for item {go.id_name[item_id]}')
                continue
            except IndexError:
                ...
            
            rows.append(cur)
            interval_start = interval_start - 86400 * 2
        self.prices_listbox[item_id] = rows
        self.updated_listbox = True
    
    def new_db(self):
        """ Delete the existing database and generate a new one + tables. Prompt user for a final confirmation. """
        if input('Are you sure you wish to delete the current NPY database? This cannot be undone!\n'
                 '\tPress "y" to confirm: ').lower() == 'y':
            print('Confirmed. Database will be deleted in 3 seconds...')
            time.sleep(5)
            self.delete()
            n = len(_item_ids)
            
            print("Database was deleted; generating new tables for each item...")
            for idx, item_id in enumerate(list(_item_ids.keys())):
                print(f'\tGenerated {idx}/{n} tables...         ', end='\r')
                self.generate_table(item_id)
            print('\nAll tables have been generated!')
        else:
            print('Aborting db update and terminating script...')
            time.sleep(5)
            exit(1)


def get_sqls(item_id: int) -> Tuple[str, str]:
    """ Return sql statements for respectively creating the table and inserting data in said table """
    if item_id not in go.item_ids:
        raise RuntimeError(f'get_sqls() was called while passing non-existent item_id={item_id}')
    
    table = f"'item{item_id:0>5}'"
    sql_c = f"""CREATE TABLE IF NOT EXISTS {table}("""
    for col in NpyDatapoint.__match_args__:
        if col[:3] == 'gap' or 'coefficient' in col:
            sql_c += f""""{col}" REAL NOT NULL DEFAULT 0.0, """
        else:
            sql_c += f""""{col}" INTEGER NOT NULL DEFAULT 0, """
    return sql_c + "PRIMARY KEY(timestamp) )", f"""INSERT OR REPLACE INTO {table} {sql_i_end}""".replace("'", "")


def import_files(src_dir: str, npy_db: File):
    """ Upload all dat files present in `src_dir` to sqlite database `npy_db` """
    files = uf.get_files(src=src_dir, ext='dat', full_path=False)
    
    for f in files:
        f = File(src_dir+f)
        try:
            item_id = int(f.file[2:7])
            if time.time()-f.mtime() < 10 or not f.file[0].isdigit():
                continue
        except TypeError as e:
            print(e)
            continue
        con = sqlite3.connect(npy_db)
        sql_c, sql_i = get_sqls(item_id)
        con.execute(sql_c)
        _c, commit = 0, False
        for values in f.load():
            try:
                con.execute(sql_i, values)
                commit = True
            except sqlite3.Error as e:
                print(sql_i)
                print(e, values)
                _c += 1
        if commit:
            con.commit()
            if _c == 0:
                f.delete()
                
                global _files_imported
                _files_imported += 1
            else:
                print(f'Failed to insert {_c} rows from file {f}')
    

def importer_task(src_dir=gp.dir_npy_import, min_runtime: int = 15, can_proceed: Callable = None):
    """ Thread designed to upload generated batches to the sqlite db. After uploading a batch, it is deleted. """
    threader_count = threading.active_count()
    min_runtime = time.perf_counter() + min_runtime
    if can_proceed is None:
        def can_proceed():
            """ Return True to keep going, False to proceed with other updates. """
            return (threading.active_count() > threader_count or
                    len(os.listdir(gp.dir_npy_import)) > 0 or
                    time.perf_counter() < min_runtime)
    
    # By design, the thread should stay active as long as threads supplying it data are active / there is data to upload
    while can_proceed():
        import_files(src_dir=src_dir, npy_db=gp.f_db_npy)
        time.sleep(3)
    prt(f"Importer thread was completed! Updating listbox...                               ", n_newline=1, n_indent=1)
    NpyDbUpdater(listbox_updater=True, thread_id=4)


def do_nothing():
    """ Default method for undefined Callable args """
    ...


class AsyncPreprocessing(threading.Thread):
    """
    Class for a single thread to produce npy db rows
    """
    def __init__(self, idx: int, callback_end: Callable = do_nothing, callback_completed: Callable = do_nothing,
                 callback_failed: Callable = do_nothing, item_ids: List[int] = go.npy_items, n_threads: int = 4,
                 decrease_count: Callable = do_nothing, **kwargs):
        """ `idx` is the index of this thread; callback_[end/completed/failed] are executed at the end of the thread """
        threading.Thread.__init__(self, name=kwargs.get('name'), daemon=kwargs.get('daemon'))
        self.on_finished = callback_end
        self.on_completed = callback_completed
        self.on_failed = callback_failed
        self.string = ''
        self.idx = idx
        self.n_threads = n_threads
        self.active = False
        self.db_path = gp.f_db_npy if kwargs.get('db_path') is None else kwargs['db_path']
        self.completed = False
        
        self.kwargs = {
            'item_ids': [i for i in item_ids if i % n_threads == idx],
            'thread_id': idx,
            'path': f_db,
            'source_db_path': gp.f_db_timeseries,
            'add_arrays': False,
            'execute_update': True,
            'prices_listbox_path': None,
            'itemdb': sqlite3.connect(database=f"file:{gp.f_db_local}?mode=ro", uri=True),
            'check_stop_execution': self.conditionally_terminate_thread,
            'update_counter': decrease_count
        }
        # NpyDbUpdater(**self.kwargs)
        # self.run()
    
    def run(self):
        """ This code is executed by an individual thread """
        self.active, _t_start = True, time.perf_counter()
        try:
            NpyDbUpdater(**self.kwargs)
            self.completed = True

        except RuntimeError as e:
            print(e)
            self.conditionally_terminate_thread()
        self.active = False
            
        # print(f"\tExecution of thread {self.idx} ended after running for {fmt.passed_pc(_t_start)}")
        self.on_finished()

    def get_id(self):
        """ returns id of the respective thread """
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id
    
    def conditionally_terminate_thread(self):
        """ Terminate this thread if it is alive while it is also marked as inactive """
        if not self.active and self.is_alive():
            self.callback_on_finished()
            self.raise_exception()
            self.join()
    
    def raise_exception(self):
        thread_id = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
    
    # def thread_completed(self):
    #     """ Executes thread was completed without any errors """
    #     self.on_failed()
    #     self.on_completed()
    
    def callback_on_finished(self):
        """ Execute on_completed OR on_failed, followed by on_finished """
        self.active = False
        if self.completed:
            self.on_completed()
        else:
            self.on_failed()
        self.on_finished()


class UpdaterThreadManager:
    """
    Multi-threaded updater class for Npy Db data. The Npy Db consists of pre-computed values that are derived from raw
    timeseries data of a subset of items. Amount of Npy Db rows is incremented on a per-hour basis.
    Items of which the price behaviour meets specific criteria (/it exceeds certain thresholds) are tracked and have its
    rows computed. Following computation of npy db rows, is computing prices listbox rows. This is a set of 56 rows per
    item (or 8 weeks), in which the buy prices are shown throughout the day in 4-hour segments, and the sell price on a
    per-day basis.
    
    Notes
    -----
    The timespan of the Npy db is ~15 months.
    All Npy db data has exactly the same resolution of 288 rows per day, or 1 row per 5 minutes (i.e., each timestamp
    can be divided by 300. Due to the sheer amount of rows per item and the time needed to compute rows for each item,
    the subset of items is restricted. Initial computation of all rows might take a while, as each tracked item has
    ~131k rows. The multi-threaded approach has N threads computing row data, and one thread uploading row data produced
     by other threads.
    """
    t0_floor_value: int = cfg.npy_round_t0
    t1_floor_value: int = cfg.npy_round_t1
    db_from: File = gp.f_db_timeseries
    db_to: File = gp.f_db_npy
    npy_columns: Tuple[str] = NpyDatapoint.__match_args__
    
    def __init__(self, n_threads: int = 3, item_ids: List[int] = go.npy_items,
                 start_time: int or float = time.perf_counter()):
        self.threads = []
        self.n_threads = n_threads
        self.start_time = start_time
        self.t0: int = 0
        self.t1: int = 0
        self.item_ids: List[int] = item_ids
        self.to_do: List[int] = []
        self.item_t1 = {}
        self.n_to_do = len(item_ids)
        self.skip_ids: List[int] = []
        self.n_created: int = 0
        self.n_deleted: int = 0
        self.n_inserted: int = 0
        self.active_threads_at_start = 0
        self.n_remaining = 0
        self.importer_active = True
        
        self.timestamps_configured = False
        self.queue_sorted = False
        self.database_preprocessed = False

        self.import_thread = AsyncTask(task=importer_task, can_proceed=self.is_active, callback_oncomplete=self.deactivate_importer)
        self.initialize()
        
    def start_threads(self):
        """ Setup up and initiate all threads, given the configurations """
        if not self.database_preprocessed:
            self.preprocess_database()
        
        self.active_threads_at_start = threading.active_count()
        self.n_remaining = len(self.to_do)
        for thread_id in range(self.n_threads):
            _thread = AsyncPreprocessing(
                idx=thread_id,
                n_threads=self.n_threads,
                item_ids=self.to_do,
                callback_completed=self.thread_completed,
                callback_failed=self.thread_failed,
                decrease_count=self.decrease_n_to_do
            )
            self.threads.append(_thread)

        print(f"\t[{fmt.passed_pc(self.start_time)}] Starting threads...")
        for idx, t in enumerate(self.threads):
            # print(f"[{fmt.passed_pc(self.start_time)}] Starting thread {idx+1}...")
            t.start()
        # print(f"[{fmt.passed_pc(self.start_time)}] Starting import thread...")
        self.import_thread.start()
        
        self.n_threads = len(self.threads)
        len_import_files = 1
        while self.n_threads > 0 or len_import_files > 0 or self.importer_active:
            time.sleep(2)
            len_import_files = len(uf.get_files(gp.dir_npy_import, ext='dat', full_path=False))
            if len_import_files + self.n_remaining > 0:
                print(f"\t[{fmt.passed_pc(self.start_time)}] n_threads active: {self.n_threads} | "
                      f"n_import files: {len_import_files} | Items remaining: {self.n_remaining}                 ",
                      end='\r')
            else:
                self.n_threads = 0
                for t in self.threads:
                    
                    t.join()
        print(f"\t[{fmt.passed_pc(self.start_time)}] Done")
    
    def decrease_n_to_do(self, value: int = 1):
        """ Method for decreasing the items remaining count by `value` """
        self.n_remaining -= value
    
    def thread_finished(self, s: str = ''):
        """ Executed at the very end of a thread, be it through failure or success """
        self.n_threads -= 1
        print(f"\t[{fmt.passed_pc(self.start_time)}] {s} Active threads remaining:{self.n_threads}                      ")
    
    def thread_completed(self):
        """ Executed if the thread is completed without any errors """
        self.thread_finished(f"Thread completed!")
    
    def thread_failed(self):
        """ Executed if the thread fails at some point """
        self.thread_finished("Thread failed!")

    def configure_default_timestamps(self):
        """ Set up the timestamp array, given the configurations. """
        src_db = sqlite3.connect(database=f"file:{self.db_from}?mode=ro", uri=True)
        src_db.row_factory = lambda c, row: row[0]
        t1 = max([src_db.execute(f"""SELECT MAX(timestamp) FROM item{i:0>5} WHERE src in {SRC.by_source('avg5m')}"""
                                 ).fetchone() for i in go.most_traded_items])
        t1 = int(round(t1 / cfg.npy_round_t1, 0) * cfg.npy_round_t1)
        self.t0 = t1 - t1 % cfg.npy_round_t0 - cfg.npy_db_timespan_days * 86400
        self.t1 = t1 - t1 % cfg.npy_round_t1
        prt(f"Timestamp range was set to {ut.loc_unix_dt(self.t0)} - {ut.loc_unix_dt(self.t1)}")
        self.timestamps_configured = True
    
    def sort_queue(self, criterion: str = 'MAX(timestamp)', na_value: int = 0, reverse_sort: bool = True):
        """ Sort item queue by `criterion`. Assign `na_value` to missing rows; reverse with `reverse_sort` """
        con = sqlite3.connect(self.db_to)
        con.row_factory = lambda c, row: row[0]
    
        result = {}
        for i in self.item_ids:
            item_score = con.execute(f"SELECT {criterion} FROM 'item{i:0>5}'").fetchone()
            if item_score is None:
                item_score = na_value
            while result.get(item_score) is not None:
                item_score += 1
            result[item_score] = i
    
        keys = list(result.keys())
        keys.sort(reverse=reverse_sort)
        try:
            sort_order = "ASC" if keys[1] > keys[0] else "DESC"
        except TypeError:
            sort_order = ""
            
        self.item_ids = [result.get(k) for k in keys]
        print(f"\tItem ids were sorted by {criterion} {sort_order}")
        self.queue_sorted = True

    def preprocess_database(self):
        """ Generate a table in the npy db for each of the ids in `item_ids`, mark an id to be skipped if its up to date """
        global new_ids, skip_ids, _rows_deleted
        
        if not self.timestamps_configured:
            self.configure_default_timestamps()
        
        _t0 = int(time.time())
        db = sqlite3.connect(database=f"file:{self.db_from}?mode=ro", uri=True)
        db.row_factory = lambda c, row: row[:2]
        con = sqlite3.connect(self.db_to)
        con.row_factory = lambda c, row: row[0]
        
        self.to_do = []
        self.skip_ids = []
        self.item_t1 = {}
        self.n_created = 0
        self.n_deleted = 0
        n = len(self.item_ids)
        prt(f"Preparing database...")
        for idx, i in enumerate(self.item_ids):
            try:
                t0 = con.execute(f"""SELECT MIN(timestamp) FROM "item{i:0>5}" """).fetchone()
                if t0 is None:
                    t0 = 0
                t1 = con.execute(f"""SELECT MAX(timestamp) FROM "item{i:0>5}" """).fetchone()
                if t1 is None:
                    t1 = 0
                else:
                    t1 = t1 - t1 % 3600 - 300
                print(f"\t[{idx+1}/{n}] Current item_id={i} | t0={ut.loc_unix_dt(t0)} | t1={ut.loc_unix_dt(t1)}", end='\r')
            except sqlite3.OperationalError as e:
                if "no such table" in str(e):
                    t0 = None
                    
                else:
                    raise e
                    
            if t0 is None:
                t0, t1 = 0, 0
                self.create_table(con=con, item_id=i)
                self.n_created += 1
                print(f"\t[{idx + 1}/{n}] Current item_id={i} | t0={ut.loc_unix_dt(t0)} | t1={ut.loc_unix_dt(t1)}",
                      end='\r')
            # Mark this id to be skipped
            if t1 >= self.t1:
                self.skip_ids.append(i)
            else:
                self.to_do.append(i)
            
            # Remove expired rows
            if 0 < t0 < self.t0:
                self.n_deleted += self.delete_rows(con=con, item_id=i)
            
            cur_t1 = t1
            while self.item_t1.get(cur_t1) is not None:
                cur_t1 += 1
            self.item_t1[cur_t1] = (i, t1)
        _rows_deleted += self.n_deleted
        self.n_deleted = 0
        
        keys = list(self.item_t1.keys())
        keys.sort()
        self.item_t1 = {self.item_t1.get(k)[0]: self.item_t1.get(k)[1] for k in keys if self.item_t1.get(k)[0] in self.to_do}
        print("")
        prt(f"Preprocessed DB in {fmt.delta_t(time.time()-_t0)} | "
            f"New tables: {self.n_created}, rows deleted: {self.n_deleted}, n_to_do: {len(self.to_do)}")
        if self.database_preprocessed:
            return
        self.database_preprocessed = True
        self.start_threads()
        
    def initialize(self):
        """ Set up the pipeline; initialize variables and execute """
        self.configure_default_timestamps()
        self.preprocess_database()
        # self.sort_queue(reverse_sort=False)
    
    def create_table(self, con: sqlite3.Connection, item_id: int):
        """ Create a new table for `item_id` and increase the `n_created` counter by 1 """
        # print(_columns, _values)
        # print()
        # print(f"""CREATE TABLE "item{item_id:0>5}" {_columns} VALUES ({", ".join(["?" for _ in range(len(_columns))])})""")
        # con.execute(f"""CREATE TABLE "item{item_id:0>5}" {_columns} VALUES ({", ".join(["?" for _ in range(len(_columns))])})""")
        con.execute(NpyDbUpdater.get_create_table_sql(item_id))
        con.commit()
        self.n_created += 1
        return
    
    def delete_rows(self, con: sqlite3.Connection, item_id: int) -> int:
        """ Count+delete expired rows from the connected db for `item_id`. Return the count. """
        params = (self.t0,)
        output = con.execute(f"""SELECT COUNT(*) FROM "item{item_id:0>5}" WHERE timestamp < ?""", params).fetchone()
        con.execute(f"""DELETE FROM "item{item_id:0>5}" WHERE timestamp < ?""", params)
        con.commit()
        return output
    
    def is_active(self):
        return self.n_remaining > 0 or len(uf.get_files(gp.dir_npy_import, ext='dat', full_path=False)) > 0
    
    def deactivate_importer(self):
        self.importer_active = False


if __name__ == '__main__':
    # db = sqlite3.connect(gp.f_db_npy)
    # for i in go.npy_items:
    #     db.execute(f"""DELETE FROM "item{i:0>5}" WHERE timestamp > ?""", (1726440900,))
    # db.commit()
    # db.execute('VACUUM')
    exit(1)
    
    # This call should initiate the entire Npy updater pipeline (after the timeseries db!)
    utm = UpdaterThreadManager()
    