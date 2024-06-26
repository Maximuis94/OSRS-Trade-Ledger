"""
This executable module contains an implementation for generating the npy db

"""
import os
import shutil
import sqlite3
import time
import warnings
from collections import namedtuple
from collections.abc import Iterable, Sequence
from typing import Tuple, List
from warnings import warn

import numpy as np
import pandas as pd
from overrides import override

import global_variables.configurations as cfg
import global_variables.osrs as go
import global_variables.path as gp
import util.array as u_ar
import util.file as uf
import util.str_formats as fmt
import util.unix_time as ut
from controller.item import create_item, Item
from data_processing.npy_array_computations import avg_price_summed_volume
from global_variables.data_classes import NpyDatapoint as NpyDp
from model.database import Database

item = create_item(2)
est_vol_per_char = 0
ts = 0
min_timestamp, max_timestamp = 0, 0
t_print = 0

cols, ar = [], np.ndarray([])


class NpyDbUpdater(Database):
    """
    Class representation of the Npy Database. Used for managing Npy Array data.
    
    """
    NpyDatapoint = NpyDp
    npy_columns = NpyDatapoint.__match_args__
    array_directory = gp.dir_npy_arrays
    
    def __init__(self, db_path: str = gp.f_db_npy, source_db_path: str = gp.f_db_timeseries, new_db: bool = False,
                 item_ids: Sequence = go.npy_items, prices_listbox_path: str = gp.f_prices_listbox, execute_update: bool = True, add_arrays: bool = True, **kwargs):
        
        if new_db and os.path.exists(db_path):
            os.remove(db_path)
        
        super().__init__(path=db_path, parse_tables=False)
        self.add_npy_array_files = add_arrays
        
        if kwargs.get('dp') is not None:
            self.NpyDatapoint = kwargs.get('dp')
            if len(self.NpyDatapoint.__match_args__) < len(self.npy_columns):
                raise ValueError("dp can be passed to add additional columns, but this one seems to have less")
            self.npy_columns = self.NpyDatapoint.__match_args__

        self.sql_c = f"""CREATE TABLE ___("""
        self.sql_i = f"""INSERT OR REPLACE INTO ___{str(self.NpyDatapoint.__match_args__)}
                        VALUES {str(tuple(['?' for _ in self.NpyDatapoint.__match_args__]))}""".replace("'", "")
        self.sql_del_rows = """DELETE FROM ___ WHERE timestamp < ?"""
        self.sql_count_del = f"""SELECT COUNT(*) FROM ___ WHERE timestamp < ?"""
        self.sql_fetch_src_wiki = """SELECT ?, price, volume, MAX(timestamp) FROM ___ WHERE src=0 AND timestamp<?"""
        self.sql_fetch_src_avg5m_buy = """SELECT price, volume, ? FROM ___ WHERE src=1 AND timestamp=?"""
        self.sql_fetch_src_avg5m_sell = """SELECT price, volume, ? FROM ___ WHERE src=2 AND timestamp=?"""
        self.sql_fetch_src_rt_buy = """SELECT price FROM ___ WHERE src=3 AND timestamp BETWEEN ? AND ?+299"""
        self.sql_fetch_src_rt_sell = """SELECT price FROM ___ WHERE src=4 AND timestamp BETWEEN ? AND ?+299"""
        self.sql_fetch_src_rt = """SELECT price FROM ___ WHERE src IN (3, 4) AND timestamp BETWEEN ? AND ?+299"""
        self.sql_fetch_npy = """SELECT * FROM ___"""
        self.sql_ts_start = """SELECT MAX(timestamp) FROM ___ """
        self.sql, self.sql_keys = {}, None
        
        for col in self.NpyDatapoint.__match_args__:
            if col[:3] == 'gap' or 'coefficient' in col:
                self.sql_c += f""""{col}" REAL NOT NULL DEFAULT 0.0, """
            else:
                self.sql_c += f""""{col}" INTEGER NOT NULL DEFAULT 0, """
        self.sql_c = self.sql_c + 'PRIMARY KEY(timestamp) )'
        self.item_id, self._r = 0, ''
        
        self.src_db = Database(source_db_path, read_only=True)
        self.ts_con_avg5m = self.src_db.cursor()
        self.ts_con_avg5m.row_factory = self.timeseries_rows_factory
        self.ts_con_rt = self.src_db.cursor()
        self.ts_con_rt.row_factory = self.realtime_row_factory
        self.ts_con_wiki = self.src_db.cursor()
        self.ts_con_wiki.row_factory = self.timeseries_rows_factory_wiki
        self.con_npy = self.cursor()
        self.con_npy.row_factory = self.factory_extract_npy
        self.column_file = gp.f_npy_column
        # self.con_npy.row_factory = lambda c, row: self.NpyDatapoint(*row)
        self.con_by_src_id = [
            self.ts_con_wiki,
            self.ts_con_avg5m,
            self.ts_con_avg5m
        ]
        try:
            self.db_size_start = os.path.getsize(self.db_path)
        except FileNotFoundError:
            self.db_size_start = 0
        self.t0, self.t1, self.timestamps, self.t_start = 0, 0, [], time.perf_counter()
        self.configure_default_timestamps()
        self.item_ids = item_ids

        self.prices_listbox_path = prices_listbox_path
        if prices_listbox_path is not None:
            try:
                self.prices_listbox = uf.load(self.prices_listbox_path)
                self.updated_listbox = False
                if not isinstance(self.prices_listbox, dict):
                    self.prices_listbox = {}
                    self.updated_listbox = True
            except FileNotFoundError:
                self.prices_listbox = {}
                self.updated_listbox = True
        else:
            self.updated_listbox = False
            self.prices_listbox = None

        self.placeholder = [[0, 0]]
        print(f'\tNpy items: {len(item_ids)} Npy timespan was set to [{ut.loc_unix_dt(self.t0)} - '
              f'{ut.loc_unix_dt(self.t1)}]')
        if execute_update or new_db:
            # print(f'Created tables in {fmt.delta_t(time.perf_counter()-self.t_start)}')
            start_insert = time.perf_counter()
            self.generate_db(item_ids=self.item_ids, t0=self.t0, t1=self.t1)
            if self.add_npy_array_files:
                self.generate_remaining_arrays(tuple(self.item_ids))
            # n_done = None
            # while n_done is None or n_done > 0:
            #     n_done = self.insert_row_data(start_idx=n_done)
            tpc = time.perf_counter()
            print(f'\n\tDone! Insert time: {fmt.delta_t(tpc-start_insert)} Total runtime: {fmt.delta_t(tpc-self.t_start)}')
        self.con = None
        
    def configure_default_timestamps(self):
        """ Set up the timestamp array, given the configurations. """
        t1 = 0
        for i in go.most_traded_items:
            t1 = max(t1, self.src_db.execute(
                self.set_table_name(f"""SELECT MAX(timestamp) FROM ___ WHERE src in (1, 2)""", i),
                factory=0).fetchone())
        self.t0, self.t1 = int(t1 - t1 % 86400 - cfg.npy_db_timespan * 86400), int(t1 - t1 % 14400)
        self.timestamps = range(self.t0, self.t1, 300)
        global min_timestamp, max_timestamp
        min_timestamp, max_timestamp = self.t0, self.t1
    
    def updater_print(self, idx, n_items, n_rows, n_deleted, n_created, exe_times_item):
        """ Print a progress update while generating the database """
        n_tables = f"New tables: {n_created}  " if n_created > 0 else ""
        if len(exe_times_item) > 0:
            print(f'\t[{fmt.delta_t(time.perf_counter() - self.t_start)}] Items: {idx + 1}/{n_items}  '
                  f'Db size: +{fmt.fsize(os.path.getsize(self.db_path) - self.db_size_start)}  '
                  f'Rows [+ {n_rows} / - {n_deleted}]  {n_tables}'
                  f'Avg/item: {fmt.delta_t(sum(exe_times_item) / len(exe_times_item))}', end='\r')
        else:
            print(f'\t[{fmt.delta_t(time.perf_counter() - self.t_start)}] Items: {idx + 1}/{n_items}  '
                  f'Db size: +{fmt.fsize(os.path.getsize(self.db_path) - self.db_size_start)}  '
                  f'Rows [+ {n_rows} / - {n_deleted}]  {n_tables}', end='\r')
    
    def generate_db(self, item_ids: Sequence = go.npy_items, t0: int = None, t1: int = None):
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

        Returns
        -------
        
        
        Notes
        -----
        Time needed to complete execution depends on the amount of rows to generate, which in turn depends on the amount
        of item_ids included in the update. If a new item_id is added to the database, 288 rows are added for each day
        in the coverage. If the rows were previously added, the amount of to-be generated rows is drastically reduced.
        The time needed to generate a table and inserting it with rows is 5-10 seconds for a coverage of 60 days (17k
        rows)

        """
        print(f'\tUpdating Npy array db...')
        if t0 is None:
            t0 = self.t0
        if t1 is None:
            t1 = self.t1
        self.con = self.write_con()
        exe_times_item = []
        n_items, n_rows, n_deleted, n_created, n_skipped = len(item_ids), 0, 0, 0, 0
        for idx, item_id in enumerate(item_ids):
            item_start = time.perf_counter()
            self.update_item_id(item_id)
            global item, est_vol_per_char
            sql_i = self.sql.get('insert')
            try:
                self.generate_table()
                # print(f'Generated new table item{item_id:0>5}')
                n_created += 1
                _t0 = t0
            except sqlite3.OperationalError:
                # print(f'Removing expired rows for item {item_id}')
                n_deleted += self.delete_rows()
                _t0 = self.execute(self.sql.get("get_t0"), factory=0).fetchone()
                if _t0 is not None and _t0 >= t1-300:
                    n_skipped += 1
                    self.updater_print(idx-n_skipped, n_items-n_skipped, n_rows, n_deleted, n_created, exe_times_item)
                    self.update_prices_listbox_entry(item_id=item_id, n_rows=cfg.prices_listbox_days)
                    
                    continue
                # print(t0, _t0, t1)
                # print(self.generate_rows_(item_id, _t0, t1))
            item = create_item(item_id)
            if not isinstance(item, Item):
                raise TypeError
            try:
                item_rows = 0
                for timestamp, w_p, w_v, w_ts, b_p, b_v, s_p, s_v, rt_p in self.generate_rows(item_id, _t0, t1):
                    # n_rows += 1
                    item_rows += 1
                    # print(rt_p)
                    n_rt = len(rt_p)
                    if n_rt > 0:
                        rt_min, rt_max, rt_avg = rt_p[0], rt_p[-1], sum(rt_p) // len(rt_p)
                        tax = int(np.average(rt_p) * .01)
                        rt_margin = rt_p[-1]-rt_p[0]-tax
                    else:
                        rt_min, rt_max, rt_avg, rt_margin = 0, 0, 0, 0
                        tax = int(max(s_p, b_p, w_p) * .01)
                    avg5m_margin = s_p - b_p-tax if min(b_p, s_p) != 0 else 0
                    est_vol_per_char = min(item.buy_limit*4, w_v // 10)
                    volume_coefficient = min(item.buy_limit, w_v) / max(item.buy_limit, 1)
                    
                    dt = ut.utc_unix_dt(timestamp)
                    avg5m_price, avg5m_volume = ((b_p+s_p)//2 if min(b_p, s_p) > 0 else max(b_p, s_p)), b_v+s_v
                    try:
                        self.con.execute(sql_i, (item_id, timestamp, dt.minute, dt.hour, dt.day, dt.month, dt.year,
                                                 dt.weekday(), timestamp//3600, timestamp // 86400, timestamp // 604800,
                                                 w_ts, w_p, w_v, w_p * w_v, w_v // 288,
                                                 b_p, b_v, b_p*b_v,
                                                 s_p, s_v, s_p*s_v,
                                                 avg5m_price, avg5m_volume, avg5m_price*avg5m_volume, avg5m_margin,
                                                 (s_p-b_p)/w_p, (b_p-w_p)/w_p, (s_p-w_p)/w_p,
                                                 rt_avg, rt_min, rt_max, n_rt, rt_margin,
                                                 tax, est_vol_per_char, volume_coefficient))
                    except OverflowError as e:
                        # print(avg5m_price*avg5m_volume, w_p*w_v)
                        raise e
                    except sqlite3.ProgrammingError as e:
                        warn(RuntimeWarning(f"Insufficient amount of bindings supplied for item_id={item_id}, "
                                            f"timestamp={timestamp}. Added 0s instead."))
                        params = [item_id, timestamp, dt.minute, dt.hour, dt.day, dt.month, dt.year,
                                  dt.weekday(), timestamp//3600, timestamp // 86400, timestamp // 604800,
                                  w_ts, w_p, w_v, w_p * w_v, w_v // 288,
                                  b_p, b_v, b_p*b_v,
                                  s_p, s_v, s_p*s_v,
                                  avg5m_price, avg5m_volume, avg5m_price*avg5m_volume, avg5m_margin,
                                  (s_p-b_p)/w_p, (b_p-w_p)/w_p, (s_p-w_p)/w_p,
                                  rt_avg, rt_min, rt_max, n_rt, rt_margin,
                                  tax, est_vol_per_char, volume_coefficient]
                        while len(params) < len(self.npy_columns):
                            params.append(0)
                        self.con.execute(sql_i, tuple(params))
                        if self.prices_listbox is not None:
                            self.update_prices_listbox_entry(item_id=item_id, n_rows=cfg.prices_listbox_days)
                            self.updated_listbox = True
                if item_rows > 0:
                    self.con.commit()
                    self.save_prices_listbox()
                    if self.add_npy_array_files:
                        self.generate_npy_array(item_id)
                    exe_times_item.append(time.perf_counter()-item_start)
                    n_rows += item_rows
            except OSError:
                if len(exe_times_item) % 10 > 1:
                    continue
            self.updater_print(idx-n_skipped, n_items-n_skipped, n_rows, n_deleted, n_created, exe_times_item)
        self.save_prices_listbox()
            # for k, v in self.prices_listbox.items():
            #     print(k, v)
            
    def update_item_id(self, item_id: int):
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
        global item
        if self.item_id != item_id:
            # print(f'Setting item_id to {item_id}                ')
            # time.sleep(2)
            item = create_item(item_id)
            self._r, self.item_id = ('___', f'"item{item_id:0>5}"'), item_id
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
            # for k, _s in self.sql.items():
            #     print(_s)
            if self.sql_keys is None:
                self.sql_keys = tuple(self.sql.keys())
            # print(self.sql['insert'])
            # exit(1)
            # print(self.sql.get('fetch_wiki'))
    
    def save_prices_listbox(self):
        if self.prices_listbox_path is not None and self.updated_listbox:
            uf.save(self.prices_listbox, self.prices_listbox_path)
            self.updated_listbox = False
    
    def generate_rows(self, item_id: int = None, t0: int = None, t1: int = None):
        """ Generate rows for `item_id` with timestamps spanning from `t0` to `t1` """
        if item_id is not None:
            self.update_item_id(item_id)
        return [list(self.get_src_data(0, timestamp, item_id)) +
                list(self.get_src_data(1, timestamp, item_id)) +
                list(self.get_src_data(2, timestamp, item_id)) +
                [self.get_src_data(5, timestamp, item_id)]
                for timestamp in range(self.t0 if t0 is None else t0, (self.t1 if t1 is None else t1), 300)]
        
    def get_src_data(self, src: int, timestamp: int, item_id: int = None):
        """
        Get data from source `src` from the currently configured item_id or set config to `item_id`, if specified.````
        The following sources have been mapped to `src`s;
        0: wiki, 1: avg5m_buy, 2: avg5m_sell, 3: realtime_buy, 4: realtime_sell, 5: realtime_merged.
        Src 0-2 yield a price and volume, while src 3-5 yield a list of prices applicable for `timestamp`
        
        Parameters
        ----------
        src : int
            The source of the data to fetch
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
        if item_id is not None:
            self.update_item_id(item_id)
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
        
    def generate_table(self, item_id: int = None):
        """ Generate a table for `item_id` """
        if item_id is not None:
            self.update_item_id(item_id)
        self.con.execute(self.sql.get('create'))
    
    def delete_rows(self, item_id: int = None, t0: int = None) -> int:
        """ Delete rows with a timestamp smaller than `t0` or self.t0. Return the amount of deleted rows. """
        if item_id is not None:
            self.update_item_id(item_id)
        parameters = (self.t0 if t0 is None else t0,)
        n_rows = self.execute(self.sql.get('count_del').replace(*self._r), parameters, factory=0).fetchone()
        self.con.execute(self.sql.get('del_rows').replace(*self._r), parameters)
        self.con.commit()
        return n_rows
    
    def generate_npy_array(self, item_id: int, overwrite: bool = True, generate_csv: bool = False):
        """ Generate arrays with data of `item_id` and export them to a npy file in the array directory. """
        if not os.path.exists(self.array_directory):
            raise FileNotFoundError(f"Unable to export array files to non-existent directory {self.array_directory}")
        if not overwrite and os.path.exists(self.array_file(item_id)):
            return
        self.update_item_id(item_id=item_id)
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
            if not os.path.exists(self.column_file) or time.time()-os.path.getmtime(self.column_file) > 3600:
                uf.save(cols, self.column_file)
            self.save_arrays(ar, cols, item_id)
    
    def array_file(self, item_id):
        """ Return the path to the array file with data for `item_id` """
        return f'{self.array_directory}{item_id:0>5}.npy'
        
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
        print(f'\nRemaining arrays were generated in {fmt.delta_t(time.perf_counter()-t_ar_gen)}')
    
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
            print(f'[{fmt.delta_t(time.perf_counter()-t_start)}] {n_done}/{n_total} done '
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
    def generate_template_db(path: str = gp.dir_template+'npy.db'):
        """ Generate a template db+npy file, which has one table, to get an impression of what the db looks like. """
        dp = namedtuple('NpyDatapoint', list(NpyDbUpdater.NpyDatapoint.__match_args__) + ['src', 'price', 'volume'])
        
        if os.path.exists(path) and os.path.getsize(path) > pow(10, 7):
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

    def update_prices_listbox_entry(self, item_id: int, n_rows: int = cfg.prices_listbox_days, n_intervals: int = 6):
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
        n_rows : int, optional, global_variables.configurations.prices_listbox_days by default
            Amount of rows to produce as output, where each row corresponds to a 24 hour timespan.
        n_intervals : int, optional, 6 by default
            Amount of smaller, equally sized intervals to divide each 24-hour timespan in
    
        Notes
        -----
        The tags are encoded as b_X_Y, with b referring to buy price, X referring to the first hour of the interval and
        Y referring to the final hour of the interval (UTC times). In the GUI, they are converted to local time rather
        than UTC time. E.g. b_4_8 refers to the buy price in the interval of 4am-8am.
        Sell prices are shown as the last interval for that row and the average across 3/6 highest sell prices across
        all intervals.
        """
        
        if self.prices_listbox is None:
            return
        interval_start = self.execute(f"SELECT MAX(timestamp) FROM item{item_id:0>5}", factory=0).fetchone()-86100
        interval_size = int(24 / n_intervals)*3600
        
        # Fix for missing entries (would otherwise
        if interval_start % interval_size != 0:
            interval_start = interval_start + 14400 - interval_start % 14400
            
        buy_tags = {0: 'b_0_4', 4: 'b_4_8', 8: 'b_8_12', 12: 'b_12_16', 16: 'b_16_20', 20: 'b_20_24'}
        rows = []
        interval_step = 288 // n_intervals
        
        for day_idx in range(n_rows):
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
                                                 (interval_start-86400,interval_start), factory=0).fetchone())
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


if __name__ == '__main__':
    # NpyDbUpdater()
    
    ...
