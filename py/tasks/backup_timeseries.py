"""
This module contains methods for backing up the timeseries database data
An alternative that is likely to be preferable is VACUUM INTO, which performs the VACUUM command on a different target,
thus creating a backup as a much faster rate.

The backup protocol listed here is useful for re-redesigning the database, though.
All item data is exported to one file per source per item.


"""
import pickle
import sqlite3
import threading
from collections.abc import Iterable
from typing import List

from venv_auto_loader.active_venv import *
import global_variables.osrs as go
import global_variables.path as gp
import util.file as uf
import util.sql as sql
import util.str_formats as fmt
from file.file import File
from common import Database
from model.timeseries import TimeseriesDB
__t0__ = time.perf_counter()


select_list = [
    "SELECT 0 AS src, timestamp, price, volume FROM 'wiki' WHERE item_id=? AND timestamp < ?",
    "SELECT 1 AS src, timestamp, buy_price AS price, buy_volume AS volume FROM 'avg5m' WHERE item_id=? AND timestamp < ? ",
    "SELECT 2 AS src, timestamp, sell_price AS price, sell_volume AS volume FROM 'avg5m' WHERE item_id=? AND timestamp < ? ",
    "SELECT 3 AS src, timestamp, price, 0 FROM 'realtime' WHERE item_id=? AND timestamp < ? AND is_buy=1 ",
    "SELECT 4 AS src, timestamp, price, 0 FROM 'realtime' WHERE item_id=? AND timestamp < ? AND is_buy=0 ",
]


def async_extract_rows(db: str, item_ids: list, backup_dir: str = gp.dir_timeseries_backup,
                       min_modified_time: float = time.time()-86400, srcs: tuple = None):
    """
    Extract all rows for item_ids in `item_ids`, and save a pickled version of the parsed rows in `backup_dir`.
    Row exports are bundled by individual source and item_id. This task is by design part of a threaded task.
    
    Parameters
    ----------
    db : str
        Path to the source database file
    item_ids : list
        List of item_ids to backup.
    backup_dir : str, optional, global_variables.path.dir_timeseries_backup
    min_modified_time : float, optional, time.time()-86400 by default
        Consider files that would have been exported and were modified less than `min_modified_time` ago part of the
        'already done' list. Used to prevent doing the same task multiple times.
    srcs : tuple, optional, None by default
        The sources to include in the backup process. By default, include all.

    Returns
    -------

    """
    db = Database(db)
    if item_ids is None:
        item_ids = [int(i[4:]) for i in sql.get_db_contents(c=db.cursor(), get_indices=False)[0] if i[:4] == 'item']
    
    for item_idx, cur_id in enumerate(item_ids):
        select = f"SELECT * FROM 'item{cur_id:0>5}' WHERE src=?"
        try:
            
            for src, table in enumerate(['wiki', 'avg5m', 'avg5m', 'realtime', 'realtime']):
                if srcs is not None and src not in srcs:
                    continue
                out_file = f'{backup_dir}{src}/{cur_id:0>5}.dat'
                if os.path.exists(out_file) and os.path.getmtime(out_file) > min_modified_time:
                    try:
                        _ = uf.load(out_file)[0]
                        continue
                    except pickle.UnpicklingError:
                        os.remove(out_file)
                    except EOFError:
                        os.remove(out_file)
                    except TypeError:
                        os.remove(out_file)
                    
                uf.save(db.execute(select, (src,), factory=tuple).fetchall(), path=out_file)
                # print(f'[{fmt.unix_(time.time())}] {cur_id:0>5}_src_{src}', end='\r')
        except Exception as e:
            print(str(e), f'@ item_id={cur_id}')


def async_extract_rows_old(db_file: str, item_ids: list = None, backup_dir: str = gp.dir_timeseries_backup,
                       min_modified_time: float = time.time()-86400, srcs: tuple = None):
    """
    Extract all rows for item_ids in `item_ids`, and save a pickled version of the parsed rows in `backup_dir`.
    
    Parameters
    ----------
    db :
    item_ids :
    backup_dir :
    min_modified_time :

    Returns
    -------

    """
    import global_variables.values as val
    
    db = Database(db_file, read_only=True)
    
    step = 1500000
    ts = 1717720000
    for item_idx, cur_id in enumerate(item_ids):
        try:
            
            for src, table in enumerate(['wiki', 'avg5m', 'avg5m', 'realtime', 'realtime']):
                if srcs is not None and src not in srcs:
                    continue
                out_file = f'{backup_dir}{src}/{cur_id:0>5}.dat'
                # print(out_file)
                if os.path.exists(out_file) and os.path.getmtime(out_file) > min_modified_time:
                    try:
                        uf.load(out_file)
                        continue
                    except pickle.UnpicklingError:
                        os.remove(out_file)
                    except EOFError:
                        os.remove(out_file)
                t1 = ts
                rows = []
                while t1 > val.min_ts_wiki and src == 0 or t1 > val.min_avg5m_ts and src > 0:
                    t0 = t1 - step
                    
                    rows += db.execute(select_list[src], (cur_id, t0, t1), factory=tuple).fetchall()
                    t1 -= step
                    # print(cur_id, t0, t1)
                uf.save(rows, path=out_file)
                print(f'[{fmt.unix_(time.time())}] {cur_id:0>5}_src_{src}', end='\r')
        except WindowsError as e:
            print(str(e), f'@ item_id={cur_id}')
            # ...


rt_start = time.perf_counter()


def print_msg(folders: Iterable, min_ts: int, n_to_do: int):
    progress = 0
    for f in folders:
        # print(min_ts, min([os.path.getmtime(file) for file in uf.get_files(src=f, full_path=True) if os.path.getmtime(file) >= min_ts]))
        progress += len([os.path.getmtime(file) for file in uf.get_files(src=f, full_path=True) if
                         os.path.getmtime(file) >= min_ts])
    print(
        f' [{fmt.delta_t(time.perf_counter() - rt_start)}] {progress} / {n_to_do} ({progress / n_to_do * 100:.1f}%) done',
        end='\r')
    return progress


class AsyncPrinter(threading.Thread):
    def __init__(self, folders: Iterable, start_time: int, n_to_do: int, **kwargs):
        threading.Thread.__init__(self, name=kwargs.get('name'), daemon=kwargs.get('daemon'))
        # self.task = task
        self.n_total = n_to_do
        self.start_time = start_time
        self.folders = folders
    
    def run(self):
        while print_msg(self.folders, self.start_time, self.n_total) < self.n_total:
            time.sleep(5)
            if time.perf_counter() - rt_start > 240 and min(
                    [time.time() - os.path.getmtime(f) for f in uf.get_files(self.folders[4])]) > 240:
                break
        # input('done')
        return True


class ExtractRowAsyncTask(threading.Thread):
    def __init__(self, db_file: File, backup_dir: str, callback_oncomplete: callable = None, item_ids: list = None, old: bool = False, **kwargs):
        threading.Thread.__init__(self, name=kwargs.get('name'), daemon=kwargs.get('daemon'))
        # self.task = task
        self.on_complete = callback_oncomplete
        self.string = ''
        self.item_ids = item_ids
        self.backup_dir = backup_dir
        self.db_file = db_file
        self.srcs = kwargs.get('srcs')
        self.old = old
    
    def run(self):
        if self.old:
            async_extract_rows_old(db_file=self.db_file, item_ids=self.item_ids, backup_dir=self.backup_dir, srcs=self.srcs)
        else:
            async_extract_rows(db=self.db_file, item_ids=self.item_ids, backup_dir=self.backup_dir)


def backup_item_data(db: Database, item_id: int, srcs: Iterable, backup_directory: str):

    for src in srcs:
        uf.save(db.execute(f"SELECT * FROM 'item{item_id}' WHERE src=?", (src,)).fetchall(),
                path=f'{backup_directory}{src}/{item_id:0>5}.dat')


def backup_db(db_file: File, backup_directory: str, n_threads: int = 4, item_ids: list = go.item_ids, srcs: Iterable = None,
              max_mtime: int = 86400, **kwargs):
    """ Create a pickled backup of the database at `db_file`. Export rows to `backup_directory`
    Parameters
    ----------
    db_file : str
        Path to the database
    backup_directory : str
        Path to the backup folder
    n_threads : int, optional, 4 by default
        Amount of threads to use
    item_ids : list, optional global_variables.osrs.item_ids by default
        Full list of item_ids to backup.
    max_mtime: int, optional, 86400 by default
        Exclude items from the to-do list if its backup file was modified less than `max_mtime` seconds ago

    Returns
    -------

    """
    db = Database(db_file, read_only=True, parse_tables=False)
    if srcs is None:
        srcs = db.execute("SELECT DISTINCT src FROM 'item00002'", factory=0).fetchall()
    if not os.path.exists(backup_directory):
        print(f' Creating {backup_directory}')
        os.mkdir(backup_directory)
    for src in srcs:
        _dir = backup_directory+f'{src}/'
        if not os.path.exists(_dir):
            print(f' Creating {_dir}')
            os.mkdir(_dir)
    _ids = []
    for i in item_ids:
        try:
            if not os.path.exists(f'{backup_directory}{srcs[-1]}/{i:0>5}.dat') or \
                    time.time()-os.path.getmtime(f'{backup_directory}{srcs[-1]}/{i:0>5}.dat') > max_mtime:
                _ids.append(i)
        except FileNotFoundError:
            _ids.append(i)
    # async_extract_rows(db_file=gp.f_db_timeseries_, item_ids=item_ids)
    active_threads = []
    uf.save(db.execute("SELECT * FROM item00002 WHERE src=0", factory=dict).fetchone(),
            backup_directory+'example_row.dat')
    for thread_id in range(n_threads):
        t = ExtractRowAsyncTask(db_file=db_file,
                                backup_dir=backup_directory,
                                item_ids=[i for idx, i in enumerate(_ids) if idx % n_threads == thread_id],
                                old=False, **kwargs)
        active_threads.append(t)
    
    for t in active_threads:
        t.run()


def create_backup(db: str or TimeseriesDB = gp.f_db_timeseries, backup_directory: str = gp.dir_timeseries_backup,
                  item_ids: Iterable = None, n_threads: int = 4):
    """
    Create a backup of the timeseries database. Data for each source for each item is exported to a separate file in.
    Creating the backups is done via threads; the item_id list is divided into N equal parts and processed
    asynchronously.

    Parameters
    ----------
    db : str or Database
        Path to a sqlite database or a Database
    item_ids : Iterable, optional, None by default
        List of item_ids to export. If set to None, derive the full list from the table names.
    n_threads : int, optional, 4 by default
        Amount of threads to run for exporting item data

    See Also
    --------
    restore_backup()
        The exported data can be restored via restore_backup()

    """
    
    if isinstance(db, str):
        db = TimeseriesDB(db)
    
    if not os.path.exists(backup_directory):
        raise FileNotFoundError(f"backup_directory {backup_directory} does not exist")
    else:
        src_list = db.execute("SELECT DISTINCT src FROM item00002", factory=0).fetchall()
        for src in src_list:
            src_dir = backup_directory + f'{src}/'
            if not os.path.exists(src_dir):
                print(f'Creating directory', src_dir)
                os.mkdir(src_dir)
    
    if item_ids is None:
        item_ids = [int(i[4:]) for i in
                    list(sql.get_db_contents(c=db.cursor(), get_tables=True, get_indices=False)[0].keys()) if
                    i[:4] == 'item']
    
    threads = []
    for thread_id in range(n_threads + 1):
        if thread_id != n_threads:
            _thread = ExtractRowAsyncTask(db.path, backup_dir=backup_directory,
                                          item_ids=[i for idx, i in enumerate(item_ids) if idx % n_threads == thread_id])
        else:
            global start_time
            _thread = AsyncPrinter([f'{backup_directory}{src}/' for src in src_list], int(time.time() - 20),
                                   len(item_ids) * 5)
        threads.append(_thread)
    for idx, t in enumerate(threads):
        t.start()


def restore_backup(db: TimeseriesDB, backup_dir: str = gp.dir_timeseries_backup, item_ids: Iterable = None,
                   srcs: Iterable = None, t0: int = None, t1: int = None, replace: bool = True) -> List[tuple]:
    """
    Restore backup data stored in `backup_dir`
    
    Parameters
    ----------
    db: str or Database
        Path to the database or an instance of Database
    backup_dir : str, optional, global_variables.path.dir_timeseries_backup
        Root folder of the backup
    item_ids : Iterable, optional, None by default
        A list of item_ids to import data for. If undefined, include all item_ids encountered.
    srcs : Iterable, optional, None by default.
        A list of source ids to include in the backup restoration process. If undefined, include all encountered
    t0 : int, optional, None by default
        Lower bound timestamp; omit entries with a timestamp lower than this value
    t1 : int, optional, None by default
        Upper bound timestamp; omit entries with a timestamp that exceed this value
    replace : bool, optional, True by default
        If True, execute INSERT OR REPLACE instead of just INSERT, allowing rows to be overwritten.
        
    Returns
    -------
    List[tuple]
        A list with failed attempts to submit data

    """
    if isinstance(db, str):
        db = TimeseriesDB(db)
    
    
    if item_ids is None:
        item_ids = [int(i[4:]) for i in list(sql.get_db_contents(db.cursor(), get_tables=True, get_indices=False)[0].keys()) if i[:4] == 'item']
    
    if srcs is None:
        srcs = db.execute('SELECT DISTINCT src, MAX(timestamp) FROM item00002', factory=0).fetchall()
        
    if t0 is None and t1 is None:
        def include_row(*_):
            return True
    elif t1 is None:
        def include_row(row):
            return row[0] >= t0
    else:
        def include_row(row):
            return t1 >= row[0] >= t0
        
    failed = []
    for src in srcs:
        cd = backup_dir+f'{src}/'
        
        for item_id in item_ids:
            
            prefix = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
            sql_i = f"{prefix} 'item{item_id:0>5}'(src, timestamp, price, volume) VALUES ({src}, ?, ?, ?)"

            try:
                rows = [el for el in uf.load(cd+f'{item_id:0>5}.dat') if include_row(el)]
            except FileNotFoundError:
                failed.append((404, src, item_id))
                continue
                
            for row_id, row in enumerate(rows):
                try:
                    db.execute(sql_i, row)
                except sqlite3.IntegrityError:
                    failed.append((-1, src, item_id, row_id))
    return failed
    

def export_rows_db_old(db_file: str, backup_dir: str, item_ids: list = go.item_ids):
    # print(l)
    # i = 1335
    ars = [[], [], [], []]
    for idx, i in enumerate(item_ids):
        ars[idx % 4].append(i)
    a = ExtractRowAsyncTask(item_ids=ars[0], db_file=db_file, backup_dir=backup_dir)
    a.start()
    b = ExtractRowAsyncTask(item_ids=ars[1], db_file=db_file, backup_dir=backup_dir)
    b.start()
    c = ExtractRowAsyncTask(item_ids=ars[2], db_file=db_file, backup_dir=backup_dir)
    c.start()
    d = ExtractRowAsyncTask(item_ids=ars[3], db_file=db_file, backup_dir=backup_dir)
    d.start()
    

if __name__ == '__main__':
    # os.mkdir(gp.dir_data+'backup_test/')
    create_backup(backup_directory=gp.dir_data+'backup_test/')