"""
Executable module that is used to fill prices from src=1, 2 that are 0 for some reason.

"""

import sqlite3
import time
from collections.abc import Iterable

import backend.download as dl
import global_variables.osrs as go
import global_variables.path as gp
import global_variables.values as val
import util.array as u_ar
import util.file as uf
import util.str_formats as fmt
from file.file import File
from model.database import Database

shutdown = False
db_size_start = None
last_print = ''
n_inserts = 0
t_commit = time.perf_counter()
commit_frequency = 20


def commit_data(db: sqlite3.Connection, t_commit: int, done: Iterable, to_do: Iterable, progress_file: str = None):
    """ Every `commit_frequency` seconds, commit rows to db and save the progress made in `progress_file` """
    if time.perf_counter() > t_commit:
        db.commit()
        to_do = u_ar.unique_values(frozenset(to_do).difference(done), return_type=list, sort_ascending=True)
        if progress_file is not None:
            uf.save(to_do, path=progress_file)
        return to_do, [], int(time.perf_counter() + commit_frequency)
    else:
        return to_do, done, t_commit


def fill_avg5m_zero_prices(db_file: File = gp.f_db_timeseries, cooldown: float = 3.0, commit_frequency_sec: int = 20,
                           item_ids: Iterable = go.most_traded_items, lb_ts: int = val.min_avg5m_ts_query_online,
                           ub_ts: int = time.time(), skip_ids: Iterable = go.timeseries_skip_ids, progress_file: File = File(gp.dir_temp+'fill_avg5m_to_do.dat')):
    """
    Iterate over all items, collect timestamps from src=1 or 2 where price=0 and try to fill the missing data.
    
    Parameters
    ----------
    db_file : str
        Path to the timeseries database
    cooldown : int, optional, 3.0 by default
        Ensures each iteration takes at least this many seconds. Used to limit the amount of requests made.
    commit_frequency_sec : int, optional, 20 by default
        Frequency in seconds for which to commit the submitted rows to the database.
    item_ids : int, optional, global_variables.osrs.most_traded_items by default
        Item_id to use as reference when gathering a list of timestamps. If set to None, iterate over all item_ids.
    lb_ts : int, optional, global_variables.values.min_avg5m_ts_query_online by default
        Lower bound timestamp; do not include timestamps smaller than `min_ts`. Is set to absolute minimum by default.
    ub_ts : int, optional, None by default
        Upper bound timestamp; exclude timestamps larger than `max_ts`. Should not exceed the largest timestamp in db.
    skip_ids : Iterable, optional, global_variables.osrs.timeseries_skip by default
        A list of item_ids that should not be included whatsoever
    progress_file : str, optional, global_variables.path.dir_temp+'fill_avg5m_to_do.dat'
        If a file exists at this path, load its contents and resume the list. If the process is interrupted via a
        KeyboardInterrupt, the to-do list minus the processed timestamps is saved at this path.
    Returns
    -------

    """
    global shutdown, db_size_start, last_print, n_inserts, t_commit
    t_commit = time.perf_counter() + commit_frequency_sec
    db = Database(db_file)
    db_size_start = db_file.fsize()
    
    if ub_ts is None:
        ub_ts = db.execute("""SELECT MAX(timestamp) FROM 'item00002' WHERE src IN (1, 2)""").fetchone()
    
    # Exclude skip_ids from item_ids
    item_ids = list(frozenset(item_ids).difference(skip_ids))
    item_ids.sort()
    ts_done = []
    start = time.perf_counter()
    # rows = db.execute("""SELECT B.timestamp, B.price, B.volume, S.price, S.volume FROM 'item10' AS B, 'item10' AS S WHERE B.src+1=S.src AND S.timestamp = B.timestamp""", factory=tuple).fetchall()
    # new_ts = time.time()
    
    i, to_do, loaded_file = None, [], False
    p = (lb_ts, ub_ts)
    if progress_file.exists():
        try:
            to_do = progress_file.load()
            loaded_file = True
            print(f'Resuming from loaded file at {progress_file}')
        except EOFError:
            print(f'EOF Error while trying to load existing progress file at {progress_file}')
    
    if not loaded_file:
        n = len(item_ids)
        print('Gathering timestamps. If you wish to proceed with the current list, press CTRL+C.\n')
        for idx, i in enumerate(item_ids):
            try:
                print(f'\tProgress: {idx+1}/{n} timestamps found: {len(to_do)}          ', end='\r')
                to_do += db.execute(
                    f"""SELECT DISTINCT timestamp FROM 'item{i:0>5}' WHERE price=0 AND src IN (1, 2) AND timestamp BETWEEN ? AND ? AND timestamp NOT IN {str(tuple(to_do))}""",
                    p, factory=0).fetchall()
                # if idx % 50 == 49:
                #     to_do = u_ar.unique_values(to_do, list)
            except KeyboardInterrupt:
                break
        to_do = u_ar.unique_values(_set=to_do, sort_ascending=True, return_type=tuple)
        progress_file.save(to_do)
    n = len(to_do)
    print(f"\n  * Gathered {n} different timestamps")

    sell_keys, buy_keys = ['avgHighPrice', 'highPriceVolume'], ['avgLowPrice', 'lowPriceVolume']
    t_commit = time.perf_counter() + commit_frequency_sec
    _, timestamps_done = '            ', []
    try:
        for idx, ts in enumerate(to_do):
            timestamps_done.append(ts)
            t_ = time.perf_counter()
            data = dl.download_wiki_prices_rt_averaged(timestamp=int(ts))[0]
            for i in item_ids:
                el = data.get(str(i))
                _insert = f"""INSERT OR REPLACE INTO 'item{i:0>5}'(src, timestamp, price, volume) VALUES (?, ?, ?, ?)"""
                for src, keys in zip((1, 2), (buy_keys, sell_keys)):
                    try:
                        price, volume = [el.get(k) for k in keys]
                        if price > 0:
                            db.execute(_insert, (src, ts, price, volume))
                            
                            n_inserts += 1
                    except AttributeError:
                        ...
                    except TypeError:
                        ...
            tpc = time.perf_counter()
            last_print = f" Timestamp #{idx+1}/{n}  {ts} {fmt.unix_(ts)}  N_inserts: {n_inserts}  " \
                         f"File: +{fmt.fsize(db_file.fsize() - db_size_start)}{_}"
            print(last_print, end='\r')
            to_do, done, t_commit = commit_data(db, t_commit, ts_done, to_do, progress_file)
            
            time.sleep(max(.1, cooldown - (tpc - t_)))
    except KeyboardInterrupt:
        # Process was manually interrupted; save progress to `progress_file`
        progress_file.save(u_ar.unique_values(frozenset(to_do).difference(ts_done), return_type=tuple, sort_ascending=True))
        shutdown = True
    finally:
        print('\n\n')
        ts_done += timestamps_done
        db.commit()
        
        db.close()
        
        # To-do list was fully completed and a previously created file exists
        if not shutdown and progress_file.exists():
            progress_file.delete()
        print(f'Done! Runtime: {fmt.delta_t(time.perf_counter() - start)} | Inserts: {n_inserts} | '
                  f'Db file size: +{fmt.fsize(db_file.fsize()-db_size_start)}')
        _ = input('Press ENTER to close')
        return
        

def fill_missing_avg5m(db_file: File = gp.f_db_timeseries, cooldown: float = 3.0, commit_frequency_sec: int = 20,
                       ref_id: int = go.most_traded_items, lb_ts: int = val.min_avg5m_ts, ub_ts: int = None,
                       skip_ids: Iterable = go.timeseries_skip_ids, fill_zeros: bool = True):
    """
    Ensure that the timeseries data from

    Parameters
    ----------
    db_file : str
        Path to the timeseries database
    cooldown : int, optional, 3.0 by default
        Ensures each iteration takes at least this many seconds. Used to limit the amount of requests made.
    commit_frequency_sec : int, optional, 20 by default
        Frequency in seconds for which to commit the submitted rows to the database.
    ref_id : int, optional, global_variables.osrs.most_traded_items by default
        Item_id to use as reference when gathering a list of timestamps. If set to None, iterate over all item_ids.
    lb_ts : int, optional, 0 by default
        Lower bound timestamp; do not include timestamps smaller than `min_ts`
    ub_ts : int, optional, time.time() by default
        Upper bound timestamp; do not include timestamps larger than `max_ts`
    skip_ids : Iterable, optional, global_variables.osrs.timeseries_skip by default
        A list of item_ids that should not be included whatsoever
    fill_zeros : bool, optional, True by default
        If True, restrict the timestamps to prices that are equal to 0 for item_ids passed as `ref_id`. If False,
        include missing timestamps as well (i.e. all timestamps between `lb_ts`, `ub_ts` where ts % 300 == 0).
            Although it is possible to scrape an exhaustive list of prices, this would render the scraper obsolete
    """
    global db_size_start
    if db_size_start is None:
        db_size_start = db_file.fsize()
    
    db = Database(db_file, read_only=False)
    
    if ub_ts is None:
        ub_ts = 0
        for i in go.most_traded_items:
            ub_ts = max(ub_ts, db.execute(f"""SELECT MAX(timestamp) FROM 'item{i:0>5}' WHERE src in (1, 2)""", factory=0).fetchone())
    start = time.perf_counter()
    # rows = db.execute("""SELECT B.timestamp, B.price, B.volume, S.price, S.volume FROM 'item10' AS B, 'item10' AS S WHERE B.src+1=S.src AND S.timestamp = B.timestamp""", factory=tuple).fetchall()
    # new_ts = time.time()
    item_ids = frozenset(go.item_ids).difference(skip_ids)
    if ref_id is None:
        ref_id = item_ids
    elif isinstance(ref_id, int):
        ref_id = [ref_id]
    
    upper_bound = db.execute(f"SELECT MAX(timestamp) FROM 'item{ref_id[0]:0>5}'", factory=0).fetchone()
    
    # to_do = u_ar.unique_values(to_do, tuple, True)
    to_do = list(range(lb_ts, ub_ts+1, 300))
    to_do.sort()
    
    sell_keys, buy_keys = ['avgHighPrice', 'highPriceVolume'], ['avgLowPrice', 'lowPriceVolume']
    n_ts = len(to_do)
    n_inserts, t_commit = 0, time.perf_counter() + commit_frequency_sec
    _ = '            '
    sql = """SELECT price, volume FROM item00002 WHERE src IN (1, 2) AND timestamp=? """
    # to_do = []
    min_qt = time.perf_counter()
    ins = """INSERT OR REPLACE INTO item___(src, timestamp, price, volume) VALUES (?, ?, ?, ?)"""
    try:
        for idx, ts in enumerate(to_do):
            prices = db.execute(sql, (ts,)).fetchall()
            tpc = time.perf_counter()
            if len(prices) == 0 and len(db.execute(sql.replace('00002', '12934'), (ts,)).fetchall()) == 0:
                if tpc < min_qt:
                    time.sleep(max(.1, min_qt-tpc))
                    min_qt = time.perf_counter()+2
                data = dl.download_wiki_prices_rt_averaged(timestamp=ts)[0]
                
                for item_id, _d in data.items():
                    _s = ins.replace('___', f'{item_id:0>5}')
                    
                    if _d.get('lowPriceVolume') > 0:
                        db.execute(_s, (1, ts, _d.get('avgLowPrice'), _d.get('lowPriceVolume')))
                        n_inserts += 1
                    if _d.get('highPriceVolume') > 0:
                        db.execute(_s, (2, ts, _d.get('avgHighPrice'), _d.get('highPriceVolume')))
                        n_inserts += 1
            print(f"{idx}/{n_ts}  Timestamp: {fmt.unix_(ts)}  N_inserts: {n_inserts}  File: +{fmt.fsize(db_file.fsize()-db_size_start)}{_}",
                  end='\r')
            if tpc > t_commit:
                db.commit()
                t_commit = tpc + commit_frequency_sec
    finally:
        db.commit()
        db.close()
        print(f'Done! Runtime: {fmt.delta_t(time.perf_counter() - start)} | '
              f'Db file size: {fmt.fsize(db_file.fsize()-db_size_start)}')
        _ = input('Press ENTER to close')
        ...


if __name__ == '__main__':
    t1 = int(time.time())
    fill_missing_avg5m(lb_ts=1719409800, ub_ts=t1-t1%14400)
    # fill_missing_avg5m(lb_ts=val.min_avg5m_ts_query_online)
    
    exit(1)
    db = Database(gp.f_db_timeseries)
    item_ids = list(frozenset(go.item_ids).difference(go.timeseries_skip_ids))
    item_ids.sort()
    upper_bound = db.execute('SELECT MAX(timestamp) FROM item00002 WHERE src in (1, 2)', factory=0).fetchone()
    print(upper_bound)
    print(fmt.unix_(1617178800))
    timestamps_all = tuple(range(val.min_avg5m_ts, upper_bound+1, 300))
    parameters = (val.min_avg5m_ts, upper_bound)
    for i in tuple(item_ids):
        for src in (1, 2):
            to_do = frozenset(timestamps_all).difference(
                db.execute(f"""SELECT timestamp FROM 'item{i:0>5}' WHERE src={src} AND timestamp BETWEEN ? AND ?""", parameters, factory=0).fetchall())
            print(i, src)
            to_do = list(to_do)
            to_do.sort()
            print(to_do[-100:])
            print(len(to_do))
            print(dl.download_wiki_prices_rt_averaged(timestamp=1716900600))
            exit(1)
