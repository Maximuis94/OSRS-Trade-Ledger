# import model.item
import os.path

import pandas as pd
import sqlite3
from typing import Dict

from common.classes.database import Database
from global_variables.importer import *
from item.db_entity import Item


# threaded_task = AsyncTask()

def db_size_updater():
    db_file = gp.f_db_timeseries
    to_file = gp.f_db_timeseries[:-3] + '_.db'
    size_start = os.path.getsize(db_file)
    t0= int(time.time())
    _t = time.time()
    time.sleep(10)
    size_start_string = f'{size_start/pow(10,9):.2f}gb'
    return_str = ''
    mtime = _t
    while _t-t0 < 900 or time.time()-mtime < 180:
        string = None
        try:
            mtime = os.path.getmtime(to_file)
            cur_size = os.path.getsize(to_file)/pow(10,9)
            # cur_size = os.path.getsize(db_file)/pow(10,9)*.23
            return_str = f'{fmt.passed_time(_t)}\tdb_to size: {cur_size:.2f}gb initial db size: {size_start_string}'
            string = return_str
        except OSError:
            pass
        except FileNotFoundError:
            pass
        finally:
            if string is not None:
                print(string, end='\r')
            else:
                print('string is None...')
            time.sleep(7)
    return return_str

def updater_completed():
    print('Database updater is shutting down...')


def remove_rows(id_threshold: int, db_path: str):
    # db = sqlite3.connect(gp.f_db_timeseries)
    # db_to = Database(gp.dir_data+'timeseries_restructured.db', read_only=True)
    # to_remove = []
    # # for s in [f'item{item_id}' for item_id in go.item_ids if db_to.execute(f"SELECT COUNT(*) FROM item{item_id}", factory=0).fetchone() > 0]:
    # for item_id in go.item_ids:
    #     if db_to.execute(f"SELECT COUNT(*) FROM item{item_id}", factory=0).fetchone() > 0:
    #         to_remove.append((item_id,))
    #     else:
    #         db_to.close()
    #         break
    # print(to_remove)
    # print(len(to_remove))
    # if input('Would you like to proceed? "y" to confirm ').lower() != 'y':
    #     return
    t_ = time.perf_counter()
    db = sqlite3.connect(db_path)
    item_ids = [i for i in go.item_ids if i <= 1307]
    n = len(item_ids)
    intervals = [(1615000000, 1616699999)]
    t0 = intervals[0][1]
    step = 749999+1
    while t0 < time.time():
        # print(fmt.unix_(t0), fmt.unix_(t0+step-1))
        intervals.append((t0, t0+step-1))
        t0 += step
    # for t0, t1 in intervals:
    #     print(fmt.unix_(t0), fmt.unix_(t1))
    n=len(intervals)
    ts = int(time.time()-(time.time()-1616688900)//2)
    t_commit = time.perf_counter() + 15
    # for idx, i in enumerate(item_ids):
    to_do = [int(f[-9:-4]) for f in uf.get_files(src=gp.dir_data+f'batch_by_id/0/')]
    
    for i in to_do:
        for table in ['avg5m', 'realtime']:
        # if True:
            for interval_idx, interval in enumerate(intervals):
                db.execute(f"""DELETE FROM "{table}" WHERE item_id={i} AND timestamp BETWEEN ? AND ?""", interval)
                # db.execute(f"""DELETE FROM "{table}" WHERE item_id<=1307 AND timestamp BETWEEN ? AND ?""", (_t0, _t1))
                # db.execute(f"""DELETE FROM "{table}" WHERE item_id={i} AND timestamp >= {ts}""")
                if time.perf_counter() > t_commit:
                    print(f' [{fmt.delta_t(time.perf_counter() - t_)}] table={table} interval={interval_idx}/{n} ({fmt.unix_(interval[0])}, {fmt.unix_(interval[1])}', end='\r')
                    db.commit()
                    t_commit = time.perf_counter() + 15
    db.commit()
    print('\n')
    v_start = time.perf_counter()
    print(f' [{fmt.unix_(time.time())}] Starting VACUUM', end='\r')
    db.execute(f"""VACUUM""")
    db.close()
    # print(f'[{fmt.delta_t(time.perf_counter() - t_)}] VACUUMING...')
    # db.execute("""VACUUM main INTO "?" """, (gp.f_db_timeseries[:-3]+'_.db'))
    # for item_id in go.item_ids:
    print(f' [{fmt.delta_t(time.perf_counter() - t_)}] VACUUM completed in {fmt.delta_t(time.perf_counter()-v_start)}')
    return id_threshold


# if __name__ =

def process_table(conn: sqlite3.Connection, con_npy: sqlite3.Connection, table: str):
    
    print(conn.execute(f"""SELECT * FROM {table}""").fetchall())
    print(con_npy.execute(f"""SELECT * FROM {table}""").fetchall())

    print('\n'*5)
    
def prices(con: sqlite3.Connection, table: str):
    sql = f"SELECT MIN(price), MAX(PRICE) FROM {table} WHERE src=0 AND timestamp > ?"
    sql_avg5m = f"SELECT MIN(price), MAX(PRICE) FROM {table} WHERE src BETWEEN 1 AND 2 AND volume > 0 AND timestamp > ?"
    return {
         "all": con.execute(f"SELECT MIN(price), MAX(PRICE) FROM {table} WHERE src=0").fetchone(),
         "2y": con.execute(sql, ((time.time()-86400*730)//86400*86400,)).fetchone(),
         "1y": con.execute(sql, ((time.time()-86400*365)//86400*86400,)).fetchone(),
         "1y-avg5m": con.execute(sql_avg5m, ((time.time()-86400*365)//86400*86400,)).fetchone(),
         "6m": con.execute(sql, ((time.time()-86400*180)//86400*86400,)).fetchone(),
         "3m": con.execute(sql, ((time.time()-86400*90)//86400*86400,)).fetchone(),
         "1m": con.execute(sql, ((time.time()-86400*30)//86400*86400,)).fetchone(),
         "2w": con.execute(sql, ((time.time()-86400*14)//86400*86400,)).fetchone(),
         "1w": con.execute(sql, ((time.time()-86400*7)//86400*86400,)).fetchone(),
         "1w-avg5m": con.execute(sql_avg5m, ((time.time()-86400*7)//86400*86400,)).fetchone(),
        "n_rt": con.execute(f"SELECT COUNT(*) FROM {table} WHERE src>2 AND timestamp > ?", ((time.time()-86400*30)//86400*86400,)).fetchone()[0],
        "n_avg5m": con.execute(f"SELECT COUNT(*) FROM {table} WHERE src BETWEEN 1 AND 2 AND timestamp > ? AND volume > 0", ((time.time()-86400*30)//86400*86400,)).fetchone()[0],
        "daily_volume": int(con.execute(f"SELECT AVG(VOLUME) FROM {table} WHERE src=0 ORDER BY timestamp DESC LIMIT 14").fetchone()[0])
    }

def extract_row(_item: Item, row: Dict[str, Tuple[int, int] | int]) -> Dict[str, any]:
    return {
        "item": _item.item_name,
        "2y-low": row["2y"][0],
        "2y-high": row["2y"][1],
        "1y-low": row["1y"][0],
        "1y-high": row["1y"][1],
        "1y-avg5m-low": row["1y-avg5m"][0],
        "1y-avg5m-high": row["1y-avg5m"][1],
        "1m-low": row["1m"][0],
        "1m-high": row["1m"][1],
        "1w-low": row["1w"][0],
        "1w-high": row["1w"][1],
        "1w-avg5m-low": row["1w-avg5m"][0],
        "1w-avg5m-high": row["1w-avg5m"][1],
        "1w-avg5m-margin": row["1w-avg5m"][1]-row["1w-avg5m"][0],
        "n_rt": row["n_rt"],
        "n_avg5m": row["n_avg5m"],
        "cur-buy": item.current_buy,
        "cur-sell": item.current_sell,
        "cur-price": item.current_ge
    }
ordered_keys = ("item", "cur-price", "cur-buy", "cur-sell", "avg_volume_day", "margin", "n_rt", "n_avg5m", "1w-low", "1w-high", "1w-avg5m-low", "1w-avg5m-high", "1w-avg5m-margin", "1m-low", "1m-high", "1y-low", "1y-high", "1y-avg5m-low", "1y-avg5m-high", "2y-low", "2y-high")
if __name__ == "__main__":
    from data_processing.npy_per_week import *
    exit(1)
    
    
    
    db_npy = Database(gp.f_db_npy)
    db = Database(gp.f_db_timeseries)
    from item.db_entity import Item
    i = Item.create(2)
    if isinstance(i, Item):
        print(i.current_sell, i.current_buy, i.current_ge)
    # exit(1)
    
    c = db_npy.cursor()
    c.row_factory = lambda cursor, row: row[0]
    rows = []
    tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    n_items = len(tables)
    keys = []
    blacklist = []
    for idx, i in enumerate(tables):
        # if len(rows) > 2:
        #     break
        item = Item.create(int(i[4:]))
        item_prices = prices(db, i)
        if item_prices['daily_volume'] > item.buy_limit*4 and (item_prices['2w'][0]) < (item_prices['2y'][0])*1.25:
            # print(item.item_name)
            # print(item)
            # for k, v in item_prices.items():
            #     print(k, v)
            # print('\n\n')
            row = extract_row(item, item_prices)
            if len(keys) == 0:
                for k in item.__dir__():
                    try:
                        if k.startswith('_') or k in blacklist or row.get(k) is not None:
                            continue
                        value = item.__getattribute__(k)
                        if isinstance(value, (int, float)):
                            row[k] = value
                            keys.append(k)
                    except NotImplementedError:
                        blacklist.append(k)
                        continue
            else:
                for k in keys:
                    try:
                        if k.startswith('_') or k in blacklist or row.get(k) is not None:
                            continue
                        value = item.__getattribute__(k)
                        if isinstance(value, (int, float)):
                            row[k] = value
                            keys.append(k)
                    except NotImplementedError:
                        blacklist.append(k)
                        continue
            rows.append({k: row[k] for k in ordered_keys})
            print(f"current item_id: {item.item_id} {item.item_name} ({idx}/{n_items})"+' '*10, end='\r')
    pd.DataFrame(rows).to_csv('test.csv', index=False)
    exit(1)
    
    
    
