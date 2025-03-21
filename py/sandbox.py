# import model.item
import os.path
import sqlite3
import time

import pandas as pd

from backend.download import realtime_prices
from global_variables.importer import *
from item.itemdb import itemdb
from model.database import Database
from model.timeseries import TimeseriesDB


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




if __name__ == "__main__":
    db = Database(gp.f_db_timeseries)
    rp = realtime_prices(True)
    
    for item_id in go.item_ids:
        sql = f"""SELECT price, volume FROM item{item_id:0>5} WHERE src=0 ORDER BY timestamp DESC"""
        sql2 = f"""SELECT price FROM item{item_id:0>5} WHERE src IN (1, 2) and price > 0 ORDER BY timestamp DESC LIMIT 1"""
        try:
            wiki_price, wiki_volume = db.execute(sql, factory=tuple).fetchone()
        except TypeError:
            continue
        
        if wiki_price is None:
            continue
        try:
            buy_price = min(db.execute(sql2, factory=0).fetchall())
        except ValueError:
            continue
        if buy_price == 0 or wiki_price < 10000 and wiki_price / buy_price - 1 < .1 or wiki_volume * (wiki_price - buy_price) < 1000000:
            continue
            
        if (wiki_price-buy_price) * itemdb[item_id].buy_limit > 1000000 and wiki_price > 10000 and wiki_price / buy_price - 1 > .5:
            print(item_id, go.id_name[item_id], wiki_price, buy_price, (wiki_price-buy_price) * itemdb[item_id].buy_limit, "" if buy_price == 0 else f"{(wiki_price / buy_price - 1) * 100:.1f}%")
            
        elif itemdb[item_id].buy_limit == 0 and wiki_price - buy_price > 1000 and not itemdb[item_id].equipable:
            # print("lim=0", item_id, go.id_name[item_id], wiki_price, buy_price, wiki_price - buy_price)
            ...
    
    # for item_id in go.item_ids[:100]:
    #     print(f"SELECT * FROM item{item_id:0>5}")
    # guide_prices = {item_id: db.execute(f"SELECT * FROM item{item_id:0>5}").fetchall() for item_id in go.npy_items[:100]}
    
    exit(883738462)
    
    
    
    
    
    
    
    
    
    
    
    # from global_variables import path as gp, osrs as go
    # con = sqlite3.connect(gp.f_db_npy)
    #
    # for i in range(max(go.item_ids)+10):
    #     try:
    #         con.execute(f"DELETE FROM 'item{i:0>5}' WHERE timestamp >= 1729468800")
    #     except sqlite3.OperationalError:
    #         ...
    # con.commit()
    # exit(1)
    
    
    
    import pandas as pd
    import util.unix_time as ut
    # con = sqlite3.connect(gp.dir_data+'to_import/dbs_merged.db')
    wd = gp.dir_data + 'to_import/'
    skipped = 0
    
    con_from = sqlite3.connect(wd+'old_realtime_data.db')
    con = sqlite3.connect(gp.f_db_timeseries)
    con_from.row_factory = lambda c, row: row[0]
    select = """SELECT src, timestamp, price, 0 FROM "timeseries" WHERE timestamp > 1664748000 AND item_id=?"""
    item_ids = con_from.execute("SELECT DISTINCT item_id FROM timeseries").fetchall()
    con_from.row_factory = lambda c, row: row
    for item_id in item_ids:
        print(f"Current id:", item_id, end='\r')
        con.executemany(f"""INSERT INTO "item{item_id:0>5}"(src, timestamp, price, volume) VALUES (?, ?, ?, ?)""",
                        con_from.execute(select, (item_id,)))
    con.commit()
    con.close()
    exit(1)
            
    # for i in range(2):
    #     data = pd.read_pickle(f"""{gp.dir_data}to_import/realtime_{i:0>3}.dat""")
    #     for row in data.to_dict('records'):
    #         try:
    #             con.execute(f"""INSERT INTO "timeseries"(item_id, src, timestamp, price, volume) VALUES (?, ?, ?, ?, 0)""",
    #                         (row['item_id'], 3+int(row['is_sale']), row['timestamp'], row['price']))
    #         except sqlite3.IntegrityError:
    #             skipped += 1
    #     print(f"Added files from [{gp.dir_data}to_import/realtime_{i:0>3}.dat]")
    # con.commit()
    # con.close()
    # print(f'Skipped a total of {skipped} rows')
    # con_from = sqlite3.connect(wd+'local_database.db')
    # for src, table in enumerate(['realtimelow', 'realtimehigh']):
    #     con_from.row_factory = lambda c, row: row[0]
    #     item_ids = con_from.execute(f"SELECT DISTINCT item_id FROM {table}").fetchall()
    #     con_from.row_factory = lambda c, row: row
    #     src += 3
    #
    #     for item_id in item_ids:
    #         sql_s = f"""SELECT {item_id}, {src}, timestamp, price FROM {table} WHERE item_id={item_id}"""
    #         sql_i = f"""INSERT OR REPLACE INTO "timeseries"(item_id, src, timestamp, price, volume) VALUES (?, ?, ?, ?, 0)"""
    #         con.executemany(sql_i, con_from.execute(sql_s))
    #     con.commit()
    i = 0
    for db_file, tables in zip(['full_database.db', 'scraped_database.db', 'full_database - Copy.db'], (['realtimelow', 'realtimehigh'], ['rt_low', 'rt_high'], ['realtimelow', 'realtimehigh'])):
        print(f"""\n\nOpening db file {db_file}...""")
        # if i < 2:
        #     i += 1
        #     continue
        con_from = sqlite3.connect(wd + db_file)
        con_from.execute("VACUUM")
        for src, table in enumerate(tables):
            src += 3
            try:
                con_from.execute(f"""CREATE INDEX "index_{table}" ON "{table}" ("item_id" ASC)""")
                print(f"Creating index for table {table} (src={src})")
                con_from.commit()
            except sqlite3.OperationalError:
                print(f"Index for table {table} (src={src}) already exists")
            con_from.row_factory = lambda c, row: row[0]
            item_ids = con_from.execute(f"SELECT DISTINCT item_id FROM {table}").fetchall()
            con_from.row_factory = lambda c, row: row
            
            for item_id in item_ids:
                print(f"""File: {db_file} Table: {table} item_id: {item_id}""", end='\r')
                sql_s = f"""SELECT {item_id}, {src}, timestamp, price FROM {table} WHERE item_id={item_id}"""
                sql_i = f"""INSERT OR REPLACE INTO "timeseries"(item_id, src, timestamp, price, volume) VALUES (?, ?, ?, ?, 0)"""
                con.executemany(sql_i, con_from.execute(sql_s))
            con.commit()
            print('')
        break
        
    # print(data.loc[data.item_id==2])
    print(ut.utc_unix_dt(1615499027))
    exit(1)
    
    
    
    import util.data_structures as ds
    a = {'a': 12, 'b': 13, 'c': 14, 'd': 15}
    b = {'a': 2, 'b': 3, 'c': 4}
    c = ds.update_existing_dict_values(a, b)
    print(c)
    a = {'a': 12, 'b': 13, 'c': 14, 'd': 15}
    b = {'a': 2, 'b': 3, 'c': 4}
    c = ds.update_existing_dict_values(b, a)
    print(c)
    