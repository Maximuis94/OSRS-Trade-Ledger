import sqlite3
import time

from venv_auto_loader.active_venv import *
import global_variables.path as gp
import global_variables.osrs as go
import util.unix_time as ut

__t0__ = time.perf_counter()

db = sqlite3.connect(database=f"{gp.dir_data}/avg5m_correction/avg5m_correction.db", uri=True)

item_ids = []
unique_ids = []
for i in range(30000):
    try:
        item_ids.append(go.itemdb.get(i).get('release_date'))
        unique_ids.append(i)
    except AttributeError:
        item_ids.append(None)
    

if __name__ == '__main__':
    db.row_factory = lambda c, row: row[0]
    max_ts = time.time() // 300 * 300 - 86400*7
    for idx, i in enumerate(unique_ids):
        table = f"item{i:0>5}"
        try:
            ts = max(item_ids[i], db.execute(f"""SELECT MIN(timestamp) FROM {table}""").fetchone())
        except TypeError:
            ts = item_ids[i]
        
        print(idx, i, end='\r')
        while ts <= max_ts:
            for src in (1, 2):
                try:
                    db.execute(f"""INSERT INTO {table}(src, timestamp, price, volume) VALUES (?, ?, 0, 0)""", (src, ts))
                except sqlite3.IntegrityError:
                    ...
            ts += 300
        db.commit()
    