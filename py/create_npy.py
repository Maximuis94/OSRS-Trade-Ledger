"""
This module contains the implementation of a threaded npy db updater.
Toevoegen: Global index; als een thread een nieuw item nodig heeft, pakt hij de global index en wordt deze vervolgens
met 1 verhoogd
Rijen als pickle exporten? Dan kan een extra thread deze ondertussen uploaden naar de centrale db
Hoe dan ook wss wel handiger om een aparte npy_db_updater te maken

Concurrency issues met globals zou je kunnen oplossen door bv thread 3 alleen toegang te geven tot globale variabelen
als time.perf_counter() * 1000 % n_threads*1000 tussen thread_id*1000 en (thread_id+1)*1000 ligt
"""

import shutil
import sqlite3
import threading
import time

import pandas as pd

import global_variables.osrs as go
import global_variables.path as gp
import backend.npy_db_updater as npy_db
from file.file import File
from global_variables.osrs import npy_items
from global_variables.values import empty_tuple
from my_util import *
from model.timeseries import TimeseriesDB

empty_db: File = gp.f_db_npy

# npy_db.reset_to_do(len(go.npy_items))

threads_completed, n_threads = 0, 4


def export_rows():
    def export_table(item_id: int, db_from: sqlite3.Connection):
        return db_from.execute(f"""SELECT * FROM 'item{item_id:0>5}'""").fetchall()
        


def is_done():
    global threads_completed
    threads_completed += 1
    if threads_completed == n_threads:
        export_rows()


def importer_status_check():
    return
        


class AsyncTask(threading.Thread):
    def __init__(self, idx: int, n_threads: int, callback_oncomplete: callable = None, **kwargs):
        threading.Thread.__init__(self, name=kwargs.get('name'), daemon=kwargs.get('daemon'))
        self.on_complete = callback_oncomplete
        self.string = ''
        self.idx = idx
        
        self.db_path = File(gp.dir_data+f'npy_{idx}.db')
        if not self.db_path.exists():
            shutil.copy2(empty_db, self.db_path)
            print(f'Created a db for thread {idx}')
        self.kwargs = {
            'item_ids': [item_id for idx, item_id in enumerate(npy_items) if idx % n_threads == self.idx],
            'db_path': self.db_path,
            'add_arrays': False,
            'execute_update': True,
            'prices_listbox_path': None,
            'itemdb': sqlite3.connect(database=f"file:{gp.f_db_local}?mode=ro", uri=True)
        }
    
    def run(self):
        npy_db.NpyDbUpdater(**self.kwargs)
    

def purge_rows():
    shutil.copy2(gp.f_db_npy.replace('.db', '_.db'), gp.f_db_npy)
    db = sqlite3.connect(gp.f_db_npy)
    for item_id in go.npy_items:
        db.execute(f"""DELETE FROM 'item{item_id:0>5}' """)
    db.commit()
    db.execute("VACUUM")


def create_npy_db_threaded(n_threads: int = 4, reset_db: bool = False):
    
    threads = []
    
    if reset_db:
        purge_rows()
    
    
    for idx in range(n_threads):
        threads.append(AsyncTask(idx, n_threads, callback_oncomplete=is_done))
    for t in threads:
        t.start()


import util.str_formats as fmt


def test_db_small(db, sql, params):
    db = Database(db, parse_tables=False)
    _t0 = time.perf_counter()
    db.connect()
    n_rows = len(db.execute(sql, params).fetchall())
    
    print(f"Query time for {n_rows} rows from one table: {int(1000*(time.perf_counter()-_t0))}ms", end='\n\n')

if __name__ == '__main__':
    select = """SELECT * FROM item00002"""# WHERE timestamp > ?"""
    values = int(time.time())
    values = (values-values%86400-86400*13,)

    for f in (gp.f_db_npy,):  # File(gp.dir_data + 'npy_.db')):
        print(f"File size of db {f.file}: {fmt.fsize(f.fsize())}")
        test_db_small(f, select, empty_tuple)

# if __name__ == '__main__':
#     import util.file as uf
#     pd.DataFrame(gp.f_prices_listbox.load().get(2)).to_csv(gp.dir_output+'listbox.csv')
#     print(gp.f_prices_listbox.load().get(2))
#     # create_npy_db_threaded(n_threads=threads)
#     # from backend.npy_db_updater_threaded import exe
#     # exe()
    