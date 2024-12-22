""" # TODO add preprocessing methods
Module with various implementations for preprocessing transferred data.

"""
import sqlite3
from collections.abc import Iterable

from venv_auto_loader.active_venv import *
import global_variables.path as gp
from model.data_source import SRC
from model.database import Database, sql_create_timeseries_item_table
__t0__ = time.perf_counter()


def add_item_data(item_ids: int or Iterable, add_table: bool = False):
    """ Transfer raw timeseries data into the npy table """
    con = Database(gp.f_db_timeseries, read_only=True)
    con2 = sqlite3.connect(gp.f_db_npy)
    
    if isinstance(item_ids, int):
        item_ids = [item_ids]
    
    for item_id in item_ids:
        if add_table:
            try:
                con2.execute(sql_create_timeseries_item_table(item_id, check_exists=False))
            except sqlite3.Error:
                ...
        
        for _src in SRC:
            con2.executemany(f"""INSERT INTO "item{item_id:0>5}"(src, timestamp, price, volume) VALUES ({_src}, ?, ?, ?)""",
                             con.execute("""SELECT timestamp, price, volume FROM item00002 WHERE src=?""", (_src.src_id,),
                                         factory=tuple).fetchall())
    con2.commit()
    con2.close()
    

def create_view_wiki_ts(item_id: int, db: Database):
    sql = """CREATE VIEW wiki_ts ()"""
    

if __name__ == "__main__":
    db = Database(gp.f_db_timeseries, read_only=True)
    