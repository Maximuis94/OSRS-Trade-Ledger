""" # TODO add preprocessing methods
Module with various implementations for preprocessing transferred data.

"""
import sqlite3
from collections.abc import Iterable

import global_variables.path as gp
from model.data_source import SRC
from model.database import Database


def add_item_data(item_ids: int or Iterable, add_table: bool = False):
    """ Transfer raw timeseries data into the npy table """
    con = Database(gp.f_db_timeseries, read_only=True)
    con2 = sqlite3.connect(gp.f_db_npy)
    
    if isinstance(item_ids, int):
        item_ids = [item_ids]
    
    for item_id in item_ids:
        if add_table:
            try:
                con2.execute(f"""CREATE TABLE "item{item_id:0>5}"("src" INTEGER NOT NULL, "timestamp" INTEGER NOT NULL, "price" INTEGER NOT NULL DEFAULT 0 CHECK (price>=0), "volume" INTEGER NOT NULL DEFAULT 0 CHECK (volume>=0), PRIMARY KEY(src, timestamp) )""")
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
    