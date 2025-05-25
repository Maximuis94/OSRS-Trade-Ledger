"""
Module with parsing logic for the runelite profile .properties file.

The file itself logs events that occurred on the Runelite client, up to some extent.

"""
import os.path
from math import floor

import json
import sqlite3
import time
from typing import Dict, List, Literal, NamedTuple

import global_variables.path as gp
from item.itemdb import itemdb
from transaction.raw.raw_runelite_profile_trade_entry import RuneliteProfileTransaction

# file = r"C:\Users\Max Moons\Documents\GitHub\OSRS-Trade-Ledger\py\testing\trade-history-rsprofile--1.json"
#
#
# loaded = json.load(open(file))
# json.dump(loaded, open(os.path.join(os.path.split(file)[0], "rsprofile_fmt.json"), 'w'), indent=2)


hash_lists = {

}
_hash_list = []
cur_hash_list = []


class RuneliteProfileTransaction(NamedTuple):
    item_id: int
    timestamp: int
    is_buy: int
    quantity: int
    price: int
    account_name: str
    
    @staticmethod
    def create(is_buy: bool, item_id: int, quantity: int, price: int, timestamp: int, account_name: str):
        i = itemdb[item_id]
        
        if i.remap_to:
            item_id = i.remap_to
            quantity = int(i.remap_quantity * quantity)
            price = int(i.remap_price * price)
        
        return RuneliteProfileTransaction(
            item_id,
            timestamp // 1000,
            int(is_buy),
            quantity,
            price,
            profile_account[account_name]
        )
    
    # @property
    # def dict(self) -> Dict[str, str | int]:
    #     return self._asdict()
    
    @property
    def dict(self) -> Dict[str, str | int]:
        return {
            "item_id": self.item_id,
            "item_name": itemdb[self.item_id].item_name,
            "timestamp": self.timestamp, #datetime.datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            "type": "B" if self.is_buy else "S",
            "price": self.price,
            "quantity": self.quantity,
            "account": self.account_name
        }
    
    @property
    def sql(self):
        t0, t1 = self.timestamp-30, self.timestamp + 86400*3
        return f"""SELECT COUNT(*) FROM "transaction" WHERE item_id={self.item_id} AND timestamp BETWEEN {t0} AND {t1} AND is_buy={self.is_buy} AND price >= {self.price} AND quantity={self.quantity}"""


    
    # global trades
    # insert_time = int(time.time())
    # db = sqlite3.connect(os.path.join(gp.dir_data, "transaction_database.db"))
    # cur = db.cursor()
    # cur.row_factory = lambda cursor, row: row[0]
    # _id = cur.execute("SELECT MAX(transaction_id) FROM raw_runelite_export_transaction").fetchone()+1
    # for t in trades:
    #     existing_ids = cur.execute("""SELECT transaction_id FROM "raw_exchange_logger_transaction" WHERE item_id=? AND timestamp=? AND is_buy=? AND quantity=? AND price=?""", t[:5]).fetchall()
    #     if len(existing_ids) > 0:
    #         db.executemany("DELETE FROM raw_runelite_export_transaction WHERE transaction_id=?", [(el,) for el in existing_ids])
    #     db.execute(f"""INSERT INTO "raw_runelite_export_transaction" (transaction_id, item_id, timestamp, is_buy, quantity, price, account_name, update_timestamp) VALUES ({_id}, ?, ?, ?, ?, ?, ?, {insert_time})""", t[:6])
    #     db.commit()
    #     _id += 1
    ...