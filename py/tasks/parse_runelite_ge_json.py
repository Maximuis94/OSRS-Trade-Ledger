"""
TODO: Check output/unsubmitted_transactions.csv voor transacties die nog gesubmit moeten worden
"""
import json
import os
import sqlite3
import time
from collections import namedtuple
from dataclasses import dataclass, field

import pandas as pd

from venv_auto_loader.active_venv import *
from model.database import Database
import global_variables.path as gp
import global_variables.osrs as go
import util.unix_time as ut
__t0__ = time.perf_counter()

to_submit = []
j_ts = int(time.time())


@dataclass(match_args=True)
class RowTuple:
    timestamp: int = field(compare=True)
    item_id: int = field(compare=True)
    is_buy: int = field(compare=True)
    price: int = field(compare=True)
    quantity: int = field(compare=True)
    value: int = field(compare=True)
    account_name: str = field(compare=True)
    
    def __init__(self, timestamp, item_id, is_buy, price, quantity, value, account_name):
        e = go.itemdb.get(item_id)
        remap_item = e.get('remap_to') > 0
        
        self.timestamp = timestamp
        self.item_id = e.get('remap_to') if remap_item else item_id
        self.is_buy = is_buy
        self.price = int(price * e.get('remap_price')) if remap_item else price
        self.quantity = int(quantity * e.get('remap_quantity')) if remap_item else quantity
        self.value = self.price * self.quantity
        self.account_name = account_name
        
    def is_submitted(self, db: sqlite3.Connection, print_entry: bool = True):
        rows = db.execute(f"""SELECT * FROM 'transaction'
                            WHERE timestamp BETWEEN ? AND ? AND item_id=? AND is_buy=? AND price BETWEEN ? AND ? AND quantity=?""",
                          (self.timestamp-6000, self.timestamp+6000, self.item_id, self.is_buy, self.price, self.price+1, self.quantity)).fetchall()
        if len(rows) > 0 and print_entry:
            print(self, 'is submitted as:', *rows)
        return len(rows) > 0
    
    def can_submit(self):
        return self.quantity > 1 and self.value > 100000
        
    @staticmethod
    def from_runelite_ge_export(json_row: dict, account_name: str):
        return RowTuple(
            timestamp=int(json_row['time'] / 1000),
            item_id=int(json_row['itemId']),
            is_buy=int(json_row['buy']),
            quantity=int(json_row['quantity']),
            price=int(json_row['price']),
            value=int(json_row['price']) * int(json_row['quantity']),
            account_name=account_name)
    
    def __repr__(self):
        return f"Timestamp: {ut.loc_unix_dt(self.timestamp)} ({self.timestamp}), " \
               f"{'Bought' if bool(self.is_buy) else 'Sold'} {self.quantity} {go.id_name[self.item_id]} " \
               f"({self.item_id}) @ {self.price} ea"
    
    def upload_transaction(self, con: sqlite3.Connection):
        if self.is_submitted(con) and self.can_submit():
            con.execute("""INSERT INTO 'transaction'(transaction_id, item_id, timestamp, is_buy, quantity, price, status, tag, update_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (con.execute("SELECT MAX(transaction_id) FROM 'transactions'").fetchone()[0]+1, self.item_id, self.timestamp, self.is_buy, self.quantity, self.price, 1, 'j', j_ts))
            

def is_runelite_json_export(path: str):
    
    return path[-5:] == '.json'


def merge_runelite_json_files(wd: str, src_dir: str):
    to_submit = []
    src_dir = None
    wd = None
    
    cur_files = [f.split('.')[0] for f in os.listdir(wd) if is_runelite_json_export(wd+f)]
    new_files = [f.split('.')[0] for f in os.listdir(src_dir) if is_runelite_json_export(src_dir+f)]
    
    accounts = [f.split('.')[0] for f in cur_files]
    print(accounts)
    db = Database(gp.f_db_local, read_only=True)
    
    merged = {a: json.loads(open(wd+a+'.json', 'rb').read().decode()) for a in accounts}
    for a, transactions in merged.items():
        transactions = [RowTuple(**t) for t in transactions]
        transactions = [t for t in transactions if t.can_submit() and not t.is_submitted(db, False) and t.account_name==a]
        to_submit += [{k: t.__getattribute__(k) for k in RowTuple.__match_args__} for t in transactions]
        print(a, len(transactions))
        
        for t in transactions[-10:]:
            print(t)
    pd.DataFrame(to_submit).to_csv(gp.dir_output+'unsubmitted_transactions.csv', index=False)
    exit(1)
    # {'timestamp': 1720284048, 'item_id': 157, 'is_buy': 0, 'quantity': 1, 'price': 4369, 'value': 4369,
    #  'account_name': 'Zwaardvis94'}
    for a in accounts:
        merged_json_file = wd + a + '.json'
        cur_rows = [json.loads(open(merged_json_file, 'rb').read().decode())]
        
        for f in new_files:
            if f[:len(a)] != a:
                continue
            print(a, f)
            continue
            rows = json.loads(open(src_dir+f, 'rb').read().decode())
            
            if f[-4:] == 'json':
                with open(wd+f, 'rb') as io:
                    account = os.path.split(f.replace('_', ''))[-1].split('.')[0]
                    if account not in accounts:
                        print('\n\n\n', account)
                        accounts.append(account)
                        merged[accounts.index(account)] = json.loads(io.read().decode())
                        print(merged[accounts.index(account)])
                    else:
                        temp = merged.get(accounts.index(account))
                        json_content = json.loads(io.read().decode())
                        for el in json_content:
                            if el not in temp:
                                temp.append(el)
                        merged[accounts.index(account)] = temp
                        print(json_content)

if __name__ == '__main__':
    merge_runelite_json_files('','')
    