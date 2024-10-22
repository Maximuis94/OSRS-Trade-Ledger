"""
Module for InventoryEntry

The InventoryEntry provides a representation on the cumulative values of executing all transactions for its Item in
chronological order. Post-transaction data refers to the values that represent these cumulative values. It is treated
differently than the original transaction properties, 'static values', as these values are updated after execution or if
 a transaction is affected by a rollback. The latter will cause its post-transaction values to be set to 0, indicating
that the transaction has not yet been executed. When loading transactions, all transactions following the oldest
transaction that does not have its post-transaction values set should be reset as well, since these values are
cumulative.
While the InventoryEntry can frequently update post-transaction data within the database, it should under no
circumstance modify static values. The columns that are considered post-transaction data are listed as the
`extra_columns` tuple within the InventoryEntry.
The InventoryEntry should under no circumstance modify static properties of a transaction; these are to be considered
read-only.
Due to the potentially large number of write operations to the local db, make sure its data (in particular transactions
submitted to the database) is backed up.

"""
import sqlite3
import time
from collections import namedtuple
from typing import List, Dict, Tuple

import global_variables.data_classes
from controller.item import create_item
from global_variables.data_classes import ExeLogEntry
from global_variables.values import ge_tax_min_ts
from model.database import Database
from model.item import Item
from model.transaction import *
from util.osrs import get_tax

import global_variables.path as gp

class InventoryEntry:
    db_path = gp.f_db_local
    extra_columns: Tuple[str] = ('average_buy', 'balance', 'value', 'n_bought', 'n_purchases', 'n_sold', 'n_sales',
                                 'profit', 'tax')
    reset_dict: Dict[str, int] = {k: 0 for k in extra_columns}
    
    def __init__(self, item: Item, reset_values: bool = False):
        if isinstance(item, int):
            item = create_item(item_id=item)
        self.item = item
        
        self.average_buy: int = 0
        self.balance: int = 0
        self.profit: int = 0
        self.value: int = 0
        self.n_sold: int = 0
        self.n_bought: int = 0
        self.n_sales: int = 0
        self.n_purchases: int = 0
        self.tax: int = 0
        
        self.execution_log: List[int] = []
        if reset_values:
            self.rollback(0)
        
        con = transaction_parser(self.db_path)
        self.transactions: Dict[int, Transaction] = {
            t.transaction_id: t for t in con.execute("SELECT * FROM 'transaction' WHERE item_id=? ORDER BY timestamp ASC",
                        (self.item.item_id,)).fetchall()}
        
        # Resume from the previous state of the db
        for _, t in self.transactions.items():
            if reset_values or t.n_sales == 0 and t.n_purchases == 0:
                self.execute_transaction(t)
            self.execution_log.append(t.transaction_id)
    
    def execute_transactions(self):
        for _, t in self.sort_transactions([v for _, v in self.transactions.items()]).items():
            if self.execute_transaction(t):
                _t = self.transactions.get(self.execution_log[-1])
                t.__dict__.update({key: _t.__getattribute__(key) for key in self.extra_columns})
    
    def execute_transaction(self, t: Transaction) -> bool:
        if t.transaction_id in self.execution_log:
            return False
        
        if t is None:
            raise KeyError(f"Unable to find a transaction in InventoryEntry for item {self.item.item_name}")
        
        if not isinstance(t, Transaction):
            raise TypeError(f'Object {t.__dict__} is not a Transaction, but a {type(t)}')
        
        tag = t.tag.lower()
        if tag is None or tag == 'e' or tag == '':
            tag = 'b' if t.is_buy else 's'
        
        # Sale
        if isinstance(t, Sale):
            return self.sale(t)
        
        # Purchase
        if isinstance(t, Purchase):
            return self.purchase(t)
        
        # Correction
        if isinstance(t, Correction):
            return self.correction(t)
        
        # Consumption
        if isinstance(t, Consumption):
            return self.consumption(t)
        
        # Production
        if isinstance(t, Production):
            return self.production(t)
        
        # Stock count
        if isinstance(t, StockCount):
            return self.stock_count(t)
        
        # Bond purchase
        if isinstance(t, Bond):
            return self.bond_purchase(t)
        
        raise ValueError(f"Encountered an unknown tag {t.tag}")
    
    # TODO integrate exe log within Local db?
    def exe_log_entry(self, t: Transaction) -> ExeLogEntry:
        return ExeLogEntry(t.transaction_id, t.timestamp, self.average_buy, self.balance, self.profit, self.value,
                           self.n_bought, self.n_purchases, self.n_sold, self.n_sales)
        
    def sale(self, t: Transaction):
        tax = self.get_tax(t)
        self.balance -= t.quantity
        self.profit += (t.price - self.average_buy - tax) * t.quantity
        self.value = self.balance * self.average_buy
        self.n_sold += t.quantity
        self.n_sales += 1
        self.tax += tax*t.quantity
        self.extend_execution_log(t)
    
    def purchase(self, t: Transaction):
        self.average_buy, self.balance = self.weighed_price(self.average_buy, self.balance, t.price, t.quantity)
        self.value = self.balance * self.average_buy
        self.n_bought += t.quantity
        self.n_purchases += 1
        self.extend_execution_log(t)
    
    def correction(self, t: Transaction):
    
        self.purchase(t)
    
    def consumption(self, t: Transaction):
        raise NotImplementedError
        self.balance -= t.quantity
        if self.balance < 0:
            self.balance = 0
    
        self.extend_execution_log(t)
    
    def production(self, t: Transaction):
        raise NotImplementedError
        self.average_buy, self.balance = self.weighed_price(self.average_buy, self.balance, t.price, t.quantity)
        self.value = self.balance * self.average_buy
    
        self.extend_execution_log(t)
    
    def stock_count(self, t: Transaction):
        
        deficit = self.balance - t.quantity
        if t.price > 0:
            if deficit > 0:
                tax = self.get_tax(t)
                self.profit += (t.price - self.average_buy - tax) * deficit
                self.n_sold += deficit
                self.n_sales += 1
                self.tax += tax*deficit
            else:
                self.n_bought += deficit
                self.n_purchases += 1
        if t.status > 1:
            t.average_buy = t.status
        self.balance = t.quantity
        self.value = self.balance * self.average_buy
        
        self.extend_execution_log(t)
    
    def bond_purchase(self, t: Transaction):
        self.average_buy, self.balance = self.weighed_price(self.average_buy, self.balance, t.price, t.quantity)
        self.value = self.balance * self.average_buy
        self.n_bought += t.quantity
        self.n_purchases += 1
        self.extend_execution_log(t)
    
    def extend_execution_log(self, t: Transaction):
        """
        Extend the execution log by filling in the extra columns with post-transaction data. Also applies to db.
        Parameters
        ----------
        t :

        Returns
        -------

        """
        _t = {'transaction_id': t.transaction_id}
        for k in self.extra_columns:
            v = self.__getattribute__(k)
            t.__setattr__(k, v)
            _t[k] = int(v)
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """UPDATE 'transaction' SET
                average_buy=:average_buy,
                balance=:balance,
                n_bought=:n_bought,
                n_purchases=:n_purchases,
                n_sold=:n_sold,
                n_sales=:n_sales,
                tax=:tax,
                profit=:profit
                WHERE transaction_id=:transaction_id""", _t)
        except OverflowError as e:
            print(_t)
            print(t)
            raise e
        con.commit()
        self.execution_log.append(t.transaction_id)
        
    def rollback(self, timestamp: int):
        t = transaction_parser(self.db_path).execute("SELECT * FROM 'transaction' WHERE item_id=? AND timestamp < ? ORDER BY timestamp DESC", (self.item.item_id, timestamp)).fetchone()
        reset_columns = False
        for e in self.execution_log:
            if e.timestamp > timestamp:
                reset_columns = True
                self.execution_log.remove(e)
        if reset_columns:
            con = sqlite3.connect(self.db_path)
            con.execute("""UPDATE 'transaction' SET
                average_buy=0, balance=0, value=0, n_bought=0, n_purchases=0, n_sold=0, n_sales=0, profit=0, tax=0
                WHERE item_id=? AND timestamp>?""", (self.item.item_id, timestamp))
            con.commit()
            con.close()
        if not isinstance(t, Transaction):
            return
        for k in self.extra_columns:
            self.__setattr__(k, t.__getattribute__(k))
    
    def submit_data(self, t):
        con = sqlite3.connect(self.db_path)
        sql = "INSERT OR REPLACE INTO 'transaction'("
        b = ") VALUES ("
        args = []
        for c in global_variables.data_classes.Transaction.__match_args__:
            if c in self.extra_columns:
                continue
            sql += c + ', '
            b += '?, '
            args.append(self.__dict__.get(c))
        con.execute(sql[:-2] + b[:-2] + ')', tuple(args))
        
    @staticmethod
    def sort_transactions(transactions: List[Transaction]) -> Dict[int, Transaction]:
        """ Sort `transactions` chronologically and return it as dict with transaction_ids as key """
        _temp = {t.timestamp: t for t in transactions}
        _timestamps = list(_temp.keys())
        _timestamps.sort()
        return {_temp.get(ts).transaction_id: _temp.get(ts) for ts in _timestamps}
    
    @staticmethod
    def weighed_price(p0, q0, p1, q1, threshold: int = 250):
        sq = q0+q1
        
        # initial quantity is below 0; set new price to the buy price of the purchase
        if q0 <= 0:
            return p1, sq
        try:
            wp = int(((p0 * q0 + p1 * q1) / sq) * 100) / 100
        except ZeroDivisionError as e:
            print(p0, p1, q0, q1)
            raise e
        if wp > threshold:
            wp = int(round(wp * 2) // 2)
        return wp, q0+q1
    
    @staticmethod
    def get_tax(t: Transaction) -> int:
        return get_tax(t.price) if t.timestamp > ge_tax_min_ts else 0


if __name__ == '__main__':
    # import util.str_formats as fmt
    t_, n = time.perf_counter(), 0
    profit = 0
    for item_id in sqlite3.connect(gp.f_db_local).execute("SELECT DISTINCT item_id FROM 'transaction'").fetchall():
        if item_id != (13573,):
            continue
        item_id = item_id[0]
        ie = InventoryEntry(item=create_item(item_id), reset_values=True)
        profit += int(Database(gp.f_db_local).execute("SELECT profit, MAX(timestamp) FROM 'transaction' WHERE item_id=?",
                                                  (item_id,), factory=0).fetchone())
        print(item_id, go.id_name[item_id], int(Database(gp.f_db_local).execute("SELECT profit, MAX(timestamp) FROM 'transaction' WHERE item_id=?",
                                                  (item_id,), factory=0).fetchone()))
        continue
        n += len(ie.execution_log)
    print('cumulative profit', fmt.number(profit))
    print(f'Transactions executed: {n}\t| time taken:', fmt.delta_t(time.perf_counter()-t_))
