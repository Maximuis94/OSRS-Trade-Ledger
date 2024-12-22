"""
Module with a Database subclass, specifically designed for the Inventory


"""
import sqlite3
import time
from collections.abc import Callable
from typing import List, Tuple, Dict

import numpy as np
from attr import dataclass

import global_variables.osrs as go
from file.file import File
from inventory._classes import SQL
from inventory.constants import transaction_db, empty_tuple
from inventory.transactions import Transaction, TransactionResults
from util.str_formats import passed_pc


@dataclass(slots=True, frozen=True, order=True)
class TransactionDatabase:
    """
    
    """
    path: File = transaction_db
    table = "transaction"
    
    def insert(self, transaction: Transaction or TransactionResults):
        """Insert `transaction` into the database"""
        if isinstance(transaction, Transaction):
            transaction = TransactionResults.from_transaction(transaction)
        
        self.execute(transaction.sql_insert)
    
    def execute(self, sql: SQL, con: sqlite3.Connection = None, row_factory: Callable[[sqlite3.Cursor, sqlite3.Row], any] = None, commit_transaction: bool = True) -> sqlite3.Cursor | sqlite3.Connection:
        
        return_con = con is None
        if return_con:
            con = sql.con
        try:
            if row_factory is not None:
                con.row_factory = row_factory
            if sql.read_only:
                return con.execute(sql.query, sql.parameters)
            con.execute(sql.query, sql.parameters)
            
            if commit_transaction:
                con.commit()
            return con if return_con else None
        except sqlite3.Error as e:
            err = e.__repr__()
            note = f"An sqlite3.{err} exception occurred while executing SQL={sql}"
            e.add_note(note)
            raise e
    
    def executemany(self, sql: List[SQL], args: List[tuple]):
        n_error = 0
        con = sqlite3.connect(self.path)
        for cur in sql:
            if cur.read_only:
                raise ValueError("Read-only transactions are not allowed for executemany; "
                                 "how am I supposed to return them?")
            try:
                if cur.read_only:
                    return con.execute(cur.query, cur.parameters)
                con.execute(cur.query, cur.parameters)
            except sqlite3.Error as e:
                n_error += 1
                print(f"An sqlite3.{e.__name__} exception occurred while executing SQL={sql}")
        if n_error > 0:
            msg = f"Execution of a total of {n_error} transactions failed, did not submit anything..."
            con.rollback()
            raise RuntimeError(msg)
        con.commit()
        return True
    
    @staticmethod
    def is_read_only_query(sql: SQL) -> bool:
        
        return sql.query[:7] == "SELECT "
    
    def connect(self, sql: SQL = None) -> sqlite3.Connection:
        """Establish and return a read-only database connection"""
        return sqlite3.connect(database=self.path
        if sql is None or not TransactionDatabase.is_read_only_query(sql) else
        f"file:{self.path}?mode=ro",
                               uri=True)
    
    def load_transactions(self, sql: SQL = None):
        """Load all the transactions from the transaction database"""
        return self.load_all_transactions(True) if sql is None else self.execute(sql, row_factory=TransactionResults.row_factory).fetchall()
    
    def get_duplicates(self, t: Transaction or TransactionResults) -> List[TransactionResults]:
        """Check if there are any transactions in the database, given Transaction `t`"""
        if isinstance(t, Transaction):
            p = (t.transaction_id, t.item_id, t.timestamp, t.is_buy, t.quantity, t.price)
        else:
            _t = t.transaction
            p = (_t.transaction_id, _t.item_id, _t.timestamp, _t.is_buy, _t.quantity, _t.price)
        return self.execute(SQL(
            f"""SELECT * FROM "{self.table}" WHERE transaction_id!=? AND item_id=? AND timestamp=? AND is_buy=? AND quantity=? AND price=? AND status=1""",
            p, True, self.path), row_factory=TransactionResults.row_factory).fetchall()
    
    @property
    def sql_select_all(self) -> str:
        """SQL statement for selecting all transactions, ordered by item_id and timestamp"""
        return f"""SELECT * FROM "{self.table}" WHERE status != 0 ORDER BY item_id ASC, timestamp ASC"""
    
    def load_all_transactions(self, exclude_inactive: bool = False) -> List[Transaction]:
        """Load all (active) transactions from the underlying database and return them as a list of Transactions"""
        if exclude_inactive:
            return self.execute(SQL(f"""SELECT * FROM "{self.table}" WHERE status != 0 ORDER BY item_id ASC, timestamp ASC""", empty_tuple, True, self.path), row_factory=TransactionResults.row_factory).fetchall()
        else:
            return self.execute(SQL(f"""SELECT * FROM "{self.table}" ORDER BY item_id ASC, timestamp ASC""", empty_tuple, True, self.path), row_factory=TransactionResults.row_factory).fetchall()


class InventoryEntry:
    """
    Representation of a single InventoryEntry. It keeps track of cumulative values for a specific item and is typically
    operated via the Inventory.
    """
    
    item_id: int
    transactions: List[TransactionResults]
    submitted: List[int]
    
    average_buy: int = 0
    balance: int = 0
    profit: int = 0
    value: int = 0
    n_bought: int = 0
    n_purchases: int = 0
    n_sold: int = 0
    n_sales: int = 0
    tax: int = 0
    
    cumulative_attributes: Tuple[str, ...] = ('average_buy', 'balance', 'profit', 'value', 'n_bought',
                                              'n_purchases', 'n_sold', 'n_sales', 'tax')
    """All attributes/columns that describe cumulative values."""
    
    db_file: File = transaction_db
    """Path to the underlying database"""
    
    table_name: str = "executed"
    """Name of the executed table that holds cumulative results"""
    
    to_do: List[SQL] = None
    """List of SQLite queries and parameters to execute"""
    
    last_submit: Transaction = None
    """Last executed Transaction, given the cumulative values (this does not imply database submissions)"""
    
    def __init__(self, item_id: int, transaction_database: File = None):
        self.item_id = item_id
        
        if transaction_database is not None:
            self.db_file = transaction_database
        self.transactions = []
        self.load_transactions()
    
    def load_transactions(self):
        """Load all transactions with active status for this item_id. Sort them in chronological order."""
        db = TransactionDatabase()
        self.transactions = db.load_transactions(self.sql_select)
    
    def load_state_ts(self, timestamp: int):
        """Load the cumulative values prior to timestamp `timestamp`"""
        con = self.connect_read_only
        self.set_state(*con.execute(f"""SELECT {", ".join(self.cumulative_attributes)} FROM "transaction" WHERE item_id=? AND timestamp < ? AND status!=0 ORDER BY timestamp DESC LIMIT 1""", (self.item_id, timestamp)).fetchone())
        
        con.row_factory = TransactionResults.row_factory
        self.last_submit = con.execute(f"""SELECT * FROM "transaction" WHERE item_id=? AND timestamp < ? AND status!=0 ORDER BY timestamp DESC LIMIT 1""", (self.item_id, timestamp)).fetchone().transaction
    
    def load_state(self, t: TransactionResults, before_t: bool = True):
        """Load the cumulative values before or after Transaction `t`"""
        con = self.connect_read_only
        if before_t:
            values = con.execute(f"""SELECT {", ".join(self.cumulative_attributes)}, transaction_id FROM "transaction" WHERE timestamp<? AND status=1 ORDER BY timestamp DESC LIMIT 1""", (t.transaction.timestamp,)).fetchone()
        else:
            values = con.execute(f"""SELECT {", ".join(self.cumulative_attributes)}, transaction_id FROM "transaction" WHERE transaction_id=?""",
                                 (t.transaction.transaction_id,)).fetchone()
        self.set_state(*values)
        
        con.row_factory = TransactionResults.row_factory
        self.last_submit = con.execute(f"""SELECT * FROM "transaction" WHERE transaction_id=?""", (values[-1],)).fetchone().transaction
    
    def set_state(self, *args, **kwargs):
        for a, v in zip(self.cumulative_attributes, args):
            self.__setattr__(a, v)
        
        for a in frozenset(self.cumulative_attributes).intersection(kwargs):
            self.__setattr__(a, kwargs[a])
    
    def execute_transaction(self, t: TransactionResults):
        """Execute Transaction `t`, by updating its cumulative values."""
        if t.transaction.tag == "X":
            if t.transaction.price > 0 and False:
                if t.transaction.quantity > self.balance:
                    balance = max(0, self.balance)
                    self.n_purchases += 1
                    average_buy = ((balance * self.average_buy) + (t.transaction.quantity * t.transaction.price)) // (balance + t.transaction.quantity)
                    self.average_buy = average_buy
                else:
                    self.n_sales += 1
                    self.profit += abs(self.balance - t.transaction.quantity) * (t.transaction.price - self.average_buy - t.transaction.tax)
                    self.tax += t.transaction.tax * abs(self.balance - t.transaction.quantity)
            self.balance = t.transaction.quantity
            self.value = self.balance * self.average_buy
        else:
            if t.transaction.is_buy == 1:  # Purchase
                # balance = max(0, balance)
                try:
                    b = max(0, self.balance)
                    self.average_buy = ((self.average_buy * b + t.transaction.price * t.transaction.quantity) //
                                        (t.transaction.quantity + b))
                except ZeroDivisionError:
                    self.average_buy = t.transaction.price
                self.balance += t.transaction.quantity
                self.n_bought += t.transaction.quantity
                self.n_purchases += 1
                
                # Update average buy price
                # average_buy = total_cost / balance
            
            elif t.transaction.is_buy == 0:  # Sale
                self.balance -= t.transaction.quantity
                self.n_sold += t.transaction.quantity
                
                # Calculate profit for this sale
                transaction_profit = (t.transaction.price - self.average_buy - t.transaction.tax) * t.transaction.quantity
                self.profit += transaction_profit
                self.n_sales += 1
                
                self.tax += t.transaction.tax * t.transaction.quantity
                
                # Update cumulative value
            self.value = max(0, self.balance * self.average_buy)
        # print(self.sql_update(t))
        self.last_submit = t.transaction
        self.to_do.append(self.sql_update(t))
    
    def execute_all(self):
        """Execute all transactions and update their cumulative values."""
        self.average_buy = 0
        self.balance = 0
        self.profit = 0
        self.value = 0
        self.n_bought = 0
        self.n_purchases = 0
        self.n_sold = 0
        self.n_sales = 0
        self.tax = 0
        
        self.to_do: List[SQL] = []
        for next_transaction in self.transactions:
            self.execute_transaction(next_transaction)
            # print(self.to_do[-1])
        
        con = sqlite3.connect(self.db_file)
        for next_sql in self.to_do:
            con.execute(next_sql.query, next_sql.parameters)
            # print(next_sql.query, next_sql.parameters)
        con.commit()
        con.close()
    
    def cast(self, attribute: str, value: any):
        
        T = self.__annotations__.get(attribute)
        if T in (int, float, str, bool):
            return T(value)
        else:
            return value
    
    @property
    def sql_select(self) -> SQL:
        """SQL instance for selecting all Transactions"""
        return SQL("""SELECT * FROM 'transaction' WHERE item_id=? AND status!=0 ORDER BY timestamp ASC""",
                   (self.item_id,), True)
    
    def sql_update(self, t: TransactionResults) -> SQL:
        """Update the cumulative values of the given transaction."""
        # return SQL(f"""UPDATE 'transaction' SET {", ".join([f"{k}=?" for k in self.cumulative_attributes])} WHERE transaction_id=?""",
        #            tuple([self.cast(a, self.__getattribute__(a)) for a in self.cumulative_attributes] + [
        #                t.transaction.transaction_id]),
        #            read_only=False, db=self.db_file)
        return SQL(f"""UPDATE 'executed' SET {", ".join([f"{k}=?" for k in self.cumulative_attributes])} WHERE transaction_id=?""",
                   tuple([self.cast(a, self.__getattribute__(a)) for a in self.cumulative_attributes] + [
                       t.transaction.transaction_id]),
                   read_only=False, db=self.db_file)
    
    def sql_overwrite(self, t: TransactionResults) -> SQL:
        """Update the cumulative values of the given transaction."""
        # return SQL(f"""UPDATE 'transaction' SET {", ".join([f"{k}=?" for k in self.cumulative_attributes])} WHERE transaction_id=?""",
        #            tuple([self.cast(a, self.__getattribute__(a)) for a in self.cumulative_attributes] + [
        #                t.transaction.transaction_id]),
        #            read_only=False, db=self.db_file)
        return f"""INSERT OR REPLACE INTO "{self.table_name}" (transaction_id, {", ".join([f"{k}" for k in self.cumulative_attributes])}) VALUES ({", ".join([f"?" for _ in range(len(self.cumulative_attributes)+1)])})"""
    
    @property
    def connect_read_only(self) -> sqlite3.Connection:
        """Read only connection with the associated .db file"""
        con = sqlite3.connect(f"file:{self.db_file}?mode=ro", uri=True)
        return con
    
    def __str__(self):
        s = ""
        for a in self.cumulative_attributes:
            s += f"{a}={self.__getattribute__(a)}, "
        
        return f"""{go.id_name[self.item_id]} {s}""".rstrip(", ")


class Inventory:
    """
    Representation of the Inventory. Individual entries can be accessed via [item_id], among others. The Inventory
    utilizes a separate table, so the transaction table can be used in a read-only fashion, rather than an IO fashion.
    The InventoryEntry table is more or less like the transaction table, while it is extended with cumulative values
    that represent the Inventory of this item after executing that Transaction
    """
    __slots__ = "db_file", "entries"
    
    db_file: File
    entries: List[InventoryEntry | None]
    
    results_table_name: str = "executed"
    attributes = InventoryEntry.cumulative_attributes
    
    def __init__(self, db_file: File = None):
        self.db_file = transaction_db if db_file is None else db_file
        self.setup_entries()
    
    def setup_entries(self, initial_timestamp: int = 0):
        """Setup new InventoryEntry instances"""
        con = self.con_read_only
        con.row_factory = lambda c, row: row[0]
        self.entries = []
        for item_id in range(max(go.item_ids)+1):
            if item_id in go.item_ids:
                self.entries.append(InventoryEntry(item_id))
            else:
                self.entries.append(None)
    
    @property
    def con_read_only(self):
        """Read only connection with the associated .db file"""
        return sqlite3.connect(f"file:{self.db_file}?mode=ro", uri=True)
    
    @property
    def con_writeable(self):
        """Read only connection with the associated .db file"""
        return sqlite3.connect(self.db_file)
    
    @staticmethod
    def format_attribute(attribute_name: str) -> str:
        """Format an attribute to include in the CREATE TABLE statement"""
        return f""" "{attribute_name}" INTEGER NOT NULL DEFAULT 0 """.strip()
    
    @property
    def sql_create_table_results(self) -> SQL:
        """Create the transaction results table in the underlying database."""
        attributes = list(Transaction.__match_args__)[1:] + list(InventoryEntry.cumulative_attributes)
        return SQL(f"""CREATE TABLE IF NOT EXISTS "{self.results_table_name}"(
            "transaction_id" INTEGER PRIMARY KEY, {", ".join([self.format_attribute(a) for a in attributes])})""",
                   empty_tuple, read_only=False)
    
    @property
    def sql_insert_row(self):
        """SQL insert statement for adding/resetting data for a particular transaction."""
        attributes = list(Transaction.__match_args__) + list(InventoryEntry.cumulative_attributes)
        n_e_t = len(Transaction.__match_args__)
        return f"""INSERT OR REPLACE INTO "{self.results_table_name}"
            ({", ".join([a for a in attributes])}) VALUES ({", ".join(["?" if idx < n_e_t else "0" for idx, _ in enumerate(range(len(Transaction.__match_args__)+len(InventoryEntry.cumulative_attributes)))])})"""
    
    @property
    def sql_update_row(self):
        
        return f"""UPDATE 'transaction' SET {", ".join([f"{k}=?" for k in self.attributes])} WHERE transaction_id=?"""
    
    def setup_table(self):
        """Setup the executed transactions table. """
        sql = self.sql_create_table_results
        con = sql.con
        con.execute(sql.query, sql.parameters)
        con.commit()
        con.close()
    
    def sync_entry(self, item_id: int):
        """Synchronize data for a single item"""
        e = self[item_id]
        e.load_transactions()
        e.execute_all()
        
    def sync_inventory_table(self):
        """Synchronize the executed table with the transaction table by adding a row for each transaction"""
        db = TransactionDatabase(self.db_file)
        
        db.execute(self.sql_create_table_results)

        con = self.con_writeable
        con.row_factory = lambda c, row: row[0]
        sql = self.sql_insert_row
        item_ids = con.execute("""SELECT DISTINCT item_id FROM "transaction" ORDER BY item_id ASC""").fetchall()
        for t in db.load_transactions():
            con.execute(sql, tuple([t.transaction.__getattribute__(a) for a in Transaction.__match_args__]))
        con.commit()
        con.close()
        
        for next_item in item_ids:
            ie = self[next_item]
            ie.execute_all()
        
    def __getitem__(self, item_id: int) -> InventoryEntry:
        """Fetch the InventoryEntry that represents item with item_id=`item_id`"""
        e = self.entries[item_id]
        
        if e is None:
            con = self.con_read_only
            if con.execute("""SELECT COUNT(*) FROM "transaction" WHERE item_id=?""", (item_id,)).fetchone()[0] == 0:
                e = f"Unable to fetch Transactions for item {go.id_name[item_id]}, as none are submitted..."
                raise RuntimeError(e)
            else:
                e = InventoryEntry(item_id)
                self.entries[item_id] = e
        return e
    
    def get_entry_at_timestamp(self, item_id: int, timestamp: int = None) -> (Dict[str, int | str] | None):
        """
        Fetch the most recent data for `item_id` at timestamp `timestamp`
        
        Parameters
        ----------
        item_id : int
            item_id of the item
        timestamp : int, optional, None by default
            The timestamp. If not passed, use the current timestamp instead.

        Returns
        -------
        None
            If no data is available, return None
        Dict[str, int | str]
            If one or more records are found, return the most recent one.

        """
        
        if timestamp is None:
            timestamp = int(time.time())
        
        con = self.con_read_only
        con.row_factory = lambda c, row: {labels[0]: row[idx] for idx, labels in enumerate(c.description)}
        return con.execute(f"""SELECT * FROM "transaction" WHERE item_id=? AND timestamp <= ? ORDER BY timestamp DESC""", (item_id, timestamp)).fetchone()
    
        
        
        




if __name__ == "__main__":
    import os
    import global_variables.path as gp
    _done = []
    
    i = Inventory()
    i.sync_inventory_table()
    exit(1)
    
    # con = sqlite3.connect(os.path.join(gp.dir_data, "local_test.db"))
    # for a in InventoryEntry.cumulative_attributes:
    #     con.execute(f"""UPDATE 'transaction' SET {a}=0""")
    # con.commit()
    t0 = time.perf_counter()
    results = []
    for item_id in go.item_ids:
        ie = InventoryEntry(item_id=item_id, transaction_database=File(os.path.join(gp.dir_data, "local_test.db")))
        # print(ie)
        # print(ie.last_submit)
        # break
        ie.execute_all()
        if ie.profit != 0:
            print(go.id_name[ie.item_id], ie.profit)
            results.append((go.id_name[ie.item_id], ie.profit))
    print(np.sum([i[1] for i in results]))
    print(passed_pc(t0))
