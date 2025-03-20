"""
Module for TransactionDatabase class that is used to interact with the transaction database

Identifying measurement errors;
exchange logger:
{'date': '2025-03-04', 'time': '02:53:09', 'state': 'BOUGHT', 'slot': 6, 'item': 5290, 'qty': 200, 'worth': 21995, 'max': 200, 'offer': 153}
- Has an offered price instead of a price, which is lower/higher, depending on transaction type
- This example illustrates very clearly that price = floor(worth / qty)

runelite export:
{ "item_id": 5290, "timestamp": 1741053189, "is_buy": 1, "price": 109, "quantity": 200, "value": 21800, "account_name": "dogadon" },
- Value was manually computed with p*q
- timestamp is lower than or equal to other 2 sources


flipping utilities:
{ "b": true, "beforeLogin": false, "cQIT": 200, "id": 5290, "p": 109, "s": 6, "st": "BOUGHT", "t": 1741053189000,
"tAA": 14255, "tQIT": 200, "tSFO": 2, "tradeStartedAt": 1741053188662, "uuid": "3cc359a9-41cb-4443-841d-12c8471a53a6" },

In summary;
price, quantity, value
exchange_logger price: NA   quantity: 200   value: 21995   offered price: 153
runelite export price: 109   quantity: 200   value: NA
flipping utilities price: 109   quantity: 200   value: NA

This example illustrates that the average price is always floored, as 110 ea would have a value of 22000, while the
price was set to 109 (which suggests a value of 200*109=21800)
NOTE: value of Runelite export is set equal to price * quantity, there may be a rounding error as this is not equal to
the amount of GP spent, the price is correct, however. Timestamp may also differ for Runelite exports.

"""
import sqlite3

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Dict

import global_variables.path as gp
from transaction.constants import update_timestamp, raw_transaction_keys
from transaction.database import *
from transaction.database import BasicTransactionDatabase
from transaction.raw import factory_raw_transaction, RawTransactionEntry, resolve_duplicates, RuneliteExportEntry, \
    runelite_export, ExchangeLoggerEntry, FlippingUtilitiesEntry, flipping_utilities, exchange_logger
from transaction.row_factories import _factory_idx0, factory_dict
from transaction.sql import sql_insert, sql_select, sql_update, sql_delete
from transaction.sql.update import sql_update_transaction
from transaction.transaction_model.transaction_entry import Transaction, factory_transaction


@dataclass(slots=True)
class TransactionDatabase(BasicTransactionDatabase):#, RawTransactionDatabase):
    """
    Class for interacting with the transaction database.
    
    Default connection configurations;
    - journal_mode to WAL for better write concurrency and crash recovery.
    - synchronous to FULL for maximum data integrity.
    - temp_store to MEMORY to leverage available RAM.
    - cache_size to roughly 4GB (using a negative value sets the size in kilobytes).
    - foreign_keys enabled to enforce foreign key constraints.
    
    Note that you can override them by passing keyword-args to connect().
    
    Notes
    -----
    Transactions from the legacy database can be imported via db.import_transactions. These transactions should be
    submitted first.
    New transactions are added by inserting the raw transaction from one of the sources (exchange logger / flipping
    utilities / runelite export). These transactions are then submitted as raw transaction. Whenever a raw transaction
    is submitted, the transaction is also checked if it has an already existing counterpart. If so, they are merged,
    prioritizing values from certain sources over others.
    After submitting/merging the raw transaction data, the raw transaction will be submitted to the transaction table
    under certain circumstances (i.e. transaction filter mechanism to prevent certain submissions like price probes to
    be registered as actual transaction).
    The transaction data should be used for computing the inventory and such.
    """
    
    path: str = field(default=str(gp.f_db_transaction_new))
    """Path to the database"""
    
    def insert_transaction(self, transaction: Transaction):
        """Insert `transaction` into the transaction table"""
        con = self.connect()
        try:
            sql, params = transaction.insert()
            con.execute(sql, params)
            con.commit()
        finally:
            # con.close()
            ...
    
    def sync_transaction(self, transaction_id: int, force_sync: bool = False):
        """Sync a Transaction with its raw counterpart"""
        con = self.connect()
        c = con.cursor()
        c.row_factory = factory_transaction
        t = c.execute(sql_select.transaction_by_transaction_id, (transaction_id,)).fetchone()
        if t is None:
            raise ValueError(f"Transaction with id={transaction_id} does not exist")
        
        # By default, only update if the tag from transaction differs from the raw transaction
        if force_sync:
            sql = sql_select.count_raw_transaction_by_transaction_id
            params = (t.raw_transaction_id,)
        else:
            sql = sql_select.count_raw_transaction_by_transaction_id
            params = (t.tag, t.raw_transaction_id)
        
        if not con.execute(sql, params).fetchone()[0]:
            return
        
        sql = sql_update_transaction
        con.execute(sql)
        con.commit()
        # con.close()
    
    def import_transactions(self, *transactions: Dict[str, int | str | float | None],
                            keys: Sequence[str] = raw_transaction_keys):
        """
        Import transactions from another table into the raw_transaction table
        
        Parameters
        ----------
        transactions : Dict[str, int | str | float | None]
            Transactions that are to be imported, as dicts
        keys : Sequence[str]
            The keys to extract from the imported transactions
        """
        
        conn = self.connect()
        c = conn.cursor()
        for t in transactions:
            raw_id, _count = c.execute(sql_select.check_if_exists, t).fetchone()
            if _count == 0:
                _t = {k: t.get(k) for k in keys}
                _t['value'] = None
                _t['tag'] = t.get('tag')
                _t['update_timestamp'] = _t.pop('update_ts')
                conn.execute(sql_insert.insert_raw_transaction_from_import, _t)
            else:
                sql = """UPDATE raw_transaction SET update_timestamp=?, tag=?, status=? WHERE transaction_id=?"""
                conn.execute(sql_update.raw_transaction_from_import_update, (t['update_ts'], t.get('tag'), t.get('status', 1), raw_id))
                print(f"Skipped {_count} {t}")
        conn.commit()
        self.sync_raw_table()
    
    @staticmethod
    def reset_database():
        """
        Reset the transaction database. Clear all transaction rows, then import from local database, followed by imports
        from raw entries.
        
        Exit upon completing the reset.
        """
        db = TransactionDatabase()
        print('\n' * 50)
        if input('Warning! Proceeding is likely to erase data that may not be easy to recover. '
                 'Are you sure? "yes" to confirm: ').lower() == 'yes':
            if input('Really??? "yes" to confirm: ').lower() == 'yes':
                ...
            else:
                raise RuntimeError("Aborted operation")
        else:
            raise RuntimeError("Aborted operation")
        
        conn = db.connect()
        c = conn.cursor()
        c.row_factory = _factory_idx0
        tables = c.execute(sql_select.table_names).fetchall()
        tables = [s for s in tables if "transaction" in s]
        tables = sorted(tables, key=len)
        
        for t in tables:
            conn.execute(sql_delete.all_rows(str(t)))
            conn.commit()
        conn.execute("UPDATE sqlite_sequence SET seq=0")
        conn.commit()
        conn.execute("VACUUM")
        print("All rows have been cleared!")
        conn.close()
        old_con = sqlite3.connect(gp.f_db_local)
        old_con.row_factory = factory_dict
        db.import_transactions(*old_con.execute(sql_select.all_transactions).fetchall(),
                               keys=('item_id', 'timestamp_created', 'timestamp',
                                     'timestamp_runelite_export', 'is_buy', 'quantity', 'max_quantity',
                                     'price', 'offered_price', 'shbjvjh', 'account_name', 'ge_slot', 'status', 'tag',
                                     'update_ts'))
    
    def insert_flipping_utilities_transaction(self, entry: FlippingUtilitiesEntry, **kwargs):
        """
        Adds a new raw runelite export transaction and adds/updates a combined raw transaction.
        Expected kwargs keys include:
            item_id, timestamp, is_buy, quantity, price,
            account_id, update_timestamp
        """
        conn = kwargs.get('connection', self.connect())
        cur = conn.cursor()
        cur.row_factory = _factory_idx0
        
        try:
            # Insert into raw_runelite_export_transaction table.
            sql = entry.sql_insert
            params = entry.sql_params
            
            has_entry = cur.execute(entry.sql_count, params[:-1]).fetchone()
            
            if has_entry:
                return
            
            cur.execute(sql, params)
            
            if entry.quantity == 0:
                return
            
            raw_id = cur.lastrowid
            
            _ids = flipping_utilities(cur, entry)
            
            if len(_ids) > 0:
                _ids = tuple(_ids.values())
                if len(set(_ids)) > 1:
                    _ids = resolve_duplicates(conn, _ids)
                
                cur.execute(sql_update.raw_transaction_from_flipping_utilities_transaction_dict,
                            {
                                "timestamp": entry.timestamp,
                                "timestamp_created": entry.timestamp_created,
                                "max_quantity": entry.max_quantity,
                                "ge_slot": None if entry.ge_slot == -1 else entry.ge_slot,
                                "account_name": entry.account_name,
                                "update_timestamp": update_timestamp,
                                "flipping_utilities_id": raw_id,
                                "transaction_id": _ids[0]
                            })
            else:
                cur.execute(sql_insert.insert_raw_transaction_dict,
                            {
                                "item_id": entry.item_id,
                                "timestamp_created": entry.timestamp_created,
                                "timestamp": entry.timestamp,
                                "is_buy": entry.is_buy,
                                "quantity": entry.quantity,
                                "max_quantity": entry.max_quantity,
                                "price": entry.price,
                                "account_name": entry.account_name,
                                "ge_slot": None if entry.ge_slot == -1 else entry.ge_slot,
                                "status": 1,
                                "tag": None,
                                "update_timestamp": update_timestamp,
                                "raw_id": raw_id,
                            })
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            # cur.close()
            # conn.close()
            ...
    
    def insert_exchange_logger_transaction(self, entry: ExchangeLoggerEntry, **kwargs):
        """
        Adds a new raw runelite export transaction and adds/updates a combined raw transaction.
        Expected kwargs keys include:
            item_id, timestamp, is_buy, quantity, price,
            account_id, update_timestamp
        """
        conn = kwargs.get('connection', self.connect())
        cur = conn.cursor()
        cur.row_factory = _factory_idx0
        
        try:
            # Insert into raw_runelite_export_transaction table.
            sql = entry.sql_insert
            params = entry.sql_params
            
            has_entry = cur.execute(entry.sql_count, params[:-1]).fetchone()
            
            if has_entry:
                return
            
            cur.execute(sql, params)
            
            if entry.quantity == 0:
                return
            
            raw_id = cur.lastrowid
            _ids = tuple(exchange_logger(cur, entry).values())
            if len(set(_ids)) > 1:
                
                _ids = resolve_duplicates(conn, _ids)
                update_sql = sql_update.raw_transaction_from_exchange_logger_dict
                cur.execute(update_sql, {
                    "timestamp": entry.timestamp,
                    "offered_price": entry.offered_price,
                    "value": entry.value,
                    "max_quantity": entry.max_quantity,
                    "ge_slot": entry.ge_slot,
                    "update_timestamp": update_timestamp,
                    "tag": "E",
                    "exchange_logger_id": raw_id,
                    "transaction_id": _ids[0]
                })
            else:
                cur.execute(sql_insert.insert_raw_exchange_logger_transaction_dict, {
                    "item_id": entry.item_id,
                    "timestamp": entry.timestamp,
                    "is_buy": entry.is_buy,
                    "quantity": entry.quantity,
                    "max_quantity": entry.max_quantity,
                    "price": entry.price,
                    "offered_price": entry.offered_price,
                    "value": entry.value,
                    "ge_slot": entry.ge_slot,
                    "status": 1,
                    "optional_field": None,
                    "update_timestamp": update_timestamp,
                    "raw_id": raw_id,
                })
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            # cur.close()
            # conn.close()
            ...
    
    def insert_runelite_export_transaction(self, entry: RuneliteExportEntry, **kwargs):
        """
        Adds a new raw runelite export transaction and adds/updates a combined raw transaction.
        Expected kwargs keys include:
            item_id, timestamp, is_buy, quantity, price,
            account_id, update_timestamp
        """
        conn = kwargs.get('connection', self.connect())
        cur = conn.cursor()
        cur.row_factory = _factory_idx0
        
        try:
            # Insert into raw_runelite_export_transaction table.
            sql = entry.sql_insert
            params = entry.sql_params
            
            has_entry = cur.execute(entry.sql_count, params[:-1]).fetchone()
            
            if has_entry:
                return
            
            cur.execute(sql, params)
            
            # If the transaction has 0 quantity, do not submit to the raw_table
            if entry.quantity == 0:
                return
            
            raw_id = cur.lastrowid
            _ids = tuple(runelite_export(cur, entry).values())
            if len(_ids) > 0:
                if len(set(_ids)) > 1:
                    _ids = resolve_duplicates(conn, _ids)
                
                # Update the merged row to add the runelite_export_id if not already set.
                try:
                    cur.execute(sql_update.raw_transaction_from_runelite_export_transaction_dict,
                                {"timestamp": entry.timestamp, "account_name": entry.account_name,
                                 "update_timestamp": entry.update_timestamp, "runelite_export_id": raw_id, "transaction_id": _ids[0]})
                except sqlite3.OperationalError as e:
                    ...
                    raise e
            else:
                # Insert a new merged transaction row. Status is set to 1, tag to "R"
                insert_sql = sql_insert.insert_raw_transaction_runelite_export_dict
                params = {
                    "item_id": entry.item_id,
                    "timestamp": entry.timestamp,
                    "timestamp_runelite_export": entry.timestamp,
                    "is_buy": entry.is_buy,
                    "quantity": entry.quantity,
                    "update_timestamp": update_timestamp,
                    "price": entry.price,
                    "runelite_export_id": raw_id,
                    "account_name": entry.account_name,
                }
                cur.execute(insert_sql, params)
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            cur.close()
            conn.close()
            ...
    
    def can_submit(self, t: RawTransactionEntry) -> bool:
        """If True, this transaction may be submitted to the transaction table"""
        return t.quantity > 1 or t.item_id == 13190
    
    def submit_raw_transaction(self, raw: RawTransactionEntry):
        """Convert the RawTransactionEntry to a TransactionEntry and insert it"""
        if self.can_submit(raw):
            transaction = Transaction.from_raw(raw)
            con = self.connect()
            
            if transaction.tag is None:
                transaction.tag = "L"
            
            sql, params = transaction.insert()
            con.execute(sql, params)
            con.commit()
    
    def sync_raw_table(self):
        """
        Iterate over all raw_transaction table entries and submit those that are eligible into the transaction table.
        The submitted transactions are linked to the sourcing raw_transaction via a foreign_key. The idea is that both
        remain synced to some extent.
        """
        con = self.connect()
        c = con.cursor()
        c.row_factory = _factory_idx0
        
        raw_ids = c.execute("""SELECT transaction_id FROM raw_transaction
        WHERE transaction_id NOT IN
        ( SELECT raw_transaction_id FROM 'transaction' ); """).fetchall()
        
        c.row_factory = factory_raw_transaction
        n_added = 0
        for transaction_id in raw_ids:
            transaction = c.execute(sql_select.raw_transaction_by_transaction_id,
                                    (transaction_id,)).fetchone()
            try:
                if self.can_submit(transaction):
                    self.submit_raw_transaction(transaction)
                    n_added += 1
            except TypeError as e:
                print(transaction)
                raise e
        print(f"Added {n_added} transactions")
#

# import json
#
# def json_to_list_of_rows(json_file):
#     # Load the JSON data from the file
#     with open(json_file, 'r') as f:
#         data = json.load(f)
#     # Convert the JSON data to a list of rows
#     rows = []
#     keys = tuple(data.keys())
#     for cur in zip(*[data.get(k).values() for k in keys]):
#         print(cur)
#         rows.append({k: v//1000 if k == "parse_time" else v for k, v in zip(keys, cur)})
#     print(len(rows))
#     return rows


# Example usage
# path = r'C:\Users\Max Moons\Documents\GitHub\OSRS-GE-Ledger\data\json_file.txt'
# json.dump(json_to_list_of_rows(path), open(os.path.splitext(path)[0] + '.json', 'w'), indent=2)
# print(rows)


