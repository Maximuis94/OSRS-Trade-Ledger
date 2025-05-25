"""
Module with various methods for finding associated raw transactions.
There are functions for attempting to find a corresponding raw transaction based on a specific combination
of transactions, as well as a merged function, that will make a call to both.

It is recommended to use the merged function.
"""
from collections import namedtuple

import sqlite3
from typing import Optional, Tuple, List, Dict

from transaction.constants import delta_t, empty_tuple
from transaction.raw.raw_exchange_logger_entry import ExchangeLoggerEntry
from transaction.raw.raw_flipping_utilities_entry import FlippingUtilitiesEntry
from transaction.raw.raw_runelite_export_entry import RuneliteExportEntry
from transaction.row_factories import factory_dict


def exchange_logger_flipping_utilities(cur: sqlite3.Cursor, t: ExchangeLoggerEntry) -> Optional[int]:
    """Returns SQL to find an identical transaction with FlippingUtilities data, given an ExchangeLogger transaction"""
    sql = """SELECT transaction_id FROM "raw_transaction"
    WHERE item_id=? AND quantity=? AND timestamp=? AND is_buy=? AND price=? AND max_quantity=? AND (ge_slot IS NULL OR ge_slot=-1 OR ge_slot=?)"""
    values = (t.item_id, t.quantity, t.timestamp, t.is_buy, t.price, t.max_quantity, t.ge_slot)
    return cur.execute(sql, values).fetchone()


def exchange_logger_runelite_export(cur: sqlite3.Cursor, t: ExchangeLoggerEntry) -> Optional[int]:
    """Returns SQL to find an identical transaction with RuneliteExport data, given an ExchangeLogger transaction"""
    sql = """SELECT transaction_id FROM "raw_transaction"
    WHERE item_id=? AND quantity=? AND timestamp BETWEEN ? AND ? AND is_buy=? AND price=?"""
    values = (t.item_id, t.quantity, t.timestamp-delta_t, t.timestamp, t.is_buy, t.price)
    return cur.execute(sql, values).fetchone()


def flipping_utilities_exchange_logger(cur: sqlite3.Cursor, t: FlippingUtilitiesEntry) -> Optional[int]:
    """Returns SQL to find an identical transaction with ExchangeLogger data, given a Flipping Utilities transaction"""
    sql = """SELECT transaction_id FROM "raw_transaction"
    WHERE item_id=? AND quantity=? AND timestamp=? AND is_buy=? AND price=? AND max_quantity=?"""
    values = (t.item_id, t.quantity, t.timestamp, t.is_buy, t.price, t.max_quantity)
    return cur.execute(sql, values).fetchone()


def flipping_utilities_runelite_export(cur: sqlite3.Cursor, t: FlippingUtilitiesEntry) -> Optional[int]:
    """Returns SQL to find an identical transaction with Runelite Export data, given a Flipping Utilities transaction"""
    sql = """SELECT transaction_id FROM "raw_transaction"
    WHERE item_id=? AND quantity=? AND timestamp BETWEEN ? AND ? AND is_buy=? AND price=? AND account_name=?"""
    values = (t.item_id, t.quantity, t.timestamp-delta_t, t.timestamp, t.is_buy, t.price, t.account_name)
    return cur.execute(sql, values).fetchone()


def runelite_export_exchange_logger(cur: sqlite3.Cursor, t: RuneliteExportEntry) -> Optional[int]:
    """Returns SQL to find an identical transaction with Exchange Logger data, given a Runelite Export transaction"""
    sql = """SELECT transaction_id FROM "raw_transaction"
    WHERE item_id=? AND quantity=? AND timestamp BETWEEN ? AND ? AND is_buy=? AND price=?"""
    values = (t.item_id, t.quantity, t.timestamp-delta_t, t.timestamp, t.is_buy, t.price)
    return cur.execute(sql, values).fetchone()


def runelite_export_flipping_utilities(cur: sqlite3.Cursor, t: RuneliteExportEntry) -> Optional[int]:
    """Returns SQL to find an identical transaction with Flipping Utilities data, given a Runelite Export transaction"""
    sql = """SELECT transaction_id FROM "raw_transaction"
    WHERE item_id=? AND quantity=? AND timestamp BETWEEN ? AND ? AND is_buy=? AND price=? AND account_name=?"""
    values = (t.item_id, t.quantity, t.timestamp, t.timestamp+delta_t, t.is_buy, t.price, t.account_name)
    return cur.execute(sql, values).fetchone()


def exchange_logger(cur: sqlite3.Cursor, t: ExchangeLoggerEntry) -> dict:
    """
    Attempt to find an associated RawTransaction based on `transaction`, which is an entry from the raw_exchange_logger
    table.
    
    Parameters
    ----------
    cur : sqlite3.Cursor
        Cursor from a connection to the associated database
    t :ExchangeLoggerEntry
        Reference transaction that will be used to determine if there is an associated transaction

    Returns
    -------
    dict
        If None, there is no matching entry in the RawTransaction table. Otherwise, the integer is the transaction_id of
        the matching entry in the raw_transaction table
    """
    ids = {}
    el = cur.execute('SELECT transaction_id FROM raw_transaction WHERE exchange_logger_id=?', (t.transaction_id,)).fetchone()
    if el is not None:
        ids[ExchangeLoggerEntry] = el
    el = exchange_logger_flipping_utilities(cur, t)
    if el is not None:
        ids[FlippingUtilitiesEntry] = el
    el = exchange_logger_runelite_export(cur, t)
    if el is not None:
        ids[RuneliteExportEntry] = el
    return ids


def flipping_utilities(cur: sqlite3.Cursor, t: FlippingUtilitiesEntry) -> dict:
    """
    Attempt to find an associated RawTransaction based on `transaction`, which is an entry from the
    raw_flipping_utilities table.

    Parameters
    ----------
    cur : sqlite3.Cursor
        Cursor from a connection to the associated database
    t : FlippingUtilitiesEntry
        Reference transaction that will be used to determine if there is an associated transaction

    Returns
    -------
    dict
        If None, there is no matching entry in the RawTransaction table. Otherwise, the integer is the transaction_id of
        the matching entry in the raw_transaction table
        """
    ids = {}
    el = cur.execute('SELECT transaction_id FROM raw_transaction WHERE flipping_utilities_id=?', (t.transaction_id,)).fetchone()
    if el is not None:
        ids[FlippingUtilitiesEntry] = el
    el = flipping_utilities_exchange_logger(cur, t)
    if el is not None:
        ids[ExchangeLoggerEntry] = el
    el = flipping_utilities_runelite_export(cur, t)
    if el is not None:
        ids[RuneliteExportEntry] = el
    return ids


def runelite_export(cur: sqlite3.Cursor, t: RuneliteExportEntry) -> dict:
    """
    Attempt to find an associated RawTransaction based on `transaction`, which is an entry from the
    raw_runelite_export table.

    Parameters
    ----------
    cur : sqlite3.Cursor
        Cursor from a connection to the associated database
    t : RuneliteExportEntry
        Reference transaction that will be used to determine if there is an associated transaction

    Returns
    -------
    dict
        If None, there is no matching entry in the RawTransaction table. Otherwise, the integer is the
        transaction_id of the matching entry in the raw_transaction table
    """
    ids = {}
    el = cur.execute('SELECT transaction_id FROM raw_transaction WHERE runelite_export_id=?', (t.transaction_id,)).fetchone()
    if el is not None:
        ids[RuneliteExportEntry] = el
    el = runelite_export_exchange_logger(cur, t)
    if el is not None:
        ids[ExchangeLoggerEntry] = el
    el = runelite_export_flipping_utilities(cur, t)
    if el is not None:
        ids[FlippingUtilitiesEntry] = el
    return ids


_tags = {
    "efr": "A",
    "ef": "B",
    "er": "C",
    "fr": "D"
}

new_tags = tuple(list(_tags.keys()) + list(_tags.values()))
legacy_tags = ("X", "b", "c", "e", "m", "p")

def update_tag(values_dict: Dict[str, any]) -> Dict[str, any]:
    """Update the tag in the values list"""
    tag = values_dict.get('tag')
    if tag in legacy_tags:
        return values_dict
    chars = ""
    
    if values_dict.get('exchange_logger_id') is not None:
        chars += "E"
    if values_dict.get('flipping_utilities_id') is not None:
        chars += "F"
    if values_dict.get('runelite_export_id') is not None:
        chars += "R"
    
    # Legacy transaction
    if len(chars) == 0:
        values_dict['tag'] = "L"
    elif len(chars) == 1:
        values_dict['tag'] = chars
    else:
        values_dict['tag'] = _tags[chars.lower()]
    return values_dict


def merge_transactions(conn: sqlite3.Connection, t_id_a: int, t_id_b: int) -> Optional[int]:
    """
    Merges two transactions in the raw_transaction table, prioritizing data from flipping_utilities, then
    exchange_logger, and finally runelite_export.
    Due to unique and foreign key constraints, this function may have become a bit overcomplicated

    
    Parameters
    ----------
    conn : sqlite3.Connection
    t_id_a : int
        transaction_id of the first transaction
    t_id_b : int
        transaction_id of the second transaction

    Returns
    -------
    

    """
    #     raw_transaction_columns = (
    #     "transaction_id",
    #     "item_id",
    #     "timestamp_created",
    #     "timestamp",
    #     "timestamp_runelite_export",
    #     "is_buy",
    #     "quantity",
    #     "max_quantity",
    #     "price",
    #     "offered_price",
    #     "value",
    #     "account_name",
    #     "ge_slot",
    #     "update_timestamp",
    #     "exchange_logger_id",
    #     "flipping_utilities_id",
    #     "runelite_export_id"
    # )
    sql = "NONE"
    params = empty_tuple
    try:
        cursor = conn.cursor()
        
        # Fetch transactions
        cursor.execute("SELECT * FROM raw_transaction WHERE transaction_id = ?", (t_id_a,))
        transaction1 = cursor.fetchone()
        cursor.execute("SELECT * FROM raw_transaction WHERE transaction_id = ?", (t_id_b,))
        transaction2 = cursor.fetchone()
        
        if transaction1 is None or transaction2 is None:
            print("One or both transaction IDs not found.")
            return
        
        # Determine priority
        priority1 = 4
        priority2 = 4
        
        if transaction1[-2]:  # flipping_utilities_id
            priority1 = 3
        elif transaction1[-3]:  # exchange_logger_id
            priority1 = 2
        elif transaction1[-1]:  # runelite_export_id
            priority1 = 1
        
        if transaction2[-2]:  # flipping_utilities_id
            priority2 = 3
        elif transaction2[-3]:  # exchange_logger_id
            priority2 = 2
        elif transaction2[-1]:  # runelite_export_id
            priority2 = 1
        
        # Determine which transaction to keep and which to merge
        keep_transaction = transaction1 if priority1 >= priority2 else transaction2
        merge_transaction = transaction2 if priority1 >= priority2 else transaction1
        
        raw_keys = (
            "transaction_id",
            "item_id",
            "timestamp_created",
            "timestamp",
            "timestamp_runelite_export",
            "is_buy",
            "quantity",
            "max_quantity",
            "price",
            "offered_price",
            "value",
            "account_name",
            "ge_slot",
            "status",
            "tag",
            "update_timestamp",
            "exchange_logger_id",
            "flipping_utilities_id",
            "runelite_export_id",
        )
        keep_data_dict = update_tag({k: v for k, v in zip(raw_keys, merge_transaction)})
        merged_data_dict = update_tag({k: v for k, v in zip(raw_keys, keep_transaction)})
        
        # Merge data (prioritizing)
        # merged_data = list(keep_transaction)
        merged_data = {}
        for i, k in enumerate(raw_keys):
            v_m, v_k = merged_data_dict.get(k), keep_data_dict.get(k)
            if k == "tag":
                cur = None
                for tag in (v_m, v_k):
                    if tag in legacy_tags:
                        cur = tag
                        break
                if cur is None:
                    cur = v_m
                merged_data[k] = cur
            else:
                merged_data[k] = v_k if v_m is None else v_m
        
        # Update the kept transaction
        sql = """
            UPDATE 'transaction' SET
                item_id=:item_id,
                timestamp_created=:timestamp_created,
                timestamp=:timestamp,
                is_buy=:is_buy,
                quantity=:quantity,
                max_quantity=:max_quantity,
                price=:price,
                offered_price=:offered_price,
                value=:value,
                ge_slot=:ge_slot,
                account_name=:account_name,
                status=:status,
                tag=:tag,
                update_timestamp=:update_timestamp
            WHERE transaction_id=:transaction_id
        """
        # params = (
        #     merged_data[1],  # item_id
        #     merged_data[2],  # timestamp_created
        #     merged_data[3],  # timestamp
        #     merged_data[5],  # is_buy
        #     merged_data[6],  # quantity
        #     merged_data[7],  # max_quantity
        #     merged_data[8],  # price
        #     merged_data[9],  # offered_price
        #     merged_data[10],  # value
        #     merged_data[12],  # ge_slot
        #     merged_data[11],  # account_name
        #     merged_data[13],  # status
        #     merged_data[14],  # tag
        #     merged_data[15],  # update_timestamp
        #     keep_transaction[0],  # transaction_id
        # )
        
        conn.execute(sql, merged_data)
        
        sql = "SELECT COUNT(*) FROM 'transaction' WHERE raw_transaction_id = ?"
        params = (keep_transaction[0],)
        if cursor.execute(sql, params).fetchone()[0]:
            sql = "DELETE FROM 'transaction' WHERE raw_transaction_id = ?"
            params = (merge_transaction[0],)
        else:
            sql = "UPDATE 'transaction' SET raw_transaction_id=? WHERE raw_transaction_id=?"
            params = (keep_transaction[0], merge_transaction[0])
        conn.execute(sql, params)
        
        sql = "DELETE FROM 'raw_transaction' WHERE transaction_id=?"
        params = (merge_transaction[0],)
        conn.execute(sql, params)
        
        
        
        # Update the kept transaction
        sql = """
            UPDATE raw_transaction
    SET
        item_id=:item_id,
        timestamp_created=:timestamp_created,
        timestamp=:timestamp,
        timestamp_runelite_export=:timestamp_runelite_export,
        is_buy=:is_buy,
        quantity=:quantity,
        max_quantity=:max_quantity,
        price=:price,
        offered_price=:offered_price,
        value=:value,
        account_name=:account_name,
        ge_slot=:ge_slot,
        status=:status,
        tag=:tag,
        update_timestamp=:update_timestamp,
        exchange_logger_id=:exchange_logger_id,
        flipping_utilities_id=:flipping_utilities_id,
        runelite_export_id=:runelite_export_id
    WHERE transaction_id=:transaction_id
        """
        conn.execute(sql, merged_data)
        
        # Delete the merged transaction
        conn.execute("DELETE FROM 'transaction' WHERE transaction_id = ?", (merge_transaction[0],))
        conn.execute("DELETE FROM raw_transaction WHERE transaction_id = ?", (merge_transaction[0],))
        
        conn.commit()
        print(f"Transactions {t_id_a} and {t_id_b} merged into {keep_transaction[0]}.")
        return keep_transaction[0]
    except Exception as e:
        conn.rollback()
        msg = (f"Failed to merge transaction {t_id_a} and {t_id_b}\n"
               f"SQL={sql}\n"
               f"parameters={', '.join([str(s) for s in params])}")
        e.add_note(msg)
        raise e
    finally:
        # conn.close()
        ...


def compare_potential_duplicates(con: sqlite3.Connection, *ids) -> List[Tuple[int, int]]:
    """Compare all raw transactions, identified by their id, and return the identical pairs (i.e. they refer to the same transaction"""
    c = con.cursor()
    c.row_factory = factory_dict
    transactions = [c.execute("""SELECT * FROM "raw_transaction" WHERE transaction_id=?""", (_id,)).fetchone() for _id
                    in ids]
    
    keys = ("account_name", "ge_slot", "max_quantity", "offered_price")
    
    results = []
    for i, t1 in enumerate(transactions):
        for j, t2 in enumerate(transactions):
            is_identical = True
            if i == j:
                continue
            
            _count = 0
            for key in keys:
                if t1.get(key) is not None and t2.get(key) is not None:
                    is_identical = is_identical and t1.get(key) != t2.get(key)
                else:
                    _count += 1
            if _count == len(keys) or is_identical:
                results.append((t1['transaction_id'], t2['transaction_id']))
    
    return results


def resolve_duplicates(conn, _ids) -> Tuple[int, ...]:
    dups = compare_potential_duplicates(conn, *_ids)
    if len(dups) > 0:
        temp = []
        for (a, b) in dups:
            keep_id = merge_transactions(conn, a, b)
            if a == keep_id and b in temp:
                temp.remove(b)
            elif b == keep_id and a in temp:
                temp.remove(a)
            temp.append(keep_id)
        return tuple(temp)
    return _ids


def merge_duplicates(path: str):
    
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.row_factory = factory_dict
    rows = cursor.execute("SELECT * FROM 'raw_transaction' ORDER BY item_id ASC, timestamp ASC").fetchall()
    p = None
    for row in rows:
        if p is None:
            p = row
            continue
        if row['quantity'] == p['quantity'] and \
            row['item_id'] == p['item_id'] and \
                row['price'] == p['price'] and \
                row['timestamp'] == p['timestamp']:
            print("merging", row['transaction_id'], p['transaction_id'])
            print(row)
            print(p, '\n\n')
            merge_transactions(conn, row['transaction_id'], p['transaction_id'])
        p = row
        
    # df = pd.DataFrame()
    #
    # transaction_ids = df['transaction_id'].tolist()
    # raw_transaction_ids = df['transaction_id'].tolist()
    # df = df.groupby(df.columns.difference(['quantity', 'price']).tolist()).agg(
    #     {'quantity': 'sum', 'price': 'mean'}).reset_index()
    #
    # ids_to_delete = frozenset(transaction_ids).difference(df['transaction_id'])
    # raw_to_delete = frozenset(raw_transaction_ids).difference(df['raw_transaction_id'])


TransactionTuple = namedtuple("TransactionTuple", (
    "transaction_id",
    "item_id",
    "timestamp",
    "is_buy",
    "quantity",
    "price",
    "status",
    "tag",
    "update_ts"
))
tt_factory = lambda c, row: TransactionTuple(*row[:9])
sql_import_tags = f"""SELECT {', '.join(TransactionTuple._fields)} FROM 'transaction' LIMIT 100"""

def import_tags(src, dst):
    """
    Traverses all raw_transaction rows and merges duplicates.
    """
    db_from = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    cur_from = db_from.cursor()
    cur_from.row_factory = tt_factory
    db_to = sqlite3.connect(dst)
    
    for t in cur_from.execute(sql_import_tags).fetchall():
        # print(t, t._asdict())
        db_to.execute("UPDATE 'raw_transaction' SET tag=:tag WHERE item_id=:item_id AND timestamp=:timestamp AND quantity=:quantity AND price=:price AND is_buy=:is_buy", t._asdict())
        db_to.execute(
            "UPDATE 'transaction' SET tag=:tag WHERE raw_transaction_id",
            t._asdict())
    db_to.execute("""UPDATE "transaction"
SET tag = (
    SELECT rt.tag
    FROM raw_transaction rt
    WHERE "transaction".raw_transaction_id = rt.transaction_id
    AND rt.tag IS NOT NULL
)
WHERE EXISTS (
    SELECT 1
    FROM raw_transaction rt
    WHERE "transaction".raw_transaction_id = rt.transaction_id
    AND rt.tag IS NOT NULL
)
AND "transaction".raw_transaction_id IN (
    SELECT rt.transaction_id
    FROM raw_transaction rt
    WHERE rt.tag IS NOT NULL
);""")
        
        
    db_to.commit()