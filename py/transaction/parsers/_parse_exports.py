"""
Module that imports parsers per source and combines them.
Recommended usage is to import via the parsers package.
"""

import time

from transaction.parsers._parse_runelite_json_exports import merge_runelite_exports
from transaction.parsers._parse_exchange_logger_exports import merge_exchange_logger_exports
from transaction.row_factories import _factory_idx0
from transaction.parsers._parse_flipping_utilities_exports import merge_flipping_utilities_exports
from transaction.database.transaction_database import TransactionDatabase


# from _parse_flipping_utilities_exports import merge_flipping_utilities_exports


def parse_exports(runelite_exports: bool = True, exchange_logger: bool = True, flipping_utilities: bool = True,
                  sync_transactions: bool = True, apply_min_timestamp: bool = True):
    """Parses transaction exports from all three sources using default parameters"""
    transaction_database = TransactionDatabase()
    
    t0 = time.time()
    if apply_min_timestamp:
        conn = transaction_database.connect(read_only=True)
        min_el_ts, min_fu_ts, min_re_ts = conn.execute("""SELECT (SELECT MAX(timestamp) FROM raw_exchange_logger_transaction),
        (SELECT MAX(timestamp) FROM raw_flipping_utilities_transaction), (SELECT MAX(timestamp) FROM raw_runelite_export_transaction)""").fetchone()
    else:
        min_el_ts, min_fu_ts, min_re_ts = None, None, None
    
    # for ts in (min_el_ts, min_fu_ts, min_re_ts):
    #     print(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
    # exit(1)
    if runelite_exports:
        t_exports = time.perf_counter()
        
        runelite_exports = merge_runelite_exports(min_ts=min_re_ts)
        con = transaction_database.connect(read_only=True)
        c = con.cursor()
        c.row_factory = _factory_idx0
        for e in runelite_exports:
            if c.execute("SELECT COUNT(*) FROM raw_runelite_export_transaction WHERE "
                         "item_id=? AND timestamp=? AND is_buy=? AND quantity=? AND price=? AND account_name=?",
                         (e.item_id, e.timestamp, e.is_buy, e.quantity, e.price, e.account_name)).fetchone():
                continue
            
            transaction_database.insert_runelite_export_transaction(e)
        t_passed = time.perf_counter()-t_exports
        print(f"\t* Parsed runelite exports in {t_passed:.1f} seconds")
        
    if exchange_logger:
        t_exchange = time.perf_counter()
        exchange_logger_exports = merge_exchange_logger_exports(min_ts=min_el_ts)
        con = transaction_database.connect(read_only=True)
        c = con.cursor()
        c.row_factory = _factory_idx0
        for e in exchange_logger_exports:
            if c.execute("SELECT COUNT(*) FROM raw_exchange_logger_transaction WHERE "
                         "item_id=? AND timestamp=? AND is_buy=? AND quantity=? AND price=? AND ge_slot=?",
                         (e.item_id, e.timestamp, e.is_buy, e.quantity, e.price, e.ge_slot)).fetchone():
                continue
            
            transaction_database.insert_exchange_logger_transaction(e)
        t_passed = time.perf_counter()-t_exchange
        print(f"\t* Parsed exchange logger exports in {t_passed:.1f} seconds")
    
    if flipping_utilities:
        t_flip = time.perf_counter()
        
        con = transaction_database.connect(read_only=True)
        c = con.cursor()
        c.row_factory = _factory_idx0
        
        uuids = c.execute("SELECT uuid FROM raw_flipping_utilities_transaction").fetchall()
        c.close()
        flipping_utilities_exports = merge_flipping_utilities_exports()
        for e in flipping_utilities_exports:
            
            # Flipping utilities use UUID instead of min_ts
            if e.uuid in uuids:
                continue
            transaction_database.insert_flipping_utilities_transaction(e)
        t_passed = time.perf_counter()-t_flip
        print(f"\t* Parsed flipping utility exports in {t_passed:.1f} seconds")
    
    if sync_transactions:
        t_sync = time.perf_counter()
        transaction_database.sync_raw_table()
        t_passed = time.perf_counter()-t_sync
        print(f"\t* Synced transactions in {t_passed:.1f} seconds")
    
    print(f"Parsed all exports in {time.time()-t0:.1f} seconds")

parse_exports()
exit(1)