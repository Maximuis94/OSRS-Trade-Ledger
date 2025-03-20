"""
Module with logic for parsing and merging runelite json exports.
merge_runelite_exports() will have all JSON files parsed, merged and checked for duplicates.

Recommended usage is to import via the parsers package.
"""
import datetime

import json
import os
import sqlite3
from typing import Tuple, Optional, List

import global_variables.path as gp
from file.file import File
from transaction.constants import TransactionState
from transaction.raw.raw_exchange_logger_entry import ExchangeLoggerEntry
from transaction.database.transaction_database import TransactionDatabase


def _parse_exchange_log_file(path: str = r"C:\Users\Max Moons\.runelite\exchange-logger\exchange.log",
                             min_ts: int = None) -> List[ExchangeLoggerEntry]:
    """Open the file at `path`, parse its contents and convert completed lines into Transactions"""
    entries = []
    with open(path, 'r') as exchange_log:
        for next_entry in exchange_log.readlines():
            next_entry = json.loads(next_entry)
            state = TransactionState.from_str(next_entry['state'])
            if not state.is_completed or next_entry['qty'] == 0:
                continue
            if min_ts:
                next_entry["timestamp"] = int(datetime.datetime.strptime(f"{next_entry['date']} {next_entry['time']}", "%Y-%m-%d %H:%M:%S").timestamp())
                if next_entry["timestamp"] < min_ts:
                    continue
            next_entry["max_quantity"] = next_entry.pop("max")
            next_entry = ExchangeLoggerEntry.raw_entry(**next_entry)
            entries.append(next_entry)
    return entries
_parse_exchange_log_file()


def extract_timestamp(log_file: str) -> int:
    """Extract the timestamp from an archived exchange log file"""
    return int(datetime.datetime.strptime(os.path.splitext(log_file)[0].split('_')[1], '%Y-%m-%d').timestamp())


def _get_archive_logs(min_ts: Optional[int] = None) -> Tuple[str, ...]:
    """Return all log files in the exchange log archive that are newer than min_ts"""
    db = TransactionDatabase()
    con = db.connect(read_only=True)
    if min_ts is None:
        min_ts = con.execute(f"""SELECT MAX(timestamp) FROM "{ExchangeLoggerEntry.table.fget(ExchangeLoggerEntry.table)}" """).fetchone()[0]
        if min_ts is None:
            min_ts = 0
        else:
            min_ts -= min_ts % 86400
    
    output = []
    for f in os.listdir(gp.dir_exchange_log_archive):
        if extract_timestamp(f) < min_ts or not f.endswith(".log"):
            continue
        output.append(os.path.join(gp.dir_exchange_log_archive, f).replace('/', os.sep))
    output += [os.path.join(gp.dir_exchange_log_src, f) for f in os.listdir(gp.dir_exchange_log_src) if f.endswith(".log")]
    return tuple(output)


def _get_account_names(db_path: File = gp.f_db_transaction_new) -> Tuple[str, ...]:
    """Return a list of account names found in the database at `db_path`"""
    con = sqlite3.connect(db_path)
    return tuple([e[0] for e in con.execute("SELECT account_name FROM account").fetchall()])


def ts_from_log_file(log_file) -> int:
    """Extract the UNIX timestamp from an archived exchange log file"""
    dt_str = os.path.splitext(os.path.basename(log_file))[0].split('_')[-1]
    return int(datetime.datetime.strptime(dt_str, '%Y-%m-%d').timestamp())


def merge_exchange_logger_exports(min_ts: Optional[int] = None) -> List[ExchangeLoggerEntry]:
    """
    Iterate over all runelite ge export json files and merge them into a single json file without any duplicate
    transactions, while slightly reformatting the raw json files;
    - Timestamp is converted from ms to s (floored + int cast), its key is converted from time to timestamp
    - account_name is added
    - itemId is converted to item_id
    - is_buy is inserted as int and its key is converted from buy to is_buy
    
    Raw files are transferred to the same folder
    
    Parameters
    ----------
    min_ts : Optional[int], None by default
        If passed, skip entries with a timestamp smaller than this value
    
    Returns
    -------
    List[ExchangeLoggerEntry]
        The parsed list of exchange logger entries
    """
    
    entries = []
    # db = TransactionDatabase()
    for next_path in _get_archive_logs():
        try:
            if min_ts and ts_from_log_file(next_path) < min_ts-172800:
                continue
        except ValueError as e:
            if str(e).__contains__("time data 'exchange' does not match format") and next_path.endswith("exchange.log"):
                ...
            else:
                raise e
        entries += _parse_exchange_log_file(next_path, min_ts=min_ts)
    return entries


# exchange_log_entries = merge_exchange_logger_exports()

    
# to_do = {}
# _dir = str(gp.dir_runelite_ge_export_raw).replace('/', os.sep)
# for f in os.listdir(_dir):
#     next_file = os.path.join(_dir, f)
#     key = (f.split('-')[0], os.path.getsize(next_file))
#
#     if to_do.get(key):
#         os.remove(next_file)
#         # print(os.path.getsize(next_file), next_file)
#     else:
#         to_do[key] = next_file
        
        
# merge_runelite_exports()
__all__ = "merge_exchange_logger_exports",
