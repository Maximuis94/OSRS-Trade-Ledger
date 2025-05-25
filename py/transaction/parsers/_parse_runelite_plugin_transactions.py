from math import floor

import os

import json

import sqlite3

from typing import Dict, List, Optional, Set, Tuple

import global_variables.path as gp
from transaction.raw.raw_runelite_plugin_trade_entry import RunelitePluginTransaction
from transaction.raw.raw_runelite_profile_trade_entry import RuneliteProfileTransaction


def transaction_is_completed(transaction: Dict[str, any]) -> bool:
    """Check if `transaction` is completed"""


def parse_exchange_log_file(path: str = gp.f_runelite_exchange_log, t0: Optional[int] = None) -> List[RunelitePluginTransaction]:
    """
    Parses the exchange log file located at `path`
    
    Parameters
    ----------
    path : str
        Path to the exchange log file
    t0 : Optional[int], None by default
        If passed, do not parse transactions with a timestamp lower than `t0`

    Returns
    -------
    List[RunelitePluginTransaction]
        A list with RunelitePluginTransaction instances based on the parsed ExchangeLog file

    """
    for line in open(path, 'r').readlines():
        print(line)
        print(RunelitePluginTransaction(**json.loads(line)))
        
parse_exchange_log_file()
exit(123)

_profile_account: Dict[str, str]
_account_names: Set[str]


def load_profile_id_account_name_mapping():
    con = sqlite3.connect(f"file:{gp.f_db_local}?mode=ro", uri=True)
    global _profile_account, _account_names
    _profile_account = {pid: name for pid, name in con.execute(
        "SELECT runelite_profile_id, account_name FROM account WHERE runelite_profile_id NOT NULL").fetchall()}
    con.close()
    
    _account_names = set(_profile_account.values())


def get_account_name(profile_id: str) -> str:
    """Extract the account name that is mapped to `profile_id`"""
    return _profile_account[profile_id]


def is_trade_history_line(line: str) -> bool:
    """True if this line meets all assumptions of a line with trade history in the runelite profile data"""
    return len(line) > 46 and line.startswith("grand") and line[33:45] == "tradeHistory"


def parse_exchange_log_lines() -> List[RuneliteProfileTransaction]:
    """Parse the trade history as defined in the runelite profile data file. Iterate over all files specified, given
    input parameters
    
    Parameters
    ----------
    files : str, Optional
        0-N files to iterate over
    parse_src : bool, Optional, True by default
        If True, parse the official runelite profile data file
    parse_backups : bool, Optional, False by default
        If True, also iterate over all of the raw runelite profile data backup files

    Returns
    -------
    List[RuneliteProfileTransaction]
        Parsed transactions as a list of RuneliteProfileTransaction instances
    """
    files = list(files)
    
    if parse_backups:
        files += [os.path.join(gp.dir_runelite_profile_raw, f) for f in os.listdir(gp.dir_runelite_profile_raw)]
    
    if parse_src:
        files.append(gp.f_runelite_profile_properties)
    
    if len(files) == 0:
        raise ValueError("Input parameters are specified such that there are no files to parse...")
    
    trades = []
    for next_file in files:
        for line in open(next_file).readlines():
            if not is_trade_history_line(line):
                continue
            
            line = line.replace("\\", "")
            account_id = line[24:32]
            try:
                account_name = _profile_account[account_id]
            except NameError:
                load_profile_id_account_name_mapping()
                account_name = _profile_account[account_id]
            
            line = json.loads(line.split("=")[1])
            
            for el in line:
                values = int(el['i']), int(floor(el['t'] / 1000)), int(el['b']), int(el['p']), int(el['q'])
                trades.append(RuneliteProfileTransaction(*values, account_name))
    return trades




# runelite_profile_trades_merged = []
# for properties_file in os.listdir(gp.dir_runelite_profile_raw):
#     for line in open(os.path.join(gp.dir_runelite_profile_raw, properties_file)).readlines():
#         if not is_trade_history_line(line):
#             continue
#         parse_trade_history_line(line)
#
# import pandas as pd
# print(matches, len(trades))
# pd.DataFrame(trades).to_csv(os.path.join(gp.dir_output, "runelite_profile_trades.csv"), index=False)


def insert_runelite_profile_transactions(*transaction: RuneliteProfileTransaction, **kwargs):
    """
    Insert one or more transaction(s) into the runelite profile transaction table. If the data is already inserted,
    simply ignore execution and move on. Because of this mechanism, the Runelite profile data file is processed first,
    as this data tends to be more reliable, since it's fully automated.
    Commit and close the database upon completion.
    
    Parameters
    ----------
    transaction : RuneliteProfileTransaction
        0-N RuneliteProfileTransaction instances to submit to the database
    
    Other Parameters
    ----------------
    db : str
        If passed, connect to the database at this path instead.

    """
    if len(transaction) == 0:
        return
    
    con = sqlite3.connect(kwargs.get("db", gp.f_db_transaction_new))
    try:
        con.executemany(transaction[0].sql_insert, [t.sql_params for t in transaction])
        con.commit()
    except sqlite3.OperationalError as e:
        con.rollback()
    con.close()


def identify_account(file_name: str) -> Optional[str]:
    """Return the account derived from `file_name`, if any"""
    for account in _profile_account.values():
        if file_name.lower().__contains__(account.lower()):
            return account


def get_ge_export_values(e: Dict[str, str | int]) -> List[int]:
    if e.get("itemId"):
        return [int(e['itemId']), int(floor(e['time'] / 1000)), int(e['buy']), int(e['price']), int(e['quantity'])]
    else:
        return [int(e['item_id']), int(e['timestamp']), int(e['is_buy']), int(e['price']), int(e['quantity'])]


def parse_runelite_json_exports(*paths) -> List[RuneliteProfileTransaction]:
    """Extract transactions from the grand-exchange.json files specified in `paths`
    
    Parameters
    ----------
    paths : str
        One or more paths with grand-exchange.json files. If a directory is encountered, it is searched for files with
        the -grand-exchange.json suffix. If you wish to parse the account that made the trade as well, include the
        account name in the file name, e.g. USERNAME-grand-exchange.json.
        
    Returns
    -------
    List[RuneliteProfileTransaction]
        Contents of all the path(s) specified
    
    Notes
    -----
    The files parsed by this function are expected to have a certain format; e.g. {"buy":true,"itemId":9144,
    "quantity":22000,"price":46,"time":1719362684422}

    """
    
    trades = []
    if len(paths) == 0:
        paths = [os.path.join(gp.dir_runelite_ge_export, f)
                 for f in os.listdir(gp.dir_runelite_ge_export) if f.endswith(".json")]
        
    for path in paths:
        account_name = identify_account(path)
        
        for el in json.load(open(path)):
            values = get_ge_export_values(el)
            if account_name is None:
                trades.append(RuneliteProfileTransaction(*values))
            else:
                trades.append(RuneliteProfileTransaction(*values, account_name))
    return trades


def runelite_profile_transaction_parser(*ge_export_paths, **kwargs):
    """Merged transaction parser that parses both ge exports and profile data files"""
    if len(ge_export_paths) == 0:
        ge_export_paths = [os.path.join(gp.dir_runelite_ge_export, f) for f in os.listdir(gp.dir_runelite_ge_export) if
                           f.endswith('.json')]
    insert_runelite_profile_transactions(*(parse_trade_history() + parse_runelite_json_exports(*ge_export_paths)), **kwargs)



def extract_from_db():
    """Export the data per account from the database table into a json file"""
    con = sqlite3.connect(f"file:{gp.f_db_transaction_new}?mode=ro", uri=True)
    sql = """SELECT item_id, timestamp, is_buy, quantity, price FROM "raw_runelite_profile_data_transaction" WHERE account_name=? ORDER BY timestamp, item_id, is_buy DESC"""
    cur_list = con.cursor()
    cur_list.row_factory = lambda _, values: f"\t[{', '.join([str(v) for v in values])}]"
    
    cur_idx0 = con.cursor()
    cur_idx0.row_factory = lambda _, row: row[0]
    for account in cur_idx0.execute("""SELECT DISTINCT account_name FROM "raw_runelite_profile_data_transaction" ORDER BY account_name""").fetchall():
        (open(os.path.join(gp.dir_runelite_profile, account+".json"), "w")
         .write("[\n"+",\n".join([*cur_list.execute(sql, (account,)).fetchall()]) + "\n]"))


# Parse runelite profile transactions with this function call;
# runelite_profile_transaction_parser()