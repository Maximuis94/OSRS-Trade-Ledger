"""
Module with logic for parsing and merging runelite json exports.
merge_runelite_exports() will have all JSON files parsed, merged and checked for duplicates.
"""
from collections.abc import Iterable

import json
import os
import sqlite3
from typing import Dict, Tuple, Optional

import global_variables.path as gp
from file.file import File
from transaction.raw.raw_runelite_export_entry import RuneliteExportEntry


def _merge_json_files(*json_files, account_name: str, merge_to: str | File = gp.dir_runelite_ge_export,
                      transfer_to: str | File = gp.dir_runelite_ge_export_raw):
    """
    Iterate over `json_files`. Load each file, and merge the contents of all files. Additionally, transfer the scattered
    raw files to one central location.
    
    Parameters
    ----------
    json_files : str
        The json_files that are to be merged.
    account_name : str
        The name of the account that made this trade
    merge_to : str | File, optional, global_variables.path.dir_runelite_export by default
        Folder to which the merged json files are to be exported
    transfer_to : str | File, optional, global_variables.path.dir_runelite_export_raw by default
        Folder to which the raw json files are to be transferred

    Returns
    -------
    Dict[str, int]
        A dict with the json file for each entry, and the amount of newly added json entries for that file.
    """
    
    skips = []
    if not isinstance(merge_to, str):
        merge_to = str(merge_to)
    if os.path.isdir(merge_to):
        skips.append(merge_to)
        merge_to = os.path.join(merge_to, f"{account_name}.json")
    
    entry_counts = {}
    entries = {}
    temp_files = []
    for file_id, json_file in enumerate(json_files):
        f = os.path.split(json_file)[1]
        n_added = len(entries)
        current_entries = json.load(open(json_file, 'r'))
        for idx, e in enumerate(current_entries):
            e.pop("account_name", None)
            e.pop("value", None)
            json_entry = (RuneliteExportEntry.raw_entry if e.get("time") else RuneliteExportEntry)(account_name=account_name, **e)
            # print(idx, json_entry.key, json_entry)
            if entries.get(json_entry.key) is None:
                entries[json_entry.key] = json_entry
        n_added = len(entries) - n_added
        entry_counts[json_file] = n_added
        print(file_id, json_file, f"{n_added}/{len(current_entries)}", "\n\n")
        
        if not os.path.exists(merge_to) or not os.path.samefile(json_file, merge_to):
            temp_file = os.path.join(gp.dir_export_parser_temp, f"{account_name}-{file_id:0>2}.json")
            os.rename(json_file, temp_file)
            temp_files.append(temp_file)
    
    if len(entries) > 0:
        json.dump([e.dict for e in entries.values()], open(merge_to, 'w'), indent=2)
        print(merge_to, len(entries))
    
    for temp_file in temp_files:
        os.rename(temp_file, temp_file.replace(gp.dir_export_parser_temp, transfer_to))
        
    json.dump(entry_counts, open(os.path.join(gp.dir_output, "entry_counts.json"), 'w'), indent=2)
    
    
    


def _get_paths(account_name: str, folders: Optional[Iterable[str]] = ()) -> Tuple[str, ...]:
    """Return all paths with runelite ge export data"""
    paths = []
    for root in [
                    gp.dir_runelite_ge_export,
                    gp.dir_runelite_ge_export_raw,
                    os.path.join(gp.dir_data, "ge_exports"),
                    gp.dir_downloads
                ] + list(folders):
        root = root.replace('/', os.sep)
        if not os.path.exists(root):
            continue
        paths += [os.path.join(root, f) for f in os.listdir(root)
                  if f.lower().startswith(account_name.lower()) and f.endswith(".json")]
    return tuple(paths)


def _get_account_names(db_path: File = gp.f_db_transaction_new) -> Tuple[str, ...]:
    """Return a list of account names found in the database at `db_path`"""
    con = sqlite3.connect(db_path)
    return tuple([e[0] for e in con.execute("SELECT account_name FROM account").fetchall()])


def merge_runelite_exports(*accounts, input_folders: Optional[Iterable[str]] = (), merged_folder: str | File = gp.dir_runelite_ge_export, min_ts: Optional[int] = None) -> Tuple[RuneliteExportEntry, ...]:
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
    accounts : str
        Accounts that are to be processed. If None are passed, extract the list from the transaction_database instead.
    input_folders : Optional[Iterable[str]], Optional, () by default
        Additional input folders to look in for runelite ge exports
    merged_folder : Optional[str | File], optional, None by default
        If passed, output the merged JSON files in this folder.
    """
    if len(accounts) == 0:
        accounts = _get_account_names()
    
    for account in accounts:
        _merge_json_files(*_get_paths(account, input_folders), account_name=account, merge_to=merged_folder)
    
    merged = []
    for json_file in os.listdir(merged_folder):
        if not json_file.endswith(".json"):
            continue
        try:
            if min_ts is None:
                merged += [RuneliteExportEntry(**e) for e in json.load(open(os.path.join(merged_folder, json_file), 'r'))]
            else:
                merged += [RuneliteExportEntry(**e) for e in json.load(open(os.path.join(merged_folder, json_file), 'r')) if e['timestamp'] > min_ts]
        except TypeError as err:
            if "unexpected keyword" in str(err):
                if min_ts is None:
                    merged += [RuneliteExportEntry.raw_entry(**e) for e in
                               json.load(open(os.path.join(merged_folder, json_file), 'r'))]
                else:
                    merged += [RuneliteExportEntry.raw_entry(**e) for e in
                               json.load(open(os.path.join(merged_folder, json_file), 'r')) if e['timestamp'] > min_ts]
            else:
                raise err
    return tuple(merged)
    
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
__all__ = "merge_runelite_exports",
