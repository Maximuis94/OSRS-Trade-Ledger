"""
This module contains various implementations that stem from using the bank memory plugin.

"""
import datetime
import os
from collections import namedtuple
from dataclasses import dataclass
from typing import List, Dict

import pandas as pd
import pyperclip

from venv_auto_loader.active_venv import *
import global_variables.path as gp
import global_variables.osrs as go
from file.file import File
from global_variables.data_classes import Transaction
from global_variables.itemdb import *
from inventory.database import Inventory
from inventory.transactions import balance

BANK_MEMORY_INITIAL_LINE: str = "Item id	Item name	Item quantity"


__t0__ = int(time.time())
_inv = Inventory()


def extract_timestamp(file_name: str):
    f = os.path.splitext(file_name)[0].split('_')[-1].split('-')
    y = 2000 + int(f[0]) if len(f[0]) == 2 else int(f[0])
    return int(datetime.datetime(y, *[int(t) for t in f[1:]]).timestamp())


@dataclass(slots=True, order=True)
class ItemCount:
    """Class for representing a single item count, as well as parsing lines."""
    item: Item
    count: int
    timestamp: int = __t0__
    
    def __post_init__(self):
        """Alter the representation of this ItemCount, if applicable"""
        if self.item.remap_to > 0:
            # print(f"Remapping {self.item} to {go.id_name[self.item.remap_to]}")
            self.item, self.count = itemdb[self.item.remap_to], int(self.count * self.item.remap_quantity)
    
    @staticmethod
    def from_line(line: str):
        """Parse `line` and return it as ItemCount"""
        try:
            item_id, item_name, item_count = line.split('\t')
            
        except ValueError:
            return None
        
        if item_id.strip().isdigit() and item_count.strip().isdigit():
            item_id, item_count = int(item_id), int(item_count)
            if itemdb[item_id] is not None:
                return ItemCount(itemdb[int(item_id)], int(item_count))
        return None
    
    @staticmethod
    def from_csv(item_id: str, item_name: str, quantity: str):
        """Convert a CSV line into an ItemCount instance"""
        if go.id_name[int(item_id)] == item_name:
            return ItemCount(itemdb[int(item_id)], int(quantity))
    
    def dict(self, ts: int = None):
        """Dict representation of this ItemCount object"""
        if ts is None:
            ts = int(time.time())
        ie = _inv.get_entry_at_timestamp(self.item.item_id, timestamp=ts)
        if ie is None:
            return {"item_id": self.item.item_id,
                    "name": self.item.item_name,
                    "count": self.count,
                    "balance": "",
                    "include": self.item.count_item}
        elif isinstance(ie, dict):
            try:
                balance = int(ie.get("balance"))
                if abs(balance) > 1000 or abs(ie.get("profit")) > 15000000:
                    ...
                else:
                    balance = ""
            except TypeError:
                balance = ""
                
            return {"item_id": self.item.item_id,
                    "name": self.item.item_name,
                    "count": self.count,
                    "balance": balance,
                    "include": self.item.count_item}
    
    def __str__(self):
        
        return f"""ItemCount(item={self.item.item_name}, count={self.count})"""


def is_bank_memory_export(string: str) -> bool:
    """Return True if `string` is a bank memory export to the clipboard."""
    return string.startswith(BANK_MEMORY_INITIAL_LINE)


def export_clipboard_text(verify_contents: bool = True, delimiter: str = '\r\n'):
    """Extract the text currently loaded on the clipboard and export it to a CSV file."""
    item_counts = pyperclip.paste()
    
    if verify_contents and not is_bank_memory_export(item_counts):
        raise RuntimeError("Given string is not a bank memory clipboard export")
    
    # if not isinstance(item_counts, str) and not is_bank_memory_export(item_counts):
    #     print(f"Skipped item counts")
    #     return False
    
    return item_counts.split(delimiter)[1:]
    

def process_item_count(clipboard_lines: List[str]) -> Dict[int, ItemCount]:
    """Parse the item count files that have been exported, but are due for processing."""
    counts: Dict[int, ItemCount] = {}
    
    for current_line in clipboard_lines:
        i = ItemCount.from_line(current_line)
        
        if i is None:
            continue
        
        _id = i.item.item_id
        if counts.get(_id) is None:
            counts[_id] = i
        else:
            i.count += counts.get(_id).count
            counts[_id] = i
    return counts


def export_out_file_name(account_name: str = None, dir_path: str | File = gp.dir_bank_memory):
    """Generate a file name to assign to the to-be-exported CSV file"""
    dtn = datetime.datetime.now()
    return os.path.join(dir_path, f"""item-counts{"" if account_name is None else f"-{account_name}"}_{str(dtn.year)[-2:]}-{dtn.month:0>2}-{dtn.day:0>2}-{dtn.hour:0>2}-{dtn.minute:0>2}.csv""")


def export_to_csv(out_file: File | str = None):
    """Export the item counts to a CSV file."""
    clipboard_str = export_clipboard_text(verify_contents=True)
    
    print(" Would you like to set an account name for this file? Press ENTER to skip, or type the name and press ENTER")
    account_name = input(' ')
    if len(account_name) == 0:
        account_name = None
    
    item_counts = process_item_count(clipboard_str)
    
    if out_file is None:
        out_file = export_out_file_name(account_name=account_name, dir_path=gp.dir_bank_memory)
    timestamp = extract_timestamp(out_file)
    
    order_by = ['include', 'item_id'], [False, True]
    (pd.DataFrame([i.dict(timestamp) for item_id, i in item_counts.items()])
     .sort_values(by=order_by[0], ascending=order_by[1])
     .to_csv(out_file, index=False))
    return out_file
    

def significant_difference(_balance: int | str, _count: int | str | int) -> bool:
    """Return True if balance value is missing or it differs enough from `_count` to update its balance"""
    # Skip if there is no value for _count
    if _count is None or isinstance(_count, str) and _count == "":
        return False
    
    if _balance == "" or _balance is None:
        return True
    
    if isinstance(_balance, str):
        _balance = int(_balance)
    
    # Return True if the difference between count and balance is greater than 1% of the largest of the two
    return abs(_count - _balance)/max(_count, abs(_balance)) > .01


def convert_to_transactions(input_file: File | str):
    """
    Convert an ItemCount export CSV file into a set of ItemCount transactions to submit. Transactions submitted will
    have the X tag. They can be identified as a single batch via their timestamp and X tag.
    Note that while it is useful to periodically update balances with actual counts, it may introduce additional risks
    if this is done too frequently. As such, various mechanisms are put into place to reduce the amount of stock count
    transactions. As a rule of thumb, if the difference between the current balance and item count is considered
    negligible, do not submit a stock count transaction.
    Furthermore, upon submitting transactions, the timestamp for which the transactions would apply is barred from being
    re-used for stock count submissions, as to prevent duplicate submissions
    
    Parameters
    ----------
    input_file : File or str
        The input csv file that is to be converted into transactions.

    """
    try:
        ts = extract_timestamp(input_file)
        
    except TypeError:
        ts = int(time.time())
    
    to_add = []
    CSVLine = namedtuple("CSVLine", ("item_id", "name", "count", "balance", "include"))
    for cur in pd.read_csv(input_file).to_dict("records"):
        cur = CSVLine(**cur)
        
        if significant_difference(cur.balance, cur.count):
            to_add.append(Transaction())
    print(ts)
    

# TODO: Parse exported CSV file, generate stock count transactions (X) from all lines w/ count_item=1 for which
#  the balance differs significantly.
if __name__ == "__main__":
    export_to_csv()
    # convert_to_transactions(r"C:\Users\Max Moons\Documents\GitHub\OSRS-Trade-Ledger\py\data\bank_memory\item-counts_24-12-22-20-04.csv")
    ...
    
    