import json
import os.path
import sqlite3
import time
from collections.abc import Iterable, Sized
from dataclasses import dataclass
from typing import Optional, Dict, List, Any, Callable

import numpy as np
import pandas as pd

from venv_auto_loader.active_venv import *
import global_variables.configurations as cfg
import global_variables.osrs as go
import global_variables.path as gp
import util.file as uf
import util.unix_time as ut
from util.logger import prt
from common.item import remap_item, create_item
from file.file import File
from item.db_entity import Item
from model.transaction import Transaction

__t0__ = time.perf_counter()
_db_path = gp.f_db_local

# Constants
LOG_FILE_LENGTH = [len("exchange_yyyy-mm-dd.log")]
IMMUTABLE_ATTRS = ('slot', 'item', 'max', 'offer', 'is_buy')
RENAMED_IMMUTABLE_ATTRS = ('slot_id', 'item_id', 'max_quantity', 'price', 'is_buy')
QUEUE_DATA_FIELDS = ['item_id', 'timestamp', 'is_buy', 'quantity', 'price']

@dataclass
class ParsedLogLine:
    timestamp: int
    is_buy: int 
    item_id: int
    quantity: int
    price: int
    max_quantity: int
    value: int
    state_id: int
    slot_id: int

def get_next_transaction_id(db_con: Optional[sqlite3.Connection] = None) -> int:
    """Returns the next transaction_id to insert"""
    if db_con is None:
        db_con = sqlite3.connect(database=f"file:{_db_path}?mode=ro", uri=True)
    return db_con.execute("SELECT MAX(transaction_id) FROM 'transaction'").fetchone()[0] + 1

def parse_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse input line from exchange_log file"""
    if not line.startswith('{'):
        if len(line) > 2:
            raise ValueError(f"Line {line} could not be identified as json/text/tabulated")
        return None
        
    data = json.loads(line)
    
    # Parse timestamp
    ts_parts = [int(t) for t in (data['date'].replace('-', ' ') + ' ' + data['time'].replace(':', ' ')).split(' ')]
    timestamp = int(time.mktime((ts_parts[0], ts_parts[1], ts_parts[2], ts_parts[3], ts_parts[4], ts_parts[5], 0, 0, 0)))
    
    # Parse other fields
    state_id = go.exchange_log_states.index(data['state'])
    is_buy = int(state_id < 3)
    quantity = int(data['qty'])
    value = int(data['worth'])
    
    # Calculate price
    try:
        price = round(value / quantity)
    except ZeroDivisionError:
        price = int(data['offer'])
        
    return {
        'timestamp': timestamp,
        'is_buy': is_buy,
        'item_id': int(data['item']),
        'quantity': quantity,
        'price': price,
        'max_quantity': int(data['max']),
        'value': value,
        'state_id': state_id,
        'slot_id': int(data['slot'])
    }

def update_submitted_lines(log_file: File = gp.f_submitted_lines_log, 
                         ts_threshold: int = int(time.time()) - 86400 * 3):
    """Remove expired lines from submitted lines log"""
    with open(log_file.path, 'r') as f:
        submitted_lines = f.readlines()
        
    temp_file = log_file.path.replace('.log', '_.log')
    filtered_lines = []
    removed_lines = []
    
    prt('Removing expired submitted lines...')
    
    for line in submitted_lines:
        entry = ExchangeLogLine(parse_line(line))
        if entry is None:
            continue
            
        if entry.timestamp > ts_threshold:
            filtered_lines.append(line)
        else:
            removed_lines.append(line)
            
    prt(f'Removed {len(removed_lines)} expired lines')
    
    with open(temp_file, 'w') as f:
        f.writelines(filtered_lines)
        
    log_file.delete()
    os.rename(temp_file, log_file)

# Type aliases
CastFunction = Callable[[Any], Any]

cast_functions: Dict[str, CastFunction] = {
    'int': int,
    'float': float, 
    'string': str,
    'npy_int': lambda v: np.array(v, dtype=int),
    'list': list
}

class ExchangeLogLine(Transaction):
    """Represents a parsed line from exchange log with additional metadata"""
    
    def __init__(self, parsed_line: Dict[str, Any]):
        super().__init__(
            transaction_id=get_next_transaction_id(),
            item_id=parsed_line['item_id'],
            timestamp=parsed_line['timestamp'],
            is_buy=parsed_line['is_buy'],
            quantity=parsed_line['quantity'],
            price=parsed_line['price'],
            status=int(not parsed_line['state_id'] % 3 == 0),
            tag='e',
            update_ts=int(time.time())
        )
        self.state_id = parsed_line['state_id']
        self.slot_id = parsed_line['slot_id']
        self.value = parsed_line['value']
        self.max_quantity = parsed_line['max_quantity']

    def to_list(self, 
                attribute_order: Iterable = go.exchange_log_archive_attribute_order,
                var_dtype: str = 'int', 
                list_dtype: str = 'list') -> List[Any]:
        """Convert attributes to specified types and return as list"""
        cast_var = cast_functions[var_dtype]
        cast_list = cast_functions[list_dtype]
        
        values = []
        for attr in attribute_order:
            val = self.__dict__.get(attr)
            values.append(cast_var(val) if val is not None else -1)
            
        return cast_list(values)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary of attributes"""
        return {k: getattr(self, k) for k in self.columns}

    def get_key(self, attribute_order: Iterable = RENAMED_IMMUTABLE_ATTRS) -> tuple:
        """Get tuple of immutable attributes as identifier"""
        return tuple(int(self.__dict__[a]) for a in attribute_order)

def parse_logs(post_exe_print: bool = True, t0: Optional[float] = None) -> bool:
    """Main entry point for parsing logs"""
    global __t0__
    if t0 is not None:
        __t0__ = t0
        
    if not process_logs(add_current=True):
        print("")
        return False
        
    submit_transaction_queue(submit_data=True)
    update_submitted_lines()
    
    if post_exe_print:
        print(f"Execution complete! Time taken: {1000 * (time.time() - __t0__):.0f}ms")
        print("Press ENTER to close this screen")
        time.sleep(60)
        
    return True

if __name__ == '__main__':
    parse_logs_background()
    input(f"Execution complete! Time taken: {1000 * (time.time() - __t0__):.0f}ms\nPress ENTER to close this screen")
