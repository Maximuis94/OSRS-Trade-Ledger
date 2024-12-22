"""
This module contains various gui-related constants and configurations.

"""
import datetime

from venv_auto_loader.active_venv import *
import util.str_formats as fmt
__t0__ = time.perf_counter()

# Width and height of the UI
width = 10
height = 10


# Value formats to apply in the GUI
value_formats = {
    'id': lambda n: n,
    'item_id': lambda n: n,
    'item_name': lambda s: fmt.shorten_string(string=s, max_length=20),
    'members': lambda b: b,
    'alch_value': lambda n: fmt.number(n=n, max_length=8),
    'buy_limit': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'stackable': lambda b: b,
    'release_date': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'equipable': lambda b: b,
    'weight': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'update_ts': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'augment_data': lambda b: b,
    'remap_to': lambda n: n,
    'remap_price': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'remap_quantity': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'target_buy': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'target_sell': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'item_group': lambda s: s,
    'timestamp': lambda ts: fmt.dt_hms(dt=datetime.datetime.fromtimestamp(ts)),
    'price': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'volume': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'buy_price': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'buy_volume': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'sell_price': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'sell_volume': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'is_buy': lambda b: b,
    'transaction_id': lambda n: n,
    'quantity': lambda n: fmt.number(n=n, max_decimals=3, max_length=8),
    'status': lambda n: n,
    'tag': lambda s: s
}
    
    
    


