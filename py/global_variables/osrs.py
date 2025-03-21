"""
Module with pre-defined osrs-related variables like item_id lists, item_id-item_name mappings, etc.



"""
import sqlite3
from typing import Dict

from venv_auto_loader.active_venv import *
import global_variables.path as gp

# import util.osrs as uo
__t0__ = time.perf_counter()

from backend.download import realtime_prices
from item.itemdb import itemdb

# User agent passed when making certain http requests
_item_table = 'item'
con = sqlite3.connect(f"file:{gp.f_db_local}?mode=ro", uri=True)
con.row_factory = lambda c, row: row[:2]
# rows = con.execute("SELECT item_name, item_id FROM item ORDER BY item_name").fetchall()
rows = con.execute(f"SELECT item_name, item_id FROM {_item_table} ORDER BY item_name").fetchall()


# item_ids: sorted list of integers [2, 6, 8, 10, 12, ...]
item_ids = [r[1] for r in rows]
item_ids.sort()

# item_names: sorted list of strings ['3rd age amulet', '3rd age axe', ...]
item_names = [r[0] for r in rows]

# id_name: list of size max(item_ids) with None at non-existent item_ids [None, None, 'Cannonball', None, None]
# Works like a dict if the index is smaller than the largest item_id, although it can raise an IndexError
id_name = {i: n for n, i in rows}
id_name = [id_name.get(n) for n in range(item_ids[-1]+1)]

# name_id: {item_name: item_id} dict {'3rd age amulet': 10344, '3rd age axe': 20011, '3rd age bow': 12424, ...}
name_id = {n: i for n, i in rows}

# itemdb: {item_id: item} dict
# 2: {'id': 2, 'item_id': 2, 'item_name': 'Cannonball', 'members': True, 'alch_value': 3, 'buy_limit': 11000,
# 'stackable': True, 'release_date': 1053993600, 'equipable': False, 'weight': 0.0, 'update_ts': 1704884052,
# 'augment_data': True, 'remap_to': 0, 'remap_price': 0.0, 'remap_quantity': 0.0, 'target_buy': 0, 'target_sell': 0,
# 'item_group': ''}
# con.row_factory = lambda c, row: Item(**{col[0]: row[i] for i, col in enumerate(c.description)})

# List of item_ids that have augment_data%2 == 1 (=True) and should be included in npy array updates
# augment_data with a value larger than 1 is marked as immutable and it will not be subject to automated re-evaluations
con.row_factory = lambda c, row: row[0]
npy_items = tuple(con.execute(f"SELECT item_id FROM {_item_table} WHERE augment_data%2=1").fetchall())

#
skip_ids: Tuple[int, ...] = (2203, 2264, 4595, 7228, 7466, 8624, 8626, 8628, 22610, 22613, 22622, 22634, 22636, 25991,
                             25994, 25997, 26000, 26003, 26006, 26009, 26012, 26015, 26018, 26021, 26024, 26027, 26030,
                             26033, 26036, 26039, 26042, 26045, 26048, 26051, 26054, 26057, 26060, 26063, 26066, 26069,
                             26072, 26075, 26078, 26081, 26084, 26087, 26090, 26093, 26096, 26099, 26102, 26105, 26108,
                             26111, 26114, 26117, 26120, 26123, 26126, 26129, 26132, 26135, 26138, 26141, 26144, 26147,
                             26602)
"""List of item_ids that are to be ignored as they are not likely to be of any use and likely to cause errors"""

timeseries_skip_ids = (9044, 9050, 26247, 2660)

con.close()
del con, rows

most_traded_items = (2, 314, 453, 554, 555, 556, 557, 560, 561, 561, 562, 565, 7936, 12934, 21820, 27616)
"""A tuple with the most traded items."""

reference_item_id = 21820
"""Revenant ether item_id"""

# nature_rune_price = con.execute("SELECT price, MAX(timestamp) FROM realtime WHERE timestamp > :ts AND item_id=561",
#                                 {'ts': max_wiki_ts}).fetchone()[0]
rt_prices: Dict[int, Tuple[int, int]] = realtime_prices(True, False)
"""A dictionary that uses int item_id as key and stores (buy, sell) prices as a tuple as entry"""

nature_rune_price: Tuple[int, int] = rt_prices.get(561)
"""The realtime buy and sell price of a Nature rune"""


def get_high_alch_profit(item_id) -> int:
    """Get the profit made for high-alching `ítem_id` """
    return itemdb.high_alch_profit(item_id)


exchange_log_archive_attribute_order = ('timestamp', 'is_buy', 'item_id', 'item_id', 'quantity', 'price', 'value',
                                        'max_quantity', 'state_id', 'slot_id')
"""Parsed exchange log lines are converted to numerical lists following this specific attribute order"""

exchange_log_states = ('BUYING', 'CANCELLED_BUY', 'BOUGHT', 'SELLING', 'CANCELLED_SELL', 'SOLD', 'EMPTY')
"""Various transaction states, as used within the exchange_log of Runelite"""

exchange_max_csv_transactions = 5000

# Automatically submitted buy and sell transactions
transaction_tag_purchase: str = 'b'
"""A Purchase that was automatically submitted"""
transaction_tag_sale: str = 's'
"""A Sale that was automatically submitted"""

# Manually submitted buy and sell transactions
transaction_tag_purchase_m: str = 'B'
"""A purchase that was manually submitted via the GUI"""

transaction_tag_sale_m: str = 'S'
"""A Sale that was manually submitted via the GUI"""

# Automatically submitted stock correction (to account for a quantity deficit)
transaction_tag_correction: str = 'd'
"""Transaction that represents an automatically inserted stock correction"""

# Manually submitted item consumption and production
transaction_tag_consumed: str = 'C'
"""Transaction that represents consumption of an item"""

transaction_tag_produced: str = 'P'
"""Transaction that represents production of an item"""

# Manually submitted item stock count
transaction_tag_counted: str = 'X'
"""Transaction produced by counting stock. The data after such a count is anchored to values of this transaction."""

# Bond purchase
transaction_tag_bond: str = 'o'
"""Transaction of a Bond purchase"""

