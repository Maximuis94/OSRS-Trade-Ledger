"""
Module with Local Database CREATE INDEX statements

"""
from collections import namedtuple

from typing import Dict

index_item_item = \
    """CREATE INDEX idx_item_item ON "item" (item_id) """

index_transaction_item_status_timestamp = \
    f"""CREATE INDEX idx_item_status_timestamp ON "transaction" (item_id, status, timestamp);"""


index_inventory_item_timestamp = \
    f"""CREATE INDEX idx_item_timestamp ON "inventory" (item_id, timestamp);"""


index_inventory_transactionid = \
    f"""CREATE INDEX idx_transactionid ON "inventory" (transaction_id);"""


index_transaction_item_timestamp = \
    f"""CREATE INDEX idx_item_status_timestamp ON "transaction" (item_id, status, timestamp);"""


index_raw_runelite_export_transaction_item_timestamp = \
    """CREATE INDEX idx_item_timestamp ON "raw_runelite_export_transaction" (item_id, timestamp);"""


index_raw_flipping_utilities_transaction_item_timestamp = \
    """CREATE INDEX idx_item_timestamp ON "raw_flipping_utilities_transaction" (item_id, timestamp);"""


index_raw_exchange_logger_transaction_item_timestamp = \
    """CREATE INDEX idx_item_timestamp ON "raw_exchange_logger_transaction" (item_id, timestamp);"""


index_raw_transaction_item_status_timestamp = \
    """CREATE INDEX idx_item_status_timestamp ON "raw_transaction" (item_id, status, timestamp);"""

_dict = dict(locals())
_keys = ('item_item', 'transaction_item_status_timestamp', 'inventory_item_timestamp', 'inventory_transactionid',
         'transaction_item_timestamp', 'raw_runelite_export_transaction_item_timestamp', 'raw_flipping_utilities_transaction_item_timestamp', 'raw_exchange_logger_transaction_item_timestamp', 'raw_transaction_item_status_timestamp')

sql_create_index = namedtuple(
    "CreateIndexSQL",
    _keys)(*[_dict[f"index_{k}"] for k in _keys])
"""NamedTuple with all the CREATE INDEX statements found in this module"""

del _dict, _keys
__all__ = "sql_create_index"
