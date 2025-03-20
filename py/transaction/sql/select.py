"""
Local database SELECT SQL queries. Queries were generated using LLM / function in _generate module.

Recommended usage;
import transaction.sql.select as sql_select
sql_select.account_by_name

"""
from transaction.constants import TableList

account_by_name = \
    f"""SELECT account_name, account_id FROM "{TableList.ACCOUNT}" WHERE account_name=?"""
"""Executable SQL for querying a row from the account table via an account_name"""

raw_runelite_export_transaction_by_item_timestamp = \
    f"""SELECT * FROM "{TableList.RAW_RUNELITE_EXPORT_TRANSACTION}" WHERE item_id=? AND timestamp=?"""
"""Executable SQL for querying raw_runelite_export_transaction by item_id and timestamp"""

raw_flipping_utilities_transaction_by_item_timestamp = \
    f"""SELECT * FROM "{TableList.RAW_FLIPPING_UTILITIES_TRANSACTION}" WHERE item_id=? AND timestamp=?"""
"""Executable SQL for querying raw_flipping_utilities_transaction by item_id and timestamp"""

raw_exchange_logger_transaction_by_item_timestamp = \
    f"""SELECT * FROM "{TableList.RAW_EXCHANGE_LOGGER_TRANSACTION}" WHERE item_id=? AND timestamp=?"""
"""Executable SQL for querying raw_exchange_logger_transaction by item_id and timestamp"""

raw_transaction_by_item_timestamp = \
    f"""SELECT * FROM "{TableList.RAW_TRANSACTION}" WHERE item_id=? AND timestamp=?"""
"""Executable SQL for querying raw_transaction by item_id and timestamp"""

transaction_by_item_status_timestamp = \
    f"""SELECT * FROM "{TableList.TRANSACTION}" WHERE item_id=? AND status=? AND timestamp=?"""
"""Executable SQL for querying transaction by item_id, status and timestamp"""

transaction_by_account_name = \
    f"""SELECT * FROM "{TableList.TRANSACTION}" WHERE account_name=?"""
"""Executable SQL for querying transaction by account_name"""

inventory_by_item_timestamp = \
    f"""SELECT * FROM "{TableList.INVENTORY}" WHERE item_id=? AND timestamp=?"""
"""Executable SQL for querying inventory by item_id and timestamp"""

inventory_by_transaction_id = \
    f"""SELECT * FROM "{TableList.INVENTORY}" WHERE transaction_id=?"""
"""Executable SQL for querying inventory by transaction_id"""

item_by_id = \
    f"""SELECT item_id, item_name FROM "{TableList.ITEM}" WHERE item_id=?"""
"""Executable SQL for querying a row from the item table by item_id"""

raw_runelite_export_transaction_by_transaction_id = \
    f"""SELECT * FROM "{TableList.RAW_RUNELITE_EXPORT_TRANSACTION}" WHERE transaction_id=?"""
"""Executable SQL for querying raw_runelite_export_transaction by transaction_id"""

raw_flipping_utilities_transaction_by_transaction_id = \
    f"""SELECT * FROM "{TableList.RAW_FLIPPING_UTILITIES_TRANSACTION}" WHERE transaction_id=?"""
"""Executable SQL for querying raw_flipping_utilities_transaction by transaction_id"""

raw_exchange_logger_transaction_by_transaction_id = \
    f"""SELECT * FROM "{TableList.RAW_EXCHANGE_LOGGER_TRANSACTION}" WHERE transaction_id=?"""
"""Executable SQL for querying raw_exchange_logger_transaction by transaction_id"""

raw_transaction_by_transaction_id = \
    f"""SELECT * FROM "{TableList.RAW_TRANSACTION}" WHERE transaction_id=?"""
"""Executable SQL for querying raw_transaction by transaction_id"""

transaction_by_transaction_id = \
    f"""SELECT * FROM "{TableList.TRANSACTION}" WHERE transaction_id=?"""
"""Executable SQL for querying transaction by transaction_id"""

count_raw_transaction_by_transaction_id = \
    f"""SELECT COUNT(*) FROM "{TableList.RAW_TRANSACTION}" WHERE transaction_id=?"""
"""Executable SQL for checking if there is a raw_transaction with a specific transaction_id"""

count_raw_transaction_by_transaction_id_tag = \
    f"""SELECT COUNT(*) FROM "{TableList.RAW_TRANSACTION}" WHERE transaction_id=? AND tag=?"""
"""Executable SQL for checking if there is a raw_transaction with a specific transaction_id and tag"""

check_if_exists = \
    f"""
    SELECT transaction_id, COUNT(*)
    FROM "{TableList.RAW_TRANSACTION}"
    WHERE item_id=:item_id AND quantity=:quantity AND timestamp=:timestamp AND price=:price AND is_buy=:is_buy
    """
"""Query that can be used to check if a particular transaction exists and if so, returns its transaction_id"""

insert_old_trasction = f"""SELECT * FROM "{TableList.TRANSACTION}" ORDER BY transaction_id"""

table_names = """SELECT name FROM sqlite_master WHERE type='table' and name != "sqlite_sequence";"""
"""Query that can be executed to fetch all table names"""

all_transactions = f"""SELECT * FROM "{TableList.TRANSACTION}" """
"""Fetches all rows from the transaction table"""

unique_items = f"""SELECT DISTINCT item_id FROM "{TableList.TRANSACTION}" ORDER BY item_id"""
"""Fetches a list of all item_ids that have been traded"""

sql_select_transaction_by_item = """SELECT * FROM "transaction"
    WHERE item_id=? AND status != 0 ORDER BY timestamp"""
"""SQL for loading transactions for the inventory"""
