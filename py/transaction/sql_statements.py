"""Module with executable SQL statements used by (parts of) the TransactionDatabase"""




_sql_id_merged_transaction: Dict[str, str] = {
    'exchange_logger': f"""SELECT transaction_id, exchange_logger_id  FROM "raw_transaction" WHERE item_id=? AND timestamp=? AND is_buy=? AND quantity=? AND value>=price*quantity AND account_id=?""",
    'flipping_utilities': f"""SELECT transaction_id, flipping_utilities_id  FROM "raw_transaction" WHERE item_id=? AND timestamp=? AND is_buy=? AND quantity=? AND price=? AND account_id=?""",
    'runelite_export': f"""SELECT transaction_id, runelite_export_id  FROM "raw_transaction" WHERE item_id=? AND timestamp>=? AND is_buy=? AND quantity=? AND price=? AND account_id=?""",
}
""""""


_sql_params_merged_transaction: Dict[str, Tuple[str, ...]] = {
    'exchange_logger': ('item_id', 'timestamp', 'is_buy', 'quantity', 'value'),
    'flipping_utilities': ('item_id', 'timestamp', 'is_buy', 'quantity', 'price'),
    'runelite_export': ('item_id', 'timestamp', 'is_buy', 'quantity', 'value', 'account_id')
}


sql_update_inventory = NotImplementedError
"""SQL for loading transactions for the inventory"""

# import global_variables.path as gp
# conn = sqlite3.connect(gp.f_db_transaction_new)
# c = conn.cursor()
# c.row_factory = factory_dict
#
# print(c.execute(sql_select_transaction).fetchall())
