"""
Local database INSERT SQL statements. Statements were generated with the generate_insert_statement() function in
_generate.py
"""
insert_raw_runelite_export_transaction = \
    """
    INSERT INTO "raw_runelite_export_transaction" ("item_id", "timestamp", "is_buy", "quantity", "price", "account_name", "update_timestamp") VALUES (?, ?, ?, ?, ?, ?, ?);
    """

insert_raw_runelite_export_transaction_dict = \
    """
    INSERT INTO "raw_runelite_export_transaction" ("item_id", "timestamp", "is_buy", "quantity", "price", "account_name", "update_timestamp") VALUES (:item_id, :timestamp, :is_buy, :quantity, :price, :account_name, :update_timestamp);
    """

insert_raw_flipping_utilities_transaction = \
    """
    INSERT INTO "raw_flipping_utilities_transaction" ("uuid", "item_id", "timestamp_created", "timestamp", "is_buy", "quantity", "max_quantity", "price", "account_name", "ge_slot", "update_timestamp") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

insert_raw_flipping_utilities_transaction_dict = \
    """
    INSERT INTO "raw_flipping_utilities_transaction" ("uuid", "item_id", "timestamp_created", "timestamp", "is_buy", "quantity", "max_quantity", "price", "account_name", "ge_slot", "update_timestamp") VALUES (:uuid, :item_id, :timestamp_created, :timestamp, :is_buy, :quantity, :max_quantity, :price, :account_name, :ge_slot, :update_timestamp);
    """

insert_raw_exchange_logger_transaction = \
    """
    INSERT INTO "raw_exchange_logger_transaction" ("item_id", "timestamp", "is_buy", "quantity", "max_quantity", "price", "offered_price", "value", "ge_slot", "update_timestamp") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

insert_raw_exchange_logger_transaction_dict = \
    """
    INSERT INTO "raw_exchange_logger_transaction" ("item_id", "timestamp", "is_buy", "quantity", "max_quantity", "price", "offered_price", "value", "ge_slot", "update_timestamp") VALUES (:item_id, :timestamp, :is_buy, :quantity, :max_quantity, :price, :offered_price, :value, :ge_slot, :update_timestamp);
    """

insert_raw_transaction = \
    """
    INSERT INTO "raw_transaction" ("item_id", "timestamp_created", "timestamp", "timestamp_runelite_export", "is_buy", "quantity", "max_quantity", "price", "offered_price", "value", "account_name", "ge_slot", "status", "tag", "update_timestamp", "exchange_logger_id", "flipping_utilities_id", "runelite_export_id") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

insert_raw_transaction_dict = \
    """
    INSERT INTO "raw_transaction" ("item_id", "timestamp_created", "timestamp", "timestamp_runelite_export", "is_buy", "quantity", "max_quantity", "price", "offered_price", "value", "account_name", "ge_slot", "status", "tag", "update_timestamp", "exchange_logger_id", "flipping_utilities_id", "runelite_export_id") VALUES (:item_id, :timestamp_created, :timestamp, :timestamp_runelite_export, :is_buy, :quantity, :max_quantity, :price, :offered_price, :value, :account_name, :ge_slot, :status, :tag, :update_timestamp, :exchange_logger_id, :flipping_utilities_id, :runelite_export_id);
    """

insert_transaction = \
    """
    INSERT INTO "transaction" ("raw_transaction_id", "item_id", "timestamp_created", "timestamp", "is_buy", "quantity", "max_quantity", "price", "offered_price", "value", "ge_slot", "account_name", "status", "tag", "update_timestamp") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

insert_transaction_dict = \
    """
    INSERT INTO "transaction" ("raw_transaction_id", "item_id", "timestamp_created", "timestamp", "is_buy", "quantity", "max_quantity", "price", "offered_price", "value", "ge_slot", "account_name", "status", "tag", "update_timestamp") VALUES (:raw_transaction_id, :item_id, :timestamp_created, :timestamp, :is_buy, :quantity, :max_quantity, :price, :offered_price, :value, :ge_slot, :account_name, :status, :tag, :update_timestamp);
    """

insert_inventory = \
    """
    INSERT INTO "inventory" ("transaction_id", "item_id", "balance", "average_buy_price", "profit", "tax", "invested_value", "current_value", "n_purchases", "n_bought", "n_sales", "n_sold", "update_timestamp") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

insert_inventory_dict = \
    """
    INSERT INTO "inventory" ("transaction_id", "item_id", "balance", "average_buy_price", "profit", "tax", "invested_value", "current_value", "n_purchases", "n_bought", "n_sales", "n_sold", "update_timestamp") VALUES (:transaction_id, :item_id, :balance, :average_buy_price, :profit, :tax, :invested_value, :current_value, :n_purchases, :n_bought, :n_sales, :n_sold, :update_timestamp);
    """

insert_account = \
    """
    INSERT INTO "account" ("account_name", "account_id") VALUES (?, ?);
    """

insert_account_dict = \
    """
    INSERT INTO "account" ("account_name", "account_id") VALUES (:account_name, :account_id);
    """

insert_item = \
    """
    INSERT INTO "item" ("item_id", "item_name", "members", "alch_value", "buy_limit", "stackable", "release_date", "equipable", "weight", "update_ts", "augment_data", "remap_to", "remap_price", "remap_quantity", "target_buy", "target_sell", "item_group", "count_item") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

insert_item_dict = \
    """
    INSERT INTO "item" ("item_id", "item_name", "members", "alch_value", "buy_limit", "stackable", "release_date", "equipable", "weight", "update_ts", "augment_data", "remap_to", "remap_price", "remap_quantity", "target_buy", "target_sell", "item_group", "count_item") VALUES (:item_id, :item_name, :members, :alch_value, :buy_limit, :stackable, :release_date, :equipable, :weight, :update_ts, :augment_data, :remap_to, :remap_price, :remap_quantity, :target_buy, :target_sell, :item_group, :count_item);
    """

insert_raw_transaction_from_import = \
    """
    INSERT INTO raw_transaction ( item_id, timestamp_created, timestamp, timestamp_runelite_export, is_buy, quantity, max_quantity, price, offered_price, value, account_name, ge_slot, status, update_timestamp ) VALUES ( :item_id, :timestamp_created, :timestamp, :timestamp_runelite_export, :is_buy, :quantity, :max_quantity, :price, :offered_price, :value, :account_name, :ge_slot, :status, :update_timestamp );
    """
"""INSERT statement used when importing transactions into raw_transaction table"""

insert_raw_transaction_runelite_export = \
    """
    INSERT INTO "raw_transaction" (
        item_id, timestamp, timestamp_runelite_export, is_buy, quantity, update_timestamp, price,
        status, tag, runelite_export_id, account_name
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

insert_raw_transaction_runelite_export_dict = \
    """
    INSERT INTO "raw_transaction" (
        item_id, timestamp, timestamp_runelite_export, is_buy, quantity, update_timestamp, price,
        status, tag, runelite_export_id, account_name
    ) VALUES (
        :item_id, :timestamp, :timestamp_runelite_export, :is_buy, :quantity, :update_timestamp, :price,
        :status, :tag, :runelite_export_id, :account_name
    )
    """


