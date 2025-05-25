"""
Module with various SQLite CREATE TABLE statements of non-timeseries tables.
"""
from collections import namedtuple

table_account = \
    """
    CREATE TABLE "account" (
        "account_name"	TEXT UNIQUE COLLATE NOCASE,
        "account_id"	INTEGER NOT NULL,
        PRIMARY KEY("account_name")
    )
    """


table_inventory = \
    """
    CREATE TABLE "inventory" (
        "id"	INTEGER,
        "transaction_id"	INTEGER NOT NULL UNIQUE,
        "item_id"	INTEGER NOT NULL,
        "timestamp" INTEGER NOT NULL,
        "balance"	INTEGER NOT NULL DEFAULT 0,
        "average_buy_price"	INTEGER NOT NULL DEFAULT 0,
        "profit"	INTEGER NOT NULL DEFAULT 0,
        "tax"	INTEGER NOT NULL DEFAULT 0,
        "invested_value"	INTEGER NOT NULL DEFAULT 0,
        "current_value"	INTEGER NOT NULL DEFAULT 0,
        "n_purchases"	INTEGER NOT NULL DEFAULT 0,
        "n_bought"	INTEGER NOT NULL DEFAULT 0,
        "n_sales"	INTEGER NOT NULL DEFAULT 0,
        "n_sold"	INTEGER NOT NULL DEFAULT 0,
        "executed"  INTEGER NOT NULL DEFAULT 0,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("id" AUTOINCREMENT),
        CONSTRAINT "fk_transaction" FOREIGN KEY("transaction_id") REFERENCES "transaction"("transaction_id")
    )
    """


table_item = \
    """
    CREATE TABLE "item" (
        "id"	INTEGER NOT NULL,
        "item_id"	INTEGER NOT NULL UNIQUE,
        "item_name"	TEXT NOT NULL UNIQUE,
        "members"	INTEGER NOT NULL DEFAULT False,
        "alch_value"	INTEGER NOT NULL DEFAULT 0,
        "buy_limit"	INTEGER NOT NULL DEFAULT 0,
        "stackable"	INTEGER NOT NULL DEFAULT False,
        "release_date"	INTEGER NOT NULL,
        "equipable"	INTEGER NOT NULL DEFAULT False,
        "weight"	REAL NOT NULL DEFAULT 0.0,
        "update_ts"	INTEGER NOT NULL,
        "augment_data"	INTEGER NOT NULL DEFAULT 0,
        "remap_to"	INTEGER NOT NULL DEFAULT 0,
        "remap_price"	REAL NOT NULL DEFAULT 0.0,
        "remap_quantity"	REAL NOT NULL DEFAULT 0.0,
        "target_buy"	INTEGER NOT NULL DEFAULT 0,
        "target_sell"	INTEGER NOT NULL DEFAULT 0,
        "item_group"	TEXT NOT NULL DEFAULT "",
        "count_item"	INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY("id")
    )
    """


table_raw_exchange_logger_transaction = \
    """
        CREATE TABLE "raw_exchange_logger_transaction" (
        "transaction_id"	INTEGER,
        "item_id"	INTEGER NOT NULL,
        "timestamp"	INTEGER NOT NULL,
        "is_buy"	INTEGER NOT NULL,
        "quantity"	INTEGER NOT NULL,
        "max_quantity"	INTEGER NOT NULL,
        "price"	INTEGER NOT NULL,
        "offered_price"	INTEGER NOT NULL,
        "value"	INTEGER NOT NULL,
        "ge_slot"	INTEGER NOT NULL,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("transaction_id" AUTOINCREMENT),
        CHECK("is_buy" BETWEEN 0 AND 1),
        CHECK("ge_slot" BETWEEN 0 AND 7)
    )
    """


table_raw_flipping_utilities_transaction = \
    """
    CREATE TABLE "raw_flipping_utilities_transaction" (
        "transaction_id"	INTEGER,
        "uuid"	TEXT NOT NULL UNIQUE,
        "item_id"	INTEGER NOT NULL,
        "timestamp_created"	INTEGER,
        "timestamp"	INTEGER NOT NULL,
        "is_buy"	INTEGER NOT NULL,
        "quantity"	INTEGER NOT NULL,
        "max_quantity"	INTEGER NOT NULL,
        "price"	INTEGER NOT NULL,
        "account_name"	TEXT NOT NULL COLLATE NOCASE,
        "ge_slot"	INTEGER,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("transaction_id"),
        FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
        CHECK("is_buy" BETWEEN 0 AND 1),
        CHECK("ge_slot" IS NULL OR "ge_slot" BETWEEN 0 AND 7)
    )
    """


# table_raw_runelite_export_transaction = \
#     """
#     CREATE TABLE "raw_runelite_export_transaction" (
#         "transaction_id"	INTEGER,
#         "item_id"	INTEGER NOT NULL,
#         "timestamp"	INTEGER NOT NULL,
#         "is_buy"	INTEGER NOT NULL,
#         "quantity"	INTEGER NOT NULL,
#         "price"	INTEGER NOT NULL,
#         "account_name"	TEXT NOT NULL COLLATE NOCASE,
#         "update_timestamp"	INTEGER NOT NULL,
#         PRIMARY KEY("transaction_id"),
#         FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
#         CHECK("is_buy" BETWEEN 0 AND 1),
#         UNIQUE("item_id", "timestamp", "is_buy", "quantity", "price")
#     )
#     """


table_raw_runelite_plugin_transaction = \
"""
CREATE TABLE "raw_runelite_plugin_transaction" (
    "transaction_id"    INTEGER PRIMARY KEY AUTOINCREMENT,
    "uuid"              TEXT UNIQUE,
    "item_id"           INTEGER    NOT NULL,
    "timestamp_created" INTEGER,
    "timestamp"         INTEGER    NOT NULL,
    "is_buy"            INTEGER    NOT NULL,
    "quantity"          INTEGER    NOT NULL,
    "max_quantity"      INTEGER    NOT NULL,
    "price"             INTEGER    NOT NULL,
    "offered_price"     INTEGER,
    "value"             INTEGER,
    "account_name"      TEXT       COLLATE NOCASE,
    "ge_slot"           INTEGER,
    "update_timestamp"  INTEGER    NOT NULL,

    FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
    CHECK("is_buy"  BETWEEN 0 AND 1),
    CHECK("ge_slot" IS NULL OR "ge_slot" BETWEEN 0 AND 7),
    UNIQUE("item_id","timestamp","is_buy","quantity","price","ge_slot")
);
"""


table_raw_runelite_profile_data_transaction = \
    """
    CREATE TABLE "raw_runelite_profile_transaction" (
        "transaction_id"	INTEGER,
        "item_id"	INTEGER NOT NULL,
        "timestamp"	INTEGER NOT NULL,
        "is_buy"	INTEGER NOT NULL,
        "quantity"	INTEGER NOT NULL,
        "price"	INTEGER NOT NULL,
        "account_name"	TEXT NOT NULL COLLATE NOCASE,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("transaction_id"),
        FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
        CHECK("is_buy" BETWEEN 0 AND 1),
        UNIQUE("item_id", "timestamp", "is_buy", "quantity", "price")
    )
    """

table_raw_runelite_export_transaction = """
CREATE TABLE "raw_runelite_export_transactions" (
    "transaction_id"    INTEGER,
    "item_id"           INTEGER NOT NULL,
    "timestamp"         INTEGER NOT NULL,
    "is_buy"            INTEGER NOT NULL,
    "quantity"          INTEGER NOT NULL,
    "price"             INTEGER NOT NULL,
    "account_name"      TEXT    NOT NULL COLLATE NOCASE,
    "update_timestamp"  INTEGER NOT NULL,
    PRIMARY KEY("transaction_id"),
    UNIQUE("item_id","timestamp","is_buy","quantity","price"),
    FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
    CHECK("is_buy" BETWEEN 0 AND 1)
);



"""


table_raw_transaction = \
    """
    CREATE TABLE "raw_transaction" (
        "transaction_id"	INTEGER,
        "item_id"	INTEGER NOT NULL,
        "timestamp_created"	INTEGER,
        "timestamp"	INTEGER NOT NULL,
        "timestamp_runelite_export"	INTEGER,
        "is_buy"	INTEGER NOT NULL,
        "quantity"	INTEGER NOT NULL,
        "max_quantity"	INTEGER,
        "price"	INTEGER NOT NULL,
        "offered_price"	INTEGER,
        "value"	INTEGER,
        "account_name"	TEXT COLLATE NOCASE,
        "ge_slot"	INTEGER,
        "status"	INTEGER DEFAULT 1,
        "tag"	TEXT,
        "update_timestamp"	INTEGER NOT NULL,
        "exchange_logger_id"	INTEGER UNIQUE,
        "flipping_utilities_id"	INTEGER UNIQUE,
        "runelite_export_id"	INTEGER UNIQUE,
        PRIMARY KEY("transaction_id" AUTOINCREMENT),
        FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
        FOREIGN KEY("exchange_logger_id") REFERENCES "raw_exchange_logger_transaction"("transaction_id"),
        FOREIGN KEY("flipping_utilities_id") REFERENCES "raw_flipping_utilities_transaction"("transaction_id"),
        FOREIGN KEY("runelite_export_id") REFERENCES "raw_runelite_export_transaction"("transaction_id"),
        CHECK("is_buy" BETWEEN 0 AND 1),
        CHECK("ge_slot" IS NULL OR "ge_slot" BETWEEN 0 AND 7)
    )
    """


table_transaction = \
    """CREATE TABLE "transaction" (
        "transaction_id"	INTEGER,
        "raw_transaction_id"	INTEGER NOT NULL UNIQUE,
        "item_id"	INTEGER NOT NULL,
        "timestamp_created"	INTEGER,
        "timestamp"	INTEGER NOT NULL,
        "is_buy"	INTEGER NOT NULL,
        "quantity"	INTEGER NOT NULL,
        "max_quantity"	INTEGER,
        "price"	INTEGER NOT NULL,
        "offered_price"	INTEGER,
        "value"	INTEGER,
        "tax" INTEGER GENERATED ALWAYS AS (
            CASE
                WHEN is_buy = 1 OR timestamp < 1639047600 THEN 0
                ELSE MIN(5000000, (CAST(price / 100 AS INTEGER) * quantity))
            END) VIRTUAL,
        "ge_slot"	INTEGER,
        "account_name"	TEXT COLLATE NOCASE,
        "status"	INTEGER NOT NULL DEFAULT 1,
        "executed"	INTEGER NOT NULL DEFAULT 0,
        "tag" TEXT NOT NULL,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("transaction_id" AUTOINCREMENT),
        FOREIGN KEY("account_name") REFERENCES "account"("account_name"),
        FOREIGN KEY("item_id") REFERENCES "item"("item_id"),
        FOREIGN KEY("raw_transaction_id") REFERENCES "raw_transaction"("transaction_id"),
        CHECK("is_buy" BETWEEN 0 AND 1),
        CHECK("ge_slot" IS NULL OR "ge_slot" BETWEEN 0 AND 7)
    )
    """


table_stock_count = \
    """CREATE TABLE "stock_count" (
        "transaction_id"	INTEGER,
        "item_id"	INTEGER NOT NULL,
        "timestamp"	INTEGER NOT NULL,
        "n_counted"	INTEGER NOT NULL,
        "apply_price"	INTEGER,
        "set_price"     INTEGER,
        "status"	INTEGER NOT NULL DEFAULT 1,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("transaction_id" AUTOINCREMENT),
        FOREIGN KEY("item_id") REFERENCES "item"("item_id")
    )
    """


table_production_rule = \
    """CREATE TABLE "production_rules" (
        "production_rule_id" INTEGER,
        "name" TEXT COLLATE NOCASE,
        PRIMARY KEY("production_rule_id" AUTOINCREMENT)
    )
    """


table_production = \
    """CREATE TABLE "item_production" (
        "transaction_id"	INTEGER,
        "production_rule_id" INTEGER NOT NULL,
        "task_id"           INTEGER,
        "item_id"	INTEGER NOT NULL,
        "timestamp"	INTEGER NOT NULL,
        "quantity"	INTEGER NOT NULL,
        "price"	INTEGER,
        "update_timestamp"	INTEGER NOT NULL,
        PRIMARY KEY("transaction_id" AUTOINCREMENT),
        FOREIGN KEY("production_rule_id") REFERENCES "production_rules"("production_rule_id"),
        FOREIGN KEY("item_id") REFERENCES "item"("item_id")
    )
    """

_dict = dict(locals())

_keys = ('account', 'inventory', 'item', 'raw_exchange_logger_transaction', 'raw_flipping_utilities_transaction',
         'raw_runelite_export_transaction', 'raw_transaction', 'transaction', 'production', 'production_rule')

sql_create_table = (namedtuple(
    "CreateTableSQL",
    _keys
)(*[_dict[f"table_{k}"] for k in _keys]))
"""NamedTuple with all the CREATE TABLE statements found in this module"""

del _keys, _dict

__all__ = "sql_create_table"
