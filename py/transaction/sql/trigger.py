"""
Module with SQL statements for creating triggers that are used by non-timeseries databases.

A trigger is an SQL query that is executed if some event has occurred. 'Some event' may refer to updating a row in a
specific table, for instance. The underlying logic for each trigger is explained in the Attributes section below.

Attributes
----------
trigger_raw_transaction_post_insert_tag_update : str
    CREATE TRIGGER statement that implements a specific system regarding transaction tags. In automatically sourced
    transactions from plugins, the tag indicates which sources have contributed to this transaction. These tags help in
    merging data from various sources, among others. If a tag is already assigned, this tag *always* overrules the
    automatically imposed tag.
    If there is no tag and no source_id, the transaction is likely to be from the early stages of the database and will
    be tagged as "L", referring to Legacy transaction.
    
trigger_transaction_post_insert_inventory_row : str
    CREATE TRIGGER statement that inserts and subsequently updates inventory rows upon insertion of a new transaction.
    The trigger ensures that inventory reflects the cumulative effect of transactions, maintaining balance, average
    buy price, profit, and other relevant metrics. It also sets the 'executed' flag based on the transaction's
    timestamp relative to other transactions for the same item.
    
trigger_transaction_post_update_inventory_row : str
    SQL statement that updates inventory rows following an update to a transaction. It recalculates and updates
    inventory metrics such as balance, average buy price, profit, and 'executed' status, ensuring consistency
    with the updated transaction data.
    
trigger_transaction_post_delete_inventory_row : str
    CREATE TRIGGER statement that deletes the corresponding inventory row when a transaction is deleted and updates
    subsequent inventory rows for the same item. This trigger maintains the integrity of the inventory data by
    reflecting the removal of a transaction and adjusting the cumulative metrics accordingly.
"""
from collections import namedtuple

from typing import Dict, NamedTuple

min_exchange_logger_timestamp: int = 1703186155
"""First timestamp registered from the exchange_logger"""

trigger_raw_transaction_post_insert_tag_update = \
    f"""
    CREATE TRIGGER raw_transaction_post_insert_tag_update
    AFTER INSERT ON "raw_transaction"
    FOR EACH ROW
    BEGIN
      UPDATE raw_transaction
      SET tag = (
        CASE
          WHEN NEW.tag IS NOT NULL THEN NEW.tag
          WHEN NEW.exchange_logger_id IS NULL
               AND NEW.flipping_utilities_id IS NULL
               AND NEW.runelite_export_id IS NULL
               AND NEW.timestamp < {min_exchange_logger_timestamp} THEN 'L'
          WHEN NEW.exchange_logger_id IS NOT NULL
               AND NEW.flipping_utilities_id IS NULL
               AND NEW.runelite_export_id IS NULL THEN 'E'
          WHEN NEW.flipping_utilities_id IS NOT NULL
               AND NEW.exchange_logger_id IS NULL
               AND NEW.runelite_export_id IS NULL THEN 'F'
          WHEN NEW.runelite_export_id IS NOT NULL
               AND NEW.exchange_logger_id IS NULL
               AND NEW.flipping_utilities_id IS NULL THEN 'R'
          WHEN NEW.flipping_utilities_id IS NOT NULL
               AND NEW.runelite_export_id IS NOT NULL
               AND NEW.exchange_logger_id IS NOT NULL THEN 'A'
          WHEN NEW.flipping_utilities_id IS NOT NULL
               AND NEW.runelite_export_id IS NULL
               AND NEW.exchange_logger_id IS NOT NULL THEN 'B'
          WHEN NEW.flipping_utilities_id IS NULL
               AND NEW.runelite_export_id IS NOT NULL
               AND NEW.exchange_logger_id IS NOT NULL THEN 'C'
          WHEN NEW.flipping_utilities_id IS NOT NULL
               AND NEW.runelite_export_id IS NOT NULL
               AND NEW.exchange_logger_id IS NULL THEN 'D'
          ELSE ""
        END
      )
      WHERE transaction_id = NEW.transaction_id;
    END;

    """
"""Updates the tag in case it has no value. "L" refers to legacy transaction. See module docstring for more info"""


trigger_transaction_post_insert_inventory_row = \
    f"""
    -- Trigger for INSERT on transaction table
    CREATE TRIGGER IF NOT EXISTS transaction_insert_inventory
    AFTER INSERT ON "transaction"
    BEGIN
        -- Insert a new inventory row
        INSERT INTO "inventory" (
            "transaction_id",
            "item_id",
            "balance",
            "average_buy_price",
            "profit",
            "tax",
            "invested_value",
            "current_value",
            "n_purchases",
            "n_bought",
            "n_sales",
            "n_sold",
            "executed",
            "update_timestamp",
            "timestamp"
        )
        SELECT
            NEW.transaction_id,
            NEW.item_id,
            CASE WHEN NEW.is_buy = 1 THEN NEW.quantity ELSE -NEW.quantity END,
            CASE WHEN NEW.is_buy = 1 THEN NEW.price ELSE 0 END,
            0,
            NEW.tax,
            CASE WHEN NEW.is_buy = 1 THEN NEW.price * NEW.quantity ELSE 0 END,
            0,
            CASE WHEN NEW.is_buy = 1 THEN 1 ELSE 0 END,
            CASE WHEN NEW.is_buy = 1 THEN NEW.quantity ELSE 0 END,
            CASE WHEN NEW.is_buy = 0 THEN 1 ELSE 0 END,
            CASE WHEN NEW.is_buy = 0 THEN NEW.quantity ELSE 0 END,
            CASE
                WHEN NEW.timestamp = (SELECT MAX(t2.timestamp) FROM "transaction" t2 WHERE t2.item_id = NEW.item_id) THEN 1
                ELSE 0
            END,
            NEW.update_timestamp,
            NEW.timestamp
        ;
        UPDATE "inventory"
        SET
            "balance" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE -t.quantity END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "average_buy_price" = (SELECT
                                        CASE
                                            WHEN SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END) > 0
                                            THEN CAST(SUM(CASE WHEN t.is_buy = 1 THEN t.price * t.quantity ELSE 0 END) AS REAL) / SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END)
                                            ELSE 0
                                        END
                                    FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "profit" = (SELECT
                            SUM(
                                (CASE WHEN t.is_buy = 0 THEN t.price * t.quantity ELSE 0 END) -
                                (CASE WHEN t.is_buy = 0 THEN (SELECT i2.average_buy_price from inventory i2 where i2.transaction_id = t.transaction_id)*t.quantity ELSE 0 END)
                            )
                        FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "tax" = (SELECT SUM(t.tax) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "invested_value" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.price * t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "current_value" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN t.price * t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "n_purchases" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN 1 ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "n_bought" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "n_sales" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN 1 ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "n_sold" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
            "executed" = CASE
                WHEN i.timestamp = (SELECT MAX(i2.timestamp) from inventory i2 where i2.item_id = NEW.item_id) THEN 1
                ELSE 0
            END,
            "update_timestamp" = NEW.update_timestamp
        WHERE "item_id" = NEW.item_id AND "timestamp" >= (SELECT timestamp from inventory where transaction_id = NEW.transaction_id);
    END;
    """

trigger_transaction_post_update_inventory_row = \
    """
    UPDATE "inventory"
    SET
        "balance" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE -t.quantity END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "average_buy_price" = (SELECT
                                    CASE
                                        WHEN SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END) > 0
                                        THEN CAST(SUM(CASE WHEN t.is_buy = 1 THEN t.price * t.quantity ELSE 0 END) AS REAL) / SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END)
                                        ELSE 0
                                    END
                                FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "profit" = (SELECT
                        SUM(
                            (CASE WHEN t.is_buy = 0 THEN t.price * t.quantity ELSE 0 END) -
                            (CASE WHEN t.is_buy = 0 THEN (SELECT i2.average_buy_price from inventory i2 where i2.transaction_id = t.transaction_id)*t.quantity ELSE 0 END)
                        )
                    FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "tax" = (SELECT SUM(t.tax) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "invested_value" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.price * t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "current_value" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN t.price * t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "n_purchases" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN 1 ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "n_bought" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "n_sales" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN 1 ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "n_sold" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = NEW.item_id AND t.timestamp <= NEW.timestamp AND t.status = 1),
        "executed" = CASE
            WHEN i.timestamp = (SELECT MAX(i2.timestamp) from inventory i2 where i2.item_id = NEW.item_id) THEN 1
            ELSE 0
        END,
        "update_timestamp" = NEW.update_timestamp
    WHERE "item_id" = NEW.item_id AND "timestamp" >= (SELECT timestamp from inventory where transaction_id = NEW.transaction_id);
    """


trigger_transaction_post_delete_inventory_row = \
    f"""
    CREATE TRIGGER IF NOT EXISTS transaction_delete_inventory
    AFTER DELETE ON "transaction"
    BEGIN
        -- Delete the corresponding inventory row
        DELETE FROM "inventory" WHERE "transaction_id" = OLD.transaction_id;
    
        -- Update inventory rows for the same item_id and later timestamps
        UPDATE "inventory"
        SET
            "balance" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE -t.quantity END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "average_buy_price" = (SELECT
                                        CASE
                                            WHEN SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END) > 0
                                            THEN CAST(SUM(CASE WHEN t.is_buy = 1 THEN t.price * t.quantity ELSE 0 END) AS REAL) / SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END)
                                            ELSE 0
                                        END
                                    FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "profit" = (SELECT
                            SUM(
                                (CASE WHEN t.is_buy = 0 THEN t.price * t.quantity ELSE 0 END) -
                                (CASE WHEN t.is_buy = 0 THEN (SELECT i2.average_buy_price from inventory i2 where i2.transaction_id = t.transaction_id)*t.quantity ELSE 0 END)
                            )
                        FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "tax" = (SELECT SUM(t.tax) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "invested_value" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.price * t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "current_value" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN t.price * t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "n_purchases" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN 1 ELSE 0 END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "n_bought" = (SELECT SUM(CASE WHEN t.is_buy = 1 THEN t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "n_sales" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN 1 ELSE 0 END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "n_sold" = (SELECT SUM(CASE WHEN t.is_buy = 0 THEN t.quantity ELSE 0 END) FROM "transaction" t WHERE t.item_id = OLD.item_id AND t.timestamp <= i.timestamp AND t.status = 1),
            "executed" = CASE
                WHEN i.timestamp = (SELECT MAX(i2.timestamp) from inventory i2 where i2.item_id = OLD.item_id) THEN 1
                ELSE 0
            END,
            "update_timestamp" = strftime('%s','now')
        WHERE "item_id" = OLD.item_id AND "timestamp" > OLD.timestamp;
    END;
    """

_dict = dict(locals())

_keys = ('raw_transaction_post_insert_tag_update', 'transaction_post_insert_inventory_row',
         'transaction_post_update_inventory_row', 'transaction_post_delete_inventory_row')

sql_create_trigger = namedtuple(
    "CreateTriggerSQL",
    _keys
)(*[_dict[f"trigger_{k}"] for k in _keys])
"""NamedTuple with all the CREATE TRIGGER statements found in this module"""

del _dict, _keys
__all__ = "sql_create_trigger"
