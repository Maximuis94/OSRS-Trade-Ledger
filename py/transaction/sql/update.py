"""
Local database UPDATE SQL statements
"""
from transaction.constants import TableList


raw_transaction_from_exchange_logger = \
    f"""
        UPDATE "{TableList.RAW_TRANSACTION}"
            SET
                timestamp = ?,
                offered_price = ?,
                value = ?,
                max_quantity = ?,
                ge_slot = ?,
                update_timestamp = CASE WHEN tag="X" OR tag="p" OR tag="e" OR tag="m" OR tag="b" OR tag="c" THEN update_timestamp ELSE ? END,
                tag = CASE WHEN tag="F" OR tag="X" OR tag="p" OR tag="e" OR tag="m" OR tag="b" OR tag="c" THEN tag ELSE ? END,
                exchange_logger_id = ?
            WHERE transaction_id = ?;
            """
"""SQL statement used to update a raw_transaction row with exchange log data provided via a tuple"""


raw_transaction_from_exchange_logger_dict = \
    f"""
    UPDATE "{TableList.RAW_TRANSACTION}"
    SET
        timestamp = :timestamp,
        offered_price = :offered_price,
        value = :value,
        max_quantity = :max_quantity,
        ge_slot = :ge_slot,
        update_timestamp = CASE
                WHEN tag IN ("X", "p", "e", "m", "b", "c") THEN update_timestamp
                ELSE :update_timestamp
            END,
        tag = CASE
            WHEN tag IN ("F", "X", "p", "e", "m", "b", "c") THEN tag
            ELSE :tag
        END,
        exchange_logger_id = :exchange_logger_id
    WHERE transaction_id = :transaction_id;
    """
"""SQL statement used to update a raw_transaction row with exchange log data provided via a dict"""


raw_transaction_from_flipping_utilities_transaction = \
    f"""
    UPDATE "{TableList.RAW_TRANSACTION}"
    SET
        timestamp = ?,
        timestamp_created = ?,
        max_quantity = ?,
        ge_slot = ?,
        account_name = CASE WHEN account_name IS NULL THEN ? ELSE account_name END,
        update_timestamp = CASE WHEN tag="X" OR tag="p" OR tag="e" OR tag="m" OR tag="b" OR tag="c" THEN update_timestamp ELSE ? END,
        flipping_utilities_id = ?
    WHERE transaction_id = ?;
    """
"""SQL statement used to update a raw_transaction row with flipping utilities data"""

raw_transaction_from_flipping_utilities_transaction_dict = \
    f"""
    UPDATE "{TableList.RAW_TRANSACTION}"
    SET
        timestamp = :timestamp,
        timestamp_created = :timestamp_created,
        max_quantity = :max_quantity,
        ge_slot = :ge_slot,
        account_name = CASE WHEN account_name IS NULL THEN :account_name ELSE account_name END,
        update_timestamp = CASE WHEN tag="X" OR tag="p" OR tag="e" OR tag="m" OR tag="b" OR tag="c" THEN update_timestamp ELSE :update_timestamp END,
        flipping_utilities_id = :flipping_utilities_id
    WHERE transaction_id = :transaction_id;
    """
"""SQL statement used to update a raw_transaction row with flipping utilities data provided via a dict"""

raw_transaction_from_runelite_export_transaction = \
    f"""
    UPDATE "{TableList.RAW_TRANSACTION}"
    SET
        timestamp_runelite_export = :timestamp,
        account_name = CASE WHEN account_name IS NULL THEN ? ELSE account_name END,
        update_timestamp = CASE WHEN tag IS NOT NULL THEN update_timestamp ELSE ? END,
        runelite_export_id = ?
    WHERE transaction_id = ?;
    """
"""SQL statement used to update a raw_transaction row with runelite export data"""

raw_transaction_from_runelite_export_transaction_dict = \
    f"""
    UPDATE "{TableList.RAW_TRANSACTION}"
    SET
        timestamp_runelite_export = :timestamp,
        account_name = CASE WHEN account_name IS NULL THEN :account_name ELSE account_name END,
        update_timestamp = CASE WHEN tag IS NOT NULL THEN update_timestamp ELSE :update_timestamp END,
        runelite_export_id = :runelite_export_id
    WHERE transaction_id = :transaction_id;
    """
"""SQL statement used to update a raw_transaction row with runelite export data provided via a dict"""

raw_transaction_from_import_update = \
    f"""UPDATE "{TableList.RAW_TRANSACTION}" SET update_timestamp=?, tag=?, status=? WHERE transaction_id=?"""
f"""Update the raw_transaction row such that its update_timestamp, tag en status"""
sql_update_transaction = (f'UPDATE "{TableList.TRANSACTION}" SET '
                          'item_id = rt.item_id, '
                          'timestamp_created = rt.timestamp_created, '
                          'timestamp = rt.timestamp, '
                          'is_buy = rt.is_buy, '
                          'quantity = rt.quantity, '
                          'max_quantity = rt.max_quantity, '
                          'price = rt.price, '
                          'offered_price = rt.offered_price, '
                          'value = rt.value, '
                          'account_name = rt.account_name, '
                          'ge_slot = rt.ge_slot, '
                          'update_timestamp = rt.update_timestamp '
                          f'FROM "{TableList.RAW_TRANSACTION}" rt '
                          f'WHERE "transaction".raw_transaction_id = rt.transaction_id; ')
"""UPDATE statement used to update an entry from the transaction table with raw_transation data"""
