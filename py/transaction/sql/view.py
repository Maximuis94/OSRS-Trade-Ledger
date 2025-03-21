"""
Module with Local Database CREATE VIEW statements

"""
from collections import namedtuple

view_transaction = \
    f"""
    CREATE VIEW transaction_with_datetime_and_item_name AS
    SELECT
        t.transaction_id,
        t.raw_transaction_id,
        t.item_id,
        i.item_name, -- Added item_name
        t.timestamp_created,
        datetime(t.timestamp_created, 'unixepoch') AS timestamp_created_datetime, -- Added datetime for timestamp_created
        t.timestamp,
        datetime(t.timestamp, 'unixepoch') AS timestamp_datetime, -- Added datetime for timestamp
        t.is_buy,
        t.quantity,
        t.max_quantity,
        t.price,
        t.offered_price,
        t.value,
        t.ge_slot,
        t.account_name,
        t.status,
        t.tag,
        t.update_timestamp,
        datetime(t.update_timestamp, 'unixepoch') AS update_timestamp_datetime, -- Added datetime for update_timestamp
        t.tax
    FROM
        "transaction" t
    LEFT JOIN
        "item" i ON t.item_id = i.item_id;
    
    """

create_transaction_summary_view = """
CREATE VIEW IF NOT EXISTS v_transaction_summary AS
SELECT 
    t.item_id,
    i.item_name,
    date(t.timestamp, 'unixepoch') as trade_date,
    t.account_name,
    SUM(CASE WHEN t.is_buy THEN t.quantity ELSE 0 END) as total_bought,
    SUM(CASE WHEN NOT t.is_buy THEN t.quantity ELSE 0 END) as total_sold,
    AVG(CASE WHEN t.is_buy THEN t.price END) as avg_buy_price,
    AVG(CASE WHEN NOT t.is_buy THEN t.price END) as avg_sell_price,
    COUNT(DISTINCT t.source) as source_count
FROM raw_transaction t
JOIN item i ON t.item_id = i.item_id
GROUP BY t.item_id, i.item_name, trade_date, t.account_name
"""

create_transaction_conflicts_view = """
CREATE VIEW IF NOT EXISTS v_transaction_conflicts AS
SELECT 
    t1.transaction_id as transaction_id_1,
    t2.transaction_id as transaction_id_2,
    t1.item_id,
    t1.timestamp,
    t1.source as source_1,
    t2.source as source_2
FROM raw_transaction t1
JOIN raw_transaction t2 ON 
    t1.item_id = t2.item_id AND
    t1.timestamp = t2.timestamp AND
    t1.quantity = t2.quantity AND
    t1.price = t2.price AND
    t1.transaction_id < t2.transaction_id
"""

# print(tuple({k[len("view_"):]: v for k, v in dict(locals()).items() if k.startswith("view_")}.keys()))

_dict = dict(locals())
_keys = ('transaction',)

sql_create_view = namedtuple(
    "CreateViewSQL",
    _keys
)(*[_dict[f"view_{k}"] for k in _keys])
"""NamedTuple with all the CREATE VIEW statements found in this module"""

del _dict, _keys
__all__ = "sql_create_view"
