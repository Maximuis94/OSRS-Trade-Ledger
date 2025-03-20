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
