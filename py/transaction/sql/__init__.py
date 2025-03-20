"""
Imports all the SQL dicts for the local database
"""
from collections import namedtuple

from dataclasses import dataclass, field

from typing import NamedTuple, Dict, Tuple

import transaction.sql.insert as sql_insert
import transaction.sql.select as sql_select
import transaction.sql.update as sql_update
import transaction.sql.delete as sql_delete
from transaction.sql.index import sql_create_index
from transaction.sql.table import sql_create_table
from transaction.sql.trigger import sql_create_trigger
from transaction.sql.view import sql_create_view


sql_create_database: Tuple[Tuple, Tuple, Tuple, Tuple] = namedtuple(
    "CreateDatabaseSQL",
    ("table", "trigger", "index", "view"),
)(sql_create_table, sql_create_trigger, sql_create_index, sql_create_view)
"""NamedTuple with all the CREATE TABLE statements found in this module"""


# interact_db_sql =


__all__ = ("sql_create_database", "sql_select", "sql_insert", "sql_update")


# import global_variables.path as gp
# import sqlite3
#
# db = sqlite3.connect(f"file:{gp.f_db_transaction_new}?mode=ro", uri=True)
# for name, sql in db.execute("SELECT name, sql FROM sqlite_master WHERE type='table';").fetchall():
#     print(f"Table {name}\n{sql}", end='\n\n\n')
