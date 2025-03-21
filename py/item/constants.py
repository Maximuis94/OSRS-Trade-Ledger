"""
Module with item-related fixed values.
"""
import sqlite3
from collections.abc import Callable
from typing import Tuple

from file.file import File
from global_variables.path import f_db_local as itemdb_file
from sqlite.column import Column
from sqlite.table import Table

ITEMDB_FILE: File = itemdb_file
"""The default ItemDB file"""


ITEMDB_PATH: str = ITEMDB_FILE.path
"""The default path to the ItemDB"""


ITEM_TABLE_NAME: str = "item"
"""Name of the table that is used to store Items"""


ITEM_TABLE: Table = Table(sqlite3.connect(f"file:{ITEMDB_PATH}?mode=ro", uri=True), ITEM_TABLE_NAME)
"""Table representation of the ItemDB item table"""


ITEM_COLUMNS: Tuple[Column, ...] = tuple(ITEM_TABLE.columns)
"""Tuple with Columns found in the ItemDB Table"""


ITEM_ROW_FACTORY: Callable[[sqlite3.Connection, tuple], any] = ITEM_TABLE.row_factory
"""Row factory used to produced an Item when selecting all column values from a Database Row"""

# Diagnostic check to verify that annotated types are equal to actual types.
# if __debug__:
#     _locals = dict(locals())
#     _annotations = __annotations__
#     print(_annotations)
#     _annotated_variables = tuple(_annotations.keys())
#     for variable, expected in _annotations.items():
#         print(expected, isinstance(expected, Iterable))
#         if variable not in _annotated_variables:
#             continue
#         try:
#             identified = type(_locals[variable])
#             if expected != type(identified):
#
#                 # collections.abc module in particular may yield type mismatches, while they are actually correct.
#                 if str(identified).lower().__contains__(expected.__name__.lower()):
#                     continue
#                 print("Mismatch between type annotation and actual typing;")
#                 print('\t', variable, expected)
#                 print(f"\tIdentified type: {identified}")
#                 print(f"\tExpected type: {type(expected)}")
#         except KeyError as e:
#             ...

if __name__ == "__main__":
    
    ...
