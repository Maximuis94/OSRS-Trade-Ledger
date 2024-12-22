"""
This module contains methods used for loading all global values


"""
import sqlite3
from collections import namedtuple
from collections.abc import Iterable
from enum import Enum

from venv_auto_loader.active_venv import *
import sqlite.row_factories
from global_variables.importer import *
from model.database import Database
__t0__ = time.perf_counter()


# def load_item_data(con: sqlite3.Connection) -> [list, list, list, dict, dict]:
#     """
#     Load item data from the sqlite itemdb table. Format it as item_id list, id_name mapping (list), item_name list,
#     name_id mapping (dict) and a id_itemdb_row mapping (dict).
#     All lists and key orders are sorted before returning them.
#
#     Returns
#     -------
#     item_ids: list, id_name: list, item_names: list, name_id: dict, itemdb: dict
#
#     """
#     itemdb, id_name, name_id = {}, {}, {}
#
#     for r in con.execute("SELECT * FROM item").fetchall():
#         itemdb[r.get('item_id')] = r
#         id_name[r.get('item_id')] = r.get('item_name')
#         name_id[r.get('item_name')] = r.get('item_id')
#     item_names, item_ids = list(name_id.keys()), list(id_name.keys())
#     item_names.sort()
#
#     return [
#         item_ids,  # Sorted list (asc) of all item_ids in the itemdb table
#         [id_name.get(item_id) for item_id in range(item_ids[-1] + 1)],  # List with item_names on idx=item_id
#         item_names,  # Sorted list (asc) of all item_names in the itemdb table
#         {name: name_id.get(name) for name in item_names},  # Dict mapping of {item_name: item_id}
#         itemdb  # Dict mapping of item_id: sqlite_row_dict
#     ]
#
#
# def load_item_data_v2(con: sqlite3.Connection) -> [list, list, list, dict, dict]:
#     """
#     Load item data from the sqlite itemdb table. Format it as item_id list, id_name mapping (list), item_name list,
#     name_id mapping (dict) and a id_itemdb_row mapping (dict).
#     All lists and key orders are sorted before returning them.
#
#     Returns
#     -------
#     item_ids: list, id_name: list, item_names: list, name_id: dict, itemdb: dict
#     """
#     con.row_factory = dict_factory
#
#     item_ids, item_names, id_name, name_id, itemdb = [], [], {}, {}, {}
#     for el in con.execute("SELECT * FROM item ORDER BY item_name").fetchall():
#         itemdb[el['item_id']] = el
#         item_ids.append(el['item_id'])
#         item_names.append(el['item_name'])
#         id_name[el['item_id']] = el['item_name']
#         name_id[el['item_name']] = el['item_id']
#     item_ids.sort()
#
#     return [
#         item_ids,  # Sorted list (asc) of all item_ids in the itemdb table
#         [id_name.get(item_id) for item_id in range(item_ids[-1] + 1)],  # List with item_names on idx=item_id
#         item_names,  # Sorted list (asc) of all item_names in the itemdb table
#         name_id,  # Dict mapping of {item_name: item_id}
#         itemdb # Dict mapping of item_id: sqlite_row_dict
#     ]

sql = "SELECT COUNT(*) FROM _T_"

sql_rowcount = {t: sql.replace('_T_', t) for t in var.tables}

_sql = sql + " WHERE item_id=:item_id"
sql_rowcount_item = {t: _sql.replace('_T_', f'"{t}"') for t in var.tables_timeseries}


# def count_rows(db: str or Database, **kwargs):
#     if isinstance(db, str):
#         db = Database(db, read_only=True)
#     c = db.cursor()
#     c.row_factory = sqlite.row_factories.factory_single_value
#
#     tables = list(db.tables.keys()) if kwargs.get('tables') is None else kwargs.get('tables')
#     for t in tables:
#         print(t, db.c_idx0.execute(sql_rowcount.get(t)).fetchone())
#     if kwargs.get('item_ids') is None:
#         return var.TableTuple(**{
#             t: db.c_idx0.execute(sql_rowcount.get(t)).fetchone() for t in tables
#         })
#     else:
#         return {
#             item_id: var.TableTuple(**{t: db.c_idx0.execute(sql_rowcount.get(t)).fetchone() for t in tables
#                                        }) for item_id in kwargs.get('item_ids')
#         }
        
        
    
if __name__ == '__main__':
    # print(count_rows(db=gp.f_db_sandbox))
    ...