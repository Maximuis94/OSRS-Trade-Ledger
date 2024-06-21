"""
This module contains various methods with sqlite database interactions.



"""
import os
import sqlite3
import time
from typing import Dict

import util.str_formats as fmt
import sqlite.row_factories as row_factory


def connect(path: str, read_only: bool = True) -> sqlite3.Connection:
    """ Connect to the sqlite database located at `path`. If `read_only`, establish a read-only connection. """
    return sqlite3.connect(database=f'file:{path}?mode=ro' if read_only else path, uri=read_only)


def get_db_contents(c: sqlite3.Connection or sqlite3.Cursor or str, get_tables: bool = True, get_indices: bool = True,
                    get_auto_generated_indices: bool = False) -> (Dict[str, dict], Dict[str, dict]):
    """
    Get contents of the sqlite db specified by `c` and return its tables and indices. The exact output depends on the
    parameters given.
    
    Parameters
    ----------
    c : sqlite3.Connection or sqlite3.Cursor or str
        An active connection with a database or a database path to connect with
    get_tables : bool, optional, True by default
        If True, include tables from `c` in the output
    get_indices : bool, optional, True by default
        If True, include indices from `c` in the output
    get_auto_generated_indices : bool, optional, False by default
        If True, include auto-generated indices from `c` in the output, provided `get_indices` is True

    Returns
    -------
    Dict[str, dict], Dict[str, dict]
        If `get_indices` and `get_tables` are True, tables and indices are returned as a tuple of dicts
    
    """
    
    if not get_tables and not get_indices:
        raise ValueError(f"At the very least either get_tables or get_indices should be True...")
    
    if isinstance(c, str):
        c = connect(path=c)
    c.row_factory, tables, indices = row_factory.factory_dict, {}, {}
    
    for el in c.execute('SELECT * FROM sqlite_master').fetchall():
        _type = el.get('type')
        if get_tables and _type == 'table':
            tables[el.get('name')] = el
        elif get_indices and _type == 'index' and \
                (not get_auto_generated_indices and el.get('sql') is not None or get_auto_generated_indices):
            indices[el.get('name')] = el
    return tables, indices


def get_tables(db: sqlite3.Connection) -> Dict[str, tuple]:
    """ Extract a table list from the parsed sqlite_master schema """
    output = {}
    tables_parsed = get_db_contents(c=db, get_indices=False, get_auto_generated_indices=False)[0]
    for name in list(tables_parsed.keys()):
        table_parsed = tables_parsed.get(name)
        if table_parsed.get('type') != 'table':
            continue
        sql = table_parsed.get('sql')
        _create = sql.split(f'"{name}"(')[-1]
        tables = [el.replace('"', '').split(' ')[0] for el in _create.split(', "')]
        output[name] = tuple([row for row in tables if len(row) > 0])
    return output


def count_rows(c: sqlite3.Cursor or sqlite3.Connection, table: str, group_by_column: str = None):
    """
    Count the rows in `table` in the database `c` is connected with. If a column_name is passed to group the results by,
    return a
    
    
    Parameters
    ----------
    c : sqlite3.Connection or sqlite3.Cursor
        Active connection with the databse
    table : str
        Name of the table that is to have its rows counted
    group_by_column : str, optional, None by default
        If passed, row counts are grouped per value of column `group_by_column`

    Returns
    -------
    Dict[str, int]
        If the results are not grouped by a column, return a dict {table_name: row_count}
    Dict[any, dict]
        If the results are grouped by a column, return a dict with values of the grouped column as key and
        a dict {column_name: column_value, 'row_count': row_count} as entries
    """
    # Use a new cursor since the row_factory is getting reset
    c = c.cursor()
    
    if group_by_column is None:
        c.row_factory = row_factory.factory_single_value
        return {table: {'table': table, 'row_count': c.execute(f"""SELECT COUNT(*) FROM '{table}'""").fetchone()}}
    else:
        c.row_factory = row_factory.factory_tuple
        # return c.execute(f"""SELECT {group_by_column}, COUNT(*) FROM '{table}'""").fetchall()
        return {value_group: {group_by_column: value_group, 'row_count': row_count} for value_group, row_count in
                c.execute(f"""SELECT {group_by_column}, COUNT(*) FROM '{table}' GROUP BY {group_by_column}""").fetchall()}


def vacuum_into(src_db: str, target_db: str = None, user_prompt: bool = False):
    """ Connect with `src_db` and execute the VACUUM INTO statement, using `target_db` as output db file. """
    t0 = time.perf_counter()
    file_size = os.path.getsize(src_db)
    if target_db is None:
        target_db = src_db[:-len(src_db.split('.')[-1])-1]+'_.db'
        db_src = sqlite3.connect(src_db)
        print(f"VACUUM main INTO '{target_db}' ")
        db_src.execute(f"VACUUM main INTO '{target_db}' ")
        db_dst = sqlite3.connect(database=f'file:{target_db}?mode=ro', uri=True)
        
        # table_dict = get_db_contents(src_db, get_tables=True, get_indices=False)[0]
        db_dst.row_factory, db_src.row_factory = row_factory.factory_single_value, row_factory.factory_single_value
        rename_target_into_src, new_size = True, os.path.getsize(target_db)
        # for name, table in table_dict.items():
        #     sql = f"""SELECT COUNT(*) FROM {name}"""
        #
        #     count_src, count_dst = db_dst.execute(sql).fetchone(), db_src.execute(sql).fetchone()
        #     if count_src != count_dst:
        #         rename_target_into_src = False
        #         print(f'Mismatch between row counts for table {name} (src={count_src}, dst={count_dst})')
    else:
        rename_target_into_src = False
        if src_db == target_db:
            raise ValueError("Unable to run VACUUM INTO on an identical source and target db. Use VACUUM instead...")
        if not os.path.exists(src_db):
            raise FileNotFoundError(f"Unable to VACUUM from a non-existent db {src_db}")
        if os.path.exists(target_db):
            raise FileExistsError(f"Unable to VACUUM to an already existing db {src_db}")
        print(f'Attempting to VACUUM {src_db} of size {os.path.getsize(src_db)/1000000:.1f}MB into {target_db}')
        print(f"VACUUM main INTO {target_db}")
        sqlite3.connect(src_db).cursor().execute("VACUUM main INTO :target_db", {'target_db': target_db})
        new_size = os.path.getsize(target_db)
    print(f'Completed .db VACUUM in {fmt.delta_t(time.perf_counter()-t0)} | '
          f'DB size: [{fmt.fsize(file_size)} -> {fmt.fsize(new_size)}] ({fmt.fsize(file_size-new_size)})')
    if rename_target_into_src:
        print(f'Renaming db...')
        try:
            os.remove(src_db)
            os.rename(target_db, src_db)
        except PermissionError:
            print(f'Failed to rename db {src_db} to {target_db}; close connection and rename it manually instead')
    if user_prompt:
        input(f'VACUUM was completed in {fmt.passed_time(t0, True)}')
    else:
        print(f'VACUUM was completed in {fmt.passed_time(t0, True)}')



