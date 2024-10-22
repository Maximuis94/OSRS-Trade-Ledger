"""
Module with implementations for setting up databases.
To prevent circular imports, this module should only be imported by the setup module.


"""
import os.path
import sqlite3
import time
from collections.abc import Iterable

import pandas as pd

from import_parent_folder import recursive_import
import global_variables.configurations as cfg
import global_variables.osrs
import global_variables.path as gp
import global_variables.variables as var
import sqlite.databases as sql_db
import util.str_formats
from file.file import File
from model.database import Database, sql_create_timeseries_item_table
from sqlite.executable import create_index

del recursive_import

def setup_sqlite_db(local_path: File = None, timeseries_db_path: File = None, add_index: bool = False, hush: bool = False,
                    **kwargs):
    """
    Setup the sqlite db designed for this project at path `db_file`.
    
    Parameters
    ----------
    local_path : str, optional, None by default
        Path
    timeseries_db_path : str, optional, None by default
        Path
    add_index : str, optional, None by default
        Path
    hush : str, optional, None by default
        Path
        
    Other Parameters
    ----------------
    new_db : bool, optional, None by default
        If True, delete the db file on paths before creating the databases, if they exist. Note that this will happen 
        without any warning.

    Returns
    -------
    bool
        Returns True if the database was created without any issues
    
    See Also
    --------
    model.database
        The Database and its subclasses are defined in model.database
    
    Raises
    ------
    FileExistsError
        If a file at `path` exists before attempting to setup the database, a FileExistsError exception is raised. This 
        can be bypassed by passing `new_db=True`
    
    Notes
    -----
    The db file can also be generated via other means. In the Database constructor, the tables are extracted from the
    connected database, which means it does not necessarily have to be the database as described below. Aside from a
    setup method, this method also serves as a reference for what the db is expected to look like.
    
    """
    # external, local = isinstance(external_db_path, str), isinstance(local_path, str)
    prt = lambda s: (... if hush else print(s))
    new_db = kwargs.get('new_db')
    
    if local_path is not None and local_path.exists():
        if new_db:
            local_path.delete()
        else:
            print(f"Unable to execute db setup for existing db {timeseries_db_path}")
            local_path = None
    if timeseries_db_path is not None and timeseries_db_path.exists():
        if new_db:
            timeseries_db_path.delete()
        else:
            print(f"Unable to execute db setup for existing db {timeseries_db_path}")
            timeseries_db_path = None
    if local_path is None and timeseries_db_path is None:
        
        raise FileExistsError(f"Unable to execute db setup; local/external db paths have not been specified or are "
                              f"already being used as a db")
    
    if timeseries_db_path is not None:
        prt(f'Attempting to setup timeseries database at {timeseries_db_path}...')
        db = Database(path=timeseries_db_path, parse_tables=False, tables=[sql_db.avg5m, sql_db.realtime, sql_db.wiki])
        db.create_tables(tables=[sql_db.avg5m, sql_db.realtime, sql_db.wiki], hush=hush, if_not_exists=not new_db)
        if add_index:
            for src, table in db.tables.items():
                db.execute(table.sql_create_index(f'index_{src}_item', 'item_id'))
                # db.execute(table.create_index(f'index_{src}_item_ts', ['item_id', 'timestamp']))
                # if src == 'realtime':
                #     db.execute(table.create_index(f'index_{src}_ts_item_buy', ['timestamp', 'item_id', 'is_buy']))
        prt(f'Successfully setup timeseries db with tables avg5m, realtime and wiki')
              # foreign_keys=Table.generate_foreign_key(name='fk_w_idb', column='item_id', reference_table='item')),
        db.commit()
        db.close()
    
    if local_path is not None:
        prt(f'Attempting to setup local db at {local_path}...')
        db = Database(path=local_path, parse_tables=False, tables=[sql_db.item, sql_db.transaction])
        db.create_tables(tables=[sql_db.item, sql_db.transaction], hush=hush, if_not_exists=not new_db)
        prt(f'Successfully setup local db with tables transaction and item')
        
        if add_index:
            t_transaction = db.tables.get('transaction')
            db.execute(t_transaction.sql_create_index('index_transaction_item', 'item_id'))
            db.execute(t_transaction.sql_create_index('index_transaction_ts', 'timestamp'))
            # db.execute(t_transaction.create_index('index_transaction_uts', 'update_ts'))
            db.execute(t_transaction.sql_create_index('index_transaction_item_ts', ['item_id', 'timestamp']))
            # db.execute(t_transaction.create_index('index_transaction_item_uts', ['item_id', 'update_ts']))
            # db.execute(t_transaction.create_index('index_transaction_item_buy', ['item_id', 'is_buy']))
            db.execute(t_transaction.sql_create_index('index_transaction_item_buy', ['item_id', 'is_buy']))
            db.execute(t_transaction.sql_create_index('index_transaction_item_tag', ['item_id', 'tag']))
            # db.execute(t_transaction.create_index('index_transaction_item_ts_tag', ['item_id', 'timestamp', 'tag']))
            
            t_item = db.tables.get('item')
            db.execute(t_item.sql_create_index('index_item_item_id', 'item_id'))
            db.execute(t_item.sql_create_index('index_item_augment', 'augment_data'))
        db.commit()
        db.close()
    return True


def generate_test_db(src_dbs: str or Iterable[str], **kwargs):
    """
     Generate a database that can be used for testing. It contains one week of timeseries data and all transactions,
      item and wiki entries.
     
    Parameters
    ----------
    src_dbs: str or collections.abc.Iterable[str]
        Database path(s) that will be used for input

    Returns
    -------

    """
    try:
        setup_sqlite_db(local_path=gp.f_db_sandbox, timeseries_db_path=gp.f_db_sandbox, add_index=True)
    except FileExistsError:
        ...
    item_ids = []
    test_db = Database(path=gp.f_db_sandbox)
    src_dbs = [Database(path=p, read_only=True) for p in ([src_dbs] if isinstance(src_dbs, str) else src_dbs)]
    tables = ['item', 'transaction']
    for t in list(test_db.tables.keys()):
        if t not in tables:
            tables.append(t)
    print(src_dbs[0].tables)
    tables = ['transaction']
    for t in tables:
        for db in src_dbs:
            if t+'s' in list(db.tables.keys()):# or t == 'item' and 'itemdb' in list(db.tables.keys()):
                src, dst = db.tables.get(t), test_db.tables.get(t)
                if src is None:
                    src = db.tables.get('transactions')
                
                dst.apply_validation = False
                select = src.select
                print(select)
                p = {}
                
                if t in ['item', 'transaction']:
                    print(select)
                    dst.insert_rows(rows=db.execute(select, p), con=test_db)
                    test_db.commit()
                    if src.name == 'itemdb':
                        test_db.row_factory = lambda c, row: row[0]
                        item_ids = test_db.execute("SELECT item_id FROM item").fetchall()
                else:
                    select += ' WHERE item_id BETWEEN :item_id AND :i2'
                    
                    if src.name != 'wiki':
                        select += ' AND timestamp >= :t0'
                        sql_maxts = f'SELECT MAX(timestamp) FROM {src.name} WHERE item_id=2'
                        p.update({
                            't0': int(db.execute(sql_maxts).fetchone().get('MAX(timestamp)')) - 86400 * 7
                        })
                    i = 0
                    while i < len(item_ids):
                        ids = item_ids[i:i+500] if i + 500 < len(item_ids) else item_ids[i:]
                        p.update({'item_id': ids[0], 'i2': ids[-1]})
                        print(p, select)
                        dst.insert_rows(rows=db.execute(select, p), con=test_db)
                        i += 500
                break
    test_db.commit()


def get_timeseries_indices(tables: Iterable and not str, index_columns):
    """ Generates a series of indices for timeseries tables """
    indices = []
    for table in tables:
        for el in ['timestamp', ('item_id', 'timestamp')]:
            index_name = f'index_{table}_'
            _el = [el] if isinstance(el, str) else el
            for s in _el:
                index_name += f"{s.replace('_', '')}_"
            
            exe = create_index(index_name[:-1], table, el)
            indices.append(exe+';')


def setup_entity_db(force_setup: bool = False) -> bool:
    """
    Setup the entity database. The entity database is not supposed to have any rows inserted, as it serves as a
    template for what the database tables should look like, according to the hard-coded tables in sqlite.databases.
    If the template database is older than the config value for the entity db update frequency, it will be removed and 
    re-created.
    
    Parameters
    ----------
    force_setup : bool, optional, False by default
        If True, Remove the existing db file and create a new one, if it already exists. It is possible force_setup can 
        get overridden via the entity db update frequency.
    
    Returns
    -------
    bool
        Return True if a new entity db was created, False if not
    
    Notes
    -----
    This is a sqlite db representation of the hard-coded tables in sqlite.tables. It is a blueprint by design, which is
    why it contains all tables defined in sqlite.databases and why it should NOT be used to upload data to, unless it is
    moved to a different path.
    """
    path, t_ = gp.f_db_entity, time.perf_counter_ns()
    if force_setup or not os.path.exists(path) or time.time()-os.path.getmtime(path) > cfg.entity_db_update_frequency:
        print(f'Setting up entity database at {path}...')
        try:
            os.remove(path)
        except FileNotFoundError:
            ...
        Database(path=path, tables=[t for _, t in sql_db.tables.items()], parse_tables=False).create_tables(hush=True)
        print(f'Entity database was created in {(time.perf_counter_ns() - t_) // pow(10, 6):.0f}ms!')
        return True
    return False


def average_times(item, avg5m, rt, wiki):
    try:
        s = 'Average query times: '
        for list_id, _list in zip(['item', 'avg5m', 'rt', 'wiki'], [item, avg5m, rt, wiki]):
            s += f'[{list_id}={util.str_formats.delta_t(sum(_list)/len(_list))}] '
        return s
    except ValueError:
        return ''
    except ZeroDivisionError:
        return ''

def count_row_per_item(commit_frequency: int = 15):
    db = Database(gp.f_db_timeseries, read_only=True)
    t_ = time.perf_counter()
    n_rows = 0
    csv_rows = []
    item_list = global_variables.osrs.npy_items
    n_items = len(item_list)
    averages = ''
    t_commit = time.perf_counter()+commit_frequency
    query_times_item, query_times_wiki, query_times_avg5m, query_times_rt = [], [], [], []
    for idx, item_id in enumerate(item_list):
        item_start = time.perf_counter()
        to_print=f'item_id={item_id} {idx}/{n_items} {idx/n_items*100:.1f}% '
        row = {'ítem_id': item_id, 'item_name': global_variables.osrs.id_name[item_id]}
        s = f'Rows/table for item_id={item_id}: '
        suffix = f'WHERE item_id={item_id} '
        for table in var.tables_timeseries:
            print(f'[{util.str_formats.delta_t(time.perf_counter() - t_)}] {to_print+averages}', end='\r')
            if table == 'wiki':
                query_start = time.perf_counter()
                row_count = db.count_rows(table=table, suffix=suffix)[0]
                n_rows += row_count
                row[f"{table}"] = row_count
                s += f'{table}={util.str_formats.number(row_count, max_decimals=1)} '
                query_times_wiki.append(time.perf_counter() - query_start)

            elif table == 'realtime':
                for is_buy in 0, 1:
                    query_start = time.perf_counter()
                    b = "buy" if bool(is_buy) else "sell"
                    row_count = db.count_rows(table=table, suffix=suffix + f'AND is_buy={is_buy}')[0]
                    # print(row_count)
                    n_rows += row_count
                    row[f"{table}_{b}"] = row_count
                    s += f'{table}_{b}={util.str_formats.number(row_count, max_decimals=1)} '
                    query_times_rt.append(time.perf_counter()-query_start)

            else:
                for is_buy in 0, 1:
                    query_start = time.perf_counter()
                    b = "buy" if bool(is_buy) else "sell"
                    row_count = db.count_rows(table=table, suffix=suffix + f'AND {b}_price>0')[0]
                    # print(row_count)
                    n_rows += row_count
                    row[f"{table}_{b}"] = row_count
                    s += f'{table}_{b}={util.str_formats.number(row_count, max_decimals=1)} '
                    query_times_avg5m.append(time.perf_counter()-query_start)
        query_times_item.append(time.perf_counter()-item_start)
        averages = average_times(item=query_times_item, avg5m=query_times_avg5m, rt=query_times_rt, wiki=query_times_wiki)
        row['average_query_times'] = averages
        row['string'] = s
        csv_rows.append(row)
        try:
            if time.perf_counter() > t_commit:
                t_commit = time.perf_counter() + commit_frequency
                pd.DataFrame(csv_rows).to_csv(gp.dir_data + 'row_counts_per_item.csv', index=False)
                
        except PermissionError:
            ...
    pd.DataFrame(csv_rows).to_csv(gp.dir_data + 'row_counts_per_item_full.csv', index=False)
    print('\n')
    input(f'Query time={util.str_formats.delta_t(time.perf_counter() - t_)} | N rows: {n_rows}                   ')
    
    print('_')


def create_timeseries_db(db_file: str, item_ids: Iterable):
    con = sqlite3.connect(db_file)
    
    for i in item_ids:
        con.execute(sql_create_timeseries_item_table(i))
    con.commit()
    con.close()


if __name__ == '__main__':
    path = None
    db = sqlite3.connect(gp.f_db_local)
    db.row_factory = lambda c, row: row[0]
    create_timeseries_db(path, db.execute("""SELECT DISTINCT item_id FROM item""").fetchall())
    exit(1)
    print(type(gp.f_db_sandbox))
    db = sqlite3.connect(gp.f_db_sandbox)
    print(db.execute("SELECT MAX(timestamp) FROM avg5m").fetchone())
    exit(1)
    sqlite3.connect(gp.f_db_timeseries).execute("VACUUM")
    exit(1)
    setup_entity_db(force_setup=True)
    exit(1)
    generate_test_db(src_dbs=[gp.f_db_timeseries, gp.f_db_transaction])
    # generate_test_db(src_dbs=[gp.f_db_transaction])
    exit(1)
    hush = False
    local, timeseries = gp.f_db_sandbox, gp.f_db_sandbox
    archive = None #gp.f_db_archive
    
    if input('Executing databases setup for creating databases at;\n'
             f' * {local}\n * {timeseries}\n * {archive}\n'
             f'Would you like to proceed? Enter "y" to confirm: ').lower() == 'y':
        if isinstance(local, str) or isinstance(timeseries, str):
            try:
                # Create indexed databases with a subset of data
                setup_sqlite_db(local_path=local, timeseries_db_path=timeseries, add_index=True, hush=hush)
            except FileExistsError:
                print('Did not setup indexed sqlite databases')
        
        if isinstance(archive, str):
            try:
                # Create non-indexed databases with a subset of data
                setup_sqlite_db(local_path=archive, timeseries_db_path=archive, hush=hush)
            except FileExistsError:
                print('Did not setup archive databases')
    else:
        print(f'Aborting database setup...')
        time.sleep(5)
        exit(-1)
