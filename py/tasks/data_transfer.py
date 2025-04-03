"""
This module contains all implementations involving database updates.

Updating the database consists of the following steps;
1. Convert+copy batch files to drive
2. Insert converted batch rows into timeseries database
3. Insert table data from rbpi into timeseries database
4. Insert item data from rbpi into timeseries database
5. Update the NpyDb + listbox rows.

These steps are implemented below. They are typically combined and executed via an executable script.
"""
import datetime
import sqlite3
from typing import List

import pandas as pd

from venv_auto_loader.active_venv import *
import global_variables.configurations as cfg
import global_variables.osrs as go
import global_variables.path as gp
import sqlite.executable
import sqlite.row_factories
import util.file as uf
import util.str_formats as fmt
from common.classes.database import sql_create_timeseries_item_table, ROConn
from common.item import Item, augment_itemdb_entry
# from common.classes.database import ROConn
from model.timeseries import sql_timeseries_insert
from sqlite.row_factories import factory_idx0, factory_dict
from util.logger import prt

__t0__ = time.perf_counter()

_t_commit = int(time.perf_counter() + cfg.data_transfer_commit_frequency)
_t_print = int(time.perf_counter() + cfg.data_transfer_print_frequency)
_dtn = datetime.datetime.now()
n_start = None
ts_threshold = int(gp.f_db_timeseries.ctime())

small_batch_log = {}
if gp.f_small_batch_log.exists():
    small_batch_log = gp.f_small_batch_log.load()


def is_invalid_export(b: str) -> bool:
    # print(int(_dtn.strftime('%y%m%d%H%M%S')), b[:12])
    return not (os.path.splitext(b)[-1] == '.db' and b[:12].isdigit() and int(b[:12]) <= int(_dtn.strftime('%y%m%d%H%M%S')))


def is_invalid_batch(b: str) -> bool:
    return not (os.path.splitext(b)[-1] == '.db' and b[:6] == 'batch_' and b[6:8].isdigit())


# @deprecated
def transfer_rbpi_db_files(dir_from: str = gp.dir_rbpi_dat, dir_to: str = gp.dir_temp, max_log_size: int = 48) \
        -> Tuple[List[str], List[str]]:
    """
    [OBSOLETE] Old version for transferring db files. Use transfer_rbpi_db_exports() instead.
    
    Copy the db batches from the raspberry pi to the disk. Returns a list of copied files and deletable copies
    """
    global small_batch_log
    small_batch_log = gp.f_small_batch_log.load() if gp.f_small_batch_log.exists() else []
    copied_files, to_remove = [], []
    for b in gp.get_files(src=dir_from):
        if is_invalid_batch(b):
            continue
        from_file = dir_from + b
        try:
            is_large_batch = b[6:9].isdigit()
            
            if not is_large_batch:
                log_entry = (int(b[6:8]), int(os.path.getmtime(from_file)), int(os.path.getsize(from_file)/1000))
                if log_entry in small_batch_log:
                    continue
            else:
                log_entry = None
            # to_file =
            to_file = dir_to + (f"{int(os.path.getmtime(dir_from+b))}.db" if is_large_batch else b)
            if os.path.exists(to_file) and is_large_batch:
                to_file = to_file[:-3]+'_.db'
            print('Copying', os.path.split(from_file)[-1], os.path.split(to_file)[-1], end='\r')
            shutil.copy2(from_file, to_file)
            copied_files.append(ROConn(to_file))
            if is_large_batch:
                to_remove.append(from_file)
                # print('Removing', from_file)
            else:
                to_remove.append(to_file)
                if log_entry is not None:
                    small_batch_log.append(log_entry)
        except IndexError:
            ...
    return copied_files, to_remove


def transfer_rbpi_db_exports(dir_from: str = gp.dir_rbpi_exports, dir_to: str = gp.dir_temp) \
        -> Tuple[List[ROConn], List[str], List[int]]:
    """
    Iterate over db files in `dir_from` that are deemed to be valid exports, given the path. Copy the files to `dir_to`
    and save a read-only connection with each db file. While establishing connections, also compile a list of unique
    item_ids found within the connected database.
    Return as a list of read-only connections, a list of source files that could be removed and a list of unique
    item_ids found across all connections.
    
    Parameters
    ----------
    dir_from : str, optional, global_variables.path.dir_rbpi_exports by default
        Source dir that is to be parsed for db files
    dir_to : str, optional, global_variables.path.dir_temp by default
        Destination dir in which the db files are copied to
    
    Returns
    -------
    List[ROConn]
        Read-only connections, one for every successfully copied file
    
    List[str]
        Source files that have been successfully copied from `dir_from`
    
    List[int]
        List of unique item_ids, taken across all db files
    """
    global small_batch_log
    sql_items = "SELECT DISTINCT item_id FROM timeseries"
    small_batch_log = gp.f_small_batch_log.load() if gp.f_small_batch_log.exists() else []
    connections, to_remove, item_ids = [], [], None
    for b in gp.get_files(src=dir_from):
        if is_invalid_export(b):
            continue
        from_file = dir_from + b
        try:
            to_file = dir_to + b
            if os.path.exists(to_file):
                to_file = to_file[:-3]+'_.db'
            print('Copying', os.path.split(from_file)[-1], os.path.split(to_file)[-1], end='\r')
            shutil.copy2(from_file, to_file)
            rc = ROConn(to_file)
            c = rc.con.cursor()
            c.row_factory = lambda cursor, row: row[0]
            if item_ids is None:
                item_ids = c.execute(sql_items).fetchall()
            else:
                item_ids += list(frozenset(item_ids).difference(c.execute(sql_items).fetchall()))
            connections.append(rc)
            to_remove.append(from_file)
        except IndexError:
            ...
    # print(len(item_ids), item_ids)
    return connections, to_remove, item_ids if item_ids is not None else go.item_ids


def timeseries_transfer_merged(path: str = gp.f_db_timeseries, start_time: int or float = time.perf_counter()):
    """ Transfer exported rbpi batches, then extracted rows from rbpi sqlite dbs """
    global small_batch_log
    timeseries_exe_start, size_ = time.perf_counter(), os.path.getsize(path)
    if 60 < time.time() % 86400 < 240:
        print(' *** A batch is about to be added, rerun the script in 5 minutes ***')
        _ = input('Press ENTER to close')
        exit(1)
    if 90 < time.time() % 3600 < 180:
        max_time = int(time.time()-time.time()%3600+180)
        while time.time() < max_time:
            time.sleep(1.0)
            print(f'Sleeping for {max_time-time.time():.1f}s...', end='\r')
    # copied_files, to_remove = transfer_rbpi_db_files()
    copied_files, to_remove, item_ids = transfer_rbpi_db_exports()
    copied_files.append(None)
    # copied_files += _copied_files
    # to_remove
    
    con_to = sqlite3.connect(path)
    success, skipped = [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]
    item_ids.sort()
    transfer_start = time.perf_counter()
    
    def print_current_file(cur: ROConn):
        prt(f'Current file: {cur.file}' + ' ' * 10, end='\r')
        
    for idx, b in enumerate(copied_files):
        b_success, b_skipped = [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]
        if b is None:
            b = ROConn(gp.dir_rbpi_dat + 'timeseries.db', allow_wcon=True)
        print_current_file(b)
        # for r in con_from.execute(f"""SELECT item_id, src, timestamp, price, volume FROM 'timeseries'
        #                        WHERE item_id NOT IN {go.timeseries_skip_ids} AND timestamp < ?""", max_ts).fetchall():
        for r in b.con.execute(f"""SELECT item_id, src, timestamp, price, volume FROM 'timeseries' WHERE item_id NOT IN {go.timeseries_skip_ids}""").fetchall():
            src = r[1]
            sql_insert = sql_timeseries_insert(item_id=r[0], replace=False)
            try:
                con_to.execute(sql_insert, r[1:])
                b_success[src] += 1
                success[src] += 1
            except sqlite3.IntegrityError:
                b_skipped[src] += 1
                skipped[src] += 1
            except sqlite3.OperationalError as e:
                if 'no such table: item' in str(e):
                    con_to.execute(sql_create_timeseries_item_table(item_id=r[0], check_exists=False))
                    prt(f" * Created table 'item{r[0]:0>5}'")
                    print_current_file(b)
                    con_to.execute(sql_insert, r[1:])
                else:
                    raise e
        con_to.commit()
        prt(f'[{idx+1}] {b.file}: ins={b_success} / skip={b_skipped} | '
              f'Total: insert={sum(b_success)} skip={sum(b_skipped)} sum={sum(b_skipped)+sum(b_success)}')
        b.con.close()
        
        if b.file[-13:] != 'timeseries.db':
            os.remove(to_remove[idx])
            print(f"\t\tDeleted src file {to_remove[idx]}")
        # print(f"""os.remove({to_remove[idx]})""")
    
    # Remove copied small batches from PC (they will be merged later) and large batches from the rbpi
    # print(f'\tRemoving {len(to_remove)} obsolete batch file{"s" if len(to_remove) > 1 else ""}...')
    # for b in to_remove:
    #     # os.remove(b)
    #     print(f"""os.remove({b})""")
    
    # gp.f_small_batch_log.save(small_batch_log)

    print('\t************************************************************************************\n'
          f'\tTotal insertions: {success} | Total skips: {skipped}\n'
          f'\tDB size: +{fmt.fsize(os.path.getsize(path)-size_)} | '
          f'Runtime: {fmt.delta_t(time.perf_counter()-start_time)}\n')


def copy_batches():
    for b in [f for f in gp.get_files(src=gp.dir_rbpi_dat) if os.path.splitext(f)[-1] == '.db' and f[:6] == 'batch_']:
        try:
            if not b[6:8].isdigit():
                continue
            shutil.copy2(gp.dir_rbpi_dat+b, gp.dir_temp+b)
            
            if b[6:9].isdigit():
                os.remove(gp.dir_rbpi_dat+b)
        except IndexError:
            ...


data_types = {'src': 'UInt8', 'item_id': 'UInt16', 'timestamp': 'UInt32', 'price': 'UInt32', 'volume': 'UInt32'}


def db_to_df(db_path: str, out_dir: str) -> bool:
    """ Convert an sqlite timeseries db into a pandas dataframe, set memory-efficient datatypes and pickle it """
    con = sqlite3.connect(db_path)
    t0 = time.perf_counter()
    n_rows = con.execute("""SELECT COUNT(*) FROM 'timeseries'""").fetchone()[0]

    def _row_factory(sqlite_cursor: sqlite3.Cursor, row) -> dict:
        """ Method that can be set as row_factory of a sqlite3.Connection so it will return dicts """
        if row[0] > 2:
            row = (*row[:4], None)
        return {col[0]: row[idx] for idx, col in enumerate(sqlite_cursor.description)}
        
    out_file = out_dir+os.path.split(db_path)[-1][:-3].replace('_', '') + '.dat'
    con.row_factory = _row_factory
    
    if os.path.exists(out_file):
        df = pd.concat([uf.load(out_file),
                        pd.DataFrame(con.execute("SELECT src, item_id, timestamp, price, volume FROM 'timeseries'").fetchall(),
                        columns=list(data_types.keys())).astype(data_types)])
        df.drop_duplicates().to_pickle(out_file)
        return True
    else:
        pd.DataFrame(con.execute("SELECT src, item_id, timestamp, price, volume FROM 'timeseries'").fetchall(),
                     columns=list(data_types.keys())).astype(data_types).to_pickle(out_file)
        return True
    

def timeseries_transfer_new():
    """  """
    batches = [f for f in gp.get_files(src=gp.dir_temp) if os.path.splitext(f)[-1] == '.db' and f[:6] == 'batch_']
    failed_insertions = 0
    success = 0
    to_remove = []
    for large_batch in (True,):# False):
        for b in batches:
            if not b[6:9].isdigit() or not os.path.exists(gp.dir_temp+b):
                print(f'Skipping {b}')
                continue
            n_e, n, n_i = 0, 0, 0
            
            try:
                db = sqlite3.connect(gp.f_db_timeseries)
                con = sqlite3.connect(gp.dir_temp+b)
                for row in con.execute('SELECT item_id, src, timestamp, price, volume FROM timeseries').fetchall():
                    if row[0] in go.timeseries_skip_ids:
                        continue
                    try:
                        db.execute(f"""INSERT INTO "item{row[0]:0>5}" (src, timestamp, price, volume) VALUES
                                (?, ?, ?, ?)""", row[1:])
                        success += 1
                    except sqlite3.IntegrityError:
                        n_i += 1
                    except sqlite3.Error as e:
                        n_e += 1
                        failed_insertions += 1
                        print(n_e, b, e)
                db.commit()
                db.close()
                if large_batch and b[6:9].isdigit():
                    batches.remove(b)
                    if n_e == 0:
                        print(f'Removing batch {b} ({n} rows extracted / {n_e} failed / {n_i} skipped)')
                        to_remove.append(gp.dir_temp+b)
                if n_e > 0:
                    print(f'Did not remove batch {b} due to {n_e} failed insertions...')
            except IndexError:
                continue
    print(f'Successful inserts:', success)
    return to_remove
    
    

##############################################################################################
# Item data transfer
##############################################################################################


def parse_item_data(path: str) -> dict:
    """
    Parse item meta-data from `db_from` and return it as a list of dicts. Remo
    
    Parameters
    ----------
    path : str
        Path to database file

    Returns
    -------
    dict
        A dict with item_id as key and a dict with item metadata as entry

    """
    if not os.path.exists(path):
        raise FileNotFoundError
    
    items = {}
    con = sqlite3.connect(path)
    con.row_factory = sqlite.row_factories.factory_dict
    
    for next_item in con.execute("""SELECT id, item_id, item_name, members, alch_value, buy_limit, stackable,
                                    release_date, equipable, weight, update_ts FROM itemdb""").fetchall():
        items[next_item.get('item_id')] = next_item
    con.close()
    return items
    

def insert_items(start_time: int or float = time.perf_counter()):
    """ Transfer all rows from idb to `db_path` """
    print(f' [{fmt.passed_pc(start_time)}] Updating item data...')
    count = 0
    db = sqlite3.connect(gp.f_db_local)
    db_from = sqlite3.connect(gp.f_db_rbpi_item)
    c = db_from.cursor()
    db.row_factory, c.row_factory = factory_idx0, factory_idx0
    new_items = tuple(frozenset(c.execute("SELECT DISTINCT item_id FROM itemdb").fetchall()).difference(db.execute("SELECT DISTINCT item_id FROM item").fetchall()))
    c.row_factory = factory_dict
    # for item_row in pickle.load(open(gp.f_db_item, 'rb')).to_dict(
    #         'records'):
    sql = f"SELECT * FROM itemdb WHERE item_id IN ({', '.join([str(el) for el in new_items])})"
    # TODO: expand with data updates instead of just adding new items
    for item_row in c.execute(sql):
        
        item_row = {k: v for k, v in augment_itemdb_entry(Item(**item_row)).__dict__.items() if
                    k in Item.sqlite_columns()}
        # print(item_row)
        # continue
        # exit(1)
        try:
            db.execute(
                """INSERT INTO "item" (id, item_id, item_name, members, alch_value, buy_limit, stackable, release_date,
                equipable, weight, update_ts, augment_data, remap_to, remap_price, remap_quantity, target_buy,
                target_sell, item_group) VALUES (:id, :item_id, :item_name, :members, :alch_value, :buy_limit,
                :stackable, :release_date, :equipable, :weight, :update_ts, :augment_data, :remap_to, :remap_price,
                :remap_quantity, :target_buy, :target_sell, :item_group)""",
                item_row)
            count += 1
        except sqlite3.IntegrityError:
            ...
    # exit(1)
    db.commit()
    db.close()
    print(f'\tAdded {count} new items')
    # exit(1)
    # src_db = rbpi_dbs.get('item')
    # idb = ItemController(path=src_db.path, table_name=src_db.table, augment_items=False)
    # rows = []
    # for item_id, data in parse_item_data(path=src_db.path).items():
    #     i = idb.get_item(item_id)
    #     if i is None:
    #         continue
    #     if isinstance(i, Item):
    #         i.__dict__.update(data)
    #         rows.append(i.__dict__)
    #         # idb.update_item(i)
    # idb.insert_rows(table_name=idb.table_name, rows=rows, replace=True)
    # idb.close()
    print(f'\tUpdated item data in {fmt.delta_t(time.perf_counter()-start_time)}\n\n')


def archive_rbpi_db_files(dir_src: str = gp.dir_temp, dir_dst: str = gp.dir_df_archive):
    """
    Convert the large db batches into dataframes and transfer them to the archive. This archive is originally designed
    for short-term backups. For a more structured/properly organized timeseries backup, an extract from the timeseries
    db file is probably more desirable.
    *** SOURCE DB FILES ARE REMOVED UPON COMPLETION ***
    
    Parameters
    ----------
    dir_src : str, optional, global_values.path.dir_temp by default
        Folder with all the source files
    dir_dst : str, optional, global_values.path.dir_df_archive by default
        Folder in which the dataframes are to be saved
    
    Notes
    -----
    Only db files that have a unix timestamp as their first 10 characters are processed. Underscores are stripped; if
    the resulting file already exists, the new data will be merged with the existing dataframe.

    """
    from global_variables.values import min_avg5m_ts
    for db_file in uf.get_files(src=dir_src, ext='db', full_path=False, add_folders=False):
        try:
            if not db_file[:10].isdigit():
                continue
            else:
                db_ts = int(db_file[:10])
                if db_ts > time.time() or db_ts < min_avg5m_ts:
                    continue
                db_path = dir_src+db_file
        except IndexError:
            continue
        if db_to_df(db_path, out_dir=dir_dst):
            os.remove(db_path)


if __name__ == "__main__":
    archive_rbpi_db_files()
    
    
    
    exit(123)
