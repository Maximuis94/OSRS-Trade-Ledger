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
import os
import sqlite3
import time
from typing import List

import pandas as pd

import global_variables.configurations as cfg
import global_variables.path as gp
import global_variables.osrs as go
import global_variables.variables as var
import sqlite.executable_statements
import sqlite.row_factories
import util.file as uf
import util.str_formats as fmt
from controller.item import ItemController, Item
from global_variables.data_classes import TimeseriesRow, rbpi_dbs, Row
from model.database import Database
from util.data_structures import datapoint

# This is the order in which src id values are assigned when creating primary keys.
src_ids = '', 'a', 'w', 'r'


_t_commit = int(time.perf_counter() + cfg.data_transfer_commit_frequency)
_t_print = int(time.perf_counter() + cfg.data_transfer_print_frequency)
n_start = None
ts_threshold = int(os.path.getctime(gp.f_db_timeseries))


def commit(_db: sqlite3.Connection, msg: str = None, force: bool = False, close_db: bool = False,
           suffix: str = '                                       '):
    """ Print `msg` if applicable and commit `_db` if applicable (don't print/commit too frequently), or if `force`"""
    global _t_commit, _t_print
    
    if time.perf_counter() > min(_t_commit, _t_print) or force:
        cur_time = int(time.perf_counter())
        
        if cur_time > _t_commit or force:
            _db.commit()
            _t_commit = cur_time + cfg.data_transfer_commit_frequency
            if close_db:
                _db.close()
        
        if cur_time > _t_print or force:
            if msg is not None:
                if force:
                    print('')
                print(msg + suffix, end='\n' if force else '\r')
            _t_print = cur_time + cfg.data_transfer_print_frequency


def convert_row(src: int, row: dict):
    """ Convert a dict row to a tuple that can be inserted into the database """
    output = [row.get('timestamp')]
    if src <= 2:
        # print(row)
        prefix = 'buy_' if src == 1 else 'sell_' if src == 2 else ''
        output.append(row.get(f'{prefix}price'))
        output.append(row.get(f'{prefix}volume'))
        return tuple(output)
    
    else:
        b = row.get('is_buy')
        if b == 1 and src == 3 or b == 0 and src == 4:
            output.append(row.get(f'price'))
            output.append(0)
            return tuple(output)
    raise ValueError


def parse_batch(batch_path: str, out_file: str = None, dir_out: str = None) -> List[TimeseriesRow]:
    """
    Parse a batch file and convert it into a list of TimeseriesRows. The input batch file is assumed to be a list of
    (datatype dicts, rows) tuples or two lists of datatype dicts and rows.
    Depending on the given parameters, it is also possible to export the converted rows to the path specified.
    
    Parameters
    ----------
    batch_path : str
        path to the batch file
    out_file : str, optional, None by default
        If passed, export the rows to this path
    dir_out : str, optional, None by default
        If passed, export the parsed list of rows to a file in `dir_out`. If no `out_file` was passed, generate one
        formatted like 1717675200_1717689600.npy

    Returns
    -------
    List[TimeseriesRow]

    """
    batch, parsed = uf.load(batch_path), []
    min_ts, max_ts, track_ts = int(time.time()), 0, dir_out is not None and out_file is None
    n = 0
    try:
        for dt, rows in batch:
            for row in pd.DataFrame(rows, columns=list(dt.keys())).astype(dt).to_dict('records'):
                dp = datapoint(row)
                n += len(dp)
                parsed += dp
                if track_ts and row.get('buy_price') is not None and row.get('item_id') == 2:
                    ts = row.get('timestamp')
                    min_ts, max_ts = min(min_ts, ts), max(max_ts, ts)
    except ValueError:
        if isinstance(batch[0], Row):
            parsed = batch
            for el in batch:
                if el.item_id in go.most_traded_items:
                    ts = el.timestamp
                    min_ts, max_ts = min(min_ts, ts), max(max_ts, ts)
        else:
            for dt, rows in zip(batch[0], batch[1]):
                
                for row in pd.DataFrame(rows, columns=list(dt.keys())).astype(dt).to_dict('records'):
                    
                    try:
                        dp = datapoint(row)
                        parsed += dp
                        n += len(dp)
                        if dir_out and row.get('buy_price') is not None and row.get('item_id') == 2:
                            ts = row.get('timestamp')
                            min_ts, max_ts = min(min_ts, ts), max(max_ts, ts)
                    except ValueError:
                        if row.get('is_sale') is not None:
                            row['is_buy'] = 1-int(row['is_sale'])
                            parsed += datapoint(row)
                            n += 1
    
    if track_ts:
        min_ts, max_ts = round(min_ts/14400, 0)*14400, round(max_ts/14400, 0)*14400
        out_file = dir_out + f'{min_ts:.0f}_{max_ts:.0f}' + os.path.splitext(batch_path)[-1]
    elif dir_out is not None:
        out_file = dir_out + out_file
    if out_file is not None:
        if os.path.exists(out_file) and len(uf.load(out_file)) != len(parsed):
            raise FileExistsError
        uf.save([row.tuple() for row in parsed], out_file)
        if len(uf.load(out_file)) != len(parsed):
            raise RuntimeError(f"Something went wrong copying the file...")
        ...
    return parsed


def insert_batch(db: Database, batch_path: str, skip_ids: list = (9044, 9050, 26247, 2660), remove_src: bool = False):
    """ Insert the batch located at `batch_path` into Database `db` """
    n_per_src = [0, 0, 0, 0, 0]
    
    for row in parse_batch(batch_path):
        if row.item_id in skip_ids:
            continue
        try:
            db.execute(
                f"INSERT OR REPLACE INTO 'item{row.item_id:0>5}'(src, timestamp, price, volume) VALUES (?, ?, ?, ?)",
                row.tuple()[1:])
        except sqlite3.Error as e:
            print(e)
            if 'no such table' in str(e):
                print('Encountered a new item_id; generating table and reformatting batch')
                db.rollback()
                print(f'Creating table item{row.item_id:0>5}')
                db.execute(
                    f"""CREATE TABLE "item{row.item_id:0>5}"("src" INTEGER NOT NULL CHECK (src BETWEEN 0 AND 4), "timestamp" INTEGER NOT NULL, "price" INTEGER NOT NULL DEFAULT 0 CHECK (price>=0), "volume" INTEGER NOT NULL DEFAULT 0 CHECK (volume>=0), PRIMARY KEY(src, timestamp) )""")
                db.commit()
                return insert_batch(db=db, batch_path=batch_path, skip_ids=skip_ids, remove_src=remove_src)
        n_per_src[row.src] += 1
    db.commit()
    
    n, repl = sum(n_per_src), [('realtime', 'rt'), ('sell', 's'), ('buy', 'b')]
    s = '\tRows inserted: '
    for src, _n in zip(var.timeseries_srcs, n_per_src):
        for el in repl:
            src.replace(*el)
        s += f'{src}: {_n}, '
    print(s+f'total: {n}')
    if remove_src:
        os.remove(batch_path)
        print(f'\t * Deleted source batch file {os.path.split(batch_path)[1]}')
    
    return n
    

def copy_batches(src_folder: str = gp.dir_rbpi_dat, transfer_folder: str = gp.dir_batch):
    """ Copy the batches from the rbpi to the local drive, while converting the contents and naming it appropriately """
    transferred_batches = []
    print(f'\tCopying batch files;\n\tFrom SRC={src_folder}\n\tTo DST={transfer_folder}...')

    # First get a list of files to copy...
    for batch_path in uf.get_files(src=gp.dir_rbpi_dat, ext='npy'):
        parse_batch(batch_path, dir_out=transfer_folder)
        transferred_batches.append(batch_path)
    return transferred_batches
    

def batch_transfer(db_to: Database, min_ts: int = None):
    """ Parse batches in rbpi data folder, transfer contents and delete them """
    batch_start = time.perf_counter()
    rows, ts_min, ts_max = [0, 0, 0], None, None
    transferred_batches = copy_batches()
    
    if min_ts is not None:
        min_ts = min_ts - min_ts % cfg.rbpi_batch_timespan
        transferred_batches = [gp.dir_batch+f for f in uf.get_files(gp.dir_batch, ext='npy', full_path=False)
                               if int(f.split('_')[0]) >= min_ts]
    
    n_rows = {}
    print(f'\tInserting batches...')
    for batch_path in transferred_batches:
        print(f" * Current file: DST/{batch_path.split('/')[-1]} (size={os.path.getsize(batch_path)/pow(10,6):.1f}mb)")
        idx, t_ = 0, time.perf_counter()
        n_rows[batch_path] = insert_batch(db=db_to, batch_path=batch_path, remove_src=True)
        print(f'\tInserted {n_rows.get(batch_path)} rows from batch {os.path.split(batch_path)[1]} '
              f'in {fmt.delta_t(time.perf_counter()-t_)}')
        
    n, srcs, repl = '', list(n_rows.keys()), [('realtime', 'rt'), ('sell', 's'), ('buy', 'b')]
    for src, n_ in n_rows.items():
        for el in repl:
            src = src.replace(*el)
        n += f'{src}: {n_}, '
    print(f'\tTransferred rows [{n[:-2]}] | Time taken: {fmt.delta_t(time.perf_counter()-batch_start)}s')
    
    n = ''
    n_inserted = 0
    for src, n_ in zip(list(n_rows.keys()), rows):
        n += f'{src}: {n_}, '
    for _, n in n_rows.items():
        n_inserted += n
    print(f'\tTotal rows transferred from batches: [{n_inserted}]')
    ts_max = db_to.get_max(table='item00002', column='timestamp')
    return ts_max


def get_insert(item_id: int, src: int):
    """ Return an INSERT OR REPLACE statement for table item`item_id`  """
    return f"""INSERT OR REPLACE INTO "item{item_id:0>5}"(src, timestamp, price, volume) VALUES ({src}, ?, ?, ?)"""


def parse_tables(db_to: sqlite3.Connection, db_dict: dict = rbpi_dbs, t0: int = None):
    """ Parse avg5m, realtime and wiki data from rbpi sqlite tables and transfer it. """
    t_ = time.perf_counter()
    t1 = int(time.time() - time.time() % 3600)
    _types = {col: var.types.get(col) for col in ['item_id']+list(var.avg5m_columns)+list(var.realtime_columns)+list(var.wiki_columns)}
    # print(_types)
    global ts_threshold
    if t0 is not None:
        ts_threshold = t0-1
        
    def rbpi_table_row_factory(c: sqlite3.Cursor, _row) -> dict:
        """ Parse rows from rbpi sqlite tables """
        r = {}
        for i, col in enumerate(c.description):
            if i == 0:
                continue
            col = col[0]
            try:
                r[col] = _types.get(col).py(_row[i])
            except AttributeError:
                if _row[i] in (0, 1):
                    r['is_buy'] = bool(abs(_row[i] - 1))
                else:
                    print(i, r, str([_col[0] for _col in c.description]))
                    print(_row)
                    raise AttributeError
        return r

    n = {}
    inserted_rows = [0, 0, 0, 0, 0]

    for src in ['avg5m', 'realtime', 'wiki']:
        if db_dict.get(src) is None:
            raise ValueError(f'db_dict {db_dict} does not have an entry for {src}')
        
        path, table = db_dict.get(src)
        con = Database(path, parse_tables=False, read_only=False)
        con.row_factory = rbpi_table_row_factory
        # _t0 = con.get_max(table=src, column='timestamp', suffix=f"WHERE timestamp > {ts_threshold}")-1
        rows = con.execute(f"SELECT * FROM {table} WHERE timestamp > ? AND timestamp < ?", (t0, t1)).fetchall()
        con.close()
        
        if len(rows) > 0:
            _row = rows[0]
            sql = sqlite.executable_statements.insert_sql_dict(row=rows[0], table=src, replace=True)
            # sql_new = """INSERT OR REPLACE INTO """
            n[src] = len(rows)
            for row in rows:
                try:
                    if row.get('buy_price') is None:
                        src = 4 - int(row.get('is_buy')) if row.get('volume') is None else 0
                        try:
                            db_to.execute(get_insert(item_id=row.get('item_id'), src=src), convert_row(src=src, row=row))
                            # if src > 0:
                            #     print(src, row, convert_row(src=src, row=row))
                            inserted_rows[src] += 1
                        except ValueError:
                            ...
                    else:
                        for src in (1, 2):
                            try:
                                db_to.execute(get_insert(item_id=row.get('item_id'), src=src), convert_row(src=src, row=row))
                                inserted_rows[src] += 1
                            except ValueError:
                                ...
                except sqlite3.Error as e:
                    print(f' Sqlite3 error occurred while executing {sql}\n\tParameters: {row}\n\t{e}')
            db_to.commit()
        row_str = ""
        for table, n_insert in zip(var.timeseries_srcs, inserted_rows):
            row_str += f'{table}: {n_insert} | '
    print(f'\tTransferred [{row_str[:-2]}] rows to table {src}')
    print(f'\tParsed all sqlite databases in {1000*(time.perf_counter()-t_):.0f}ms\n')


def timeseries_transfer(path: str = gp.f_db_timeseries):
    """ Transfer exported rbpi batches, then extracted rows from rbpi sqlite dbs """
    timeseries_exe_start, size_ = time.perf_counter(), os.path.getsize(path)
    if 260 < time.time() % 14400 < 480:
        print(' *** A batch is about to be added, rerun the script in 5 minutes ***')
        _ = input('Press ENTER to close')
        exit(1)
    print(f' [{fmt.dt_(fmt_str="%d-%m %H:%M:%S")}] Importing data from the Raspberry Pi...')
    timeseries_database = Database(path, read_only=False)
    print(f' Transferring batches...')
    t_ = time.perf_counter()
    min_table_ts = batch_transfer(db_to=timeseries_database)
    print(f'\tTransferred batches in {fmt.delta_t(time.perf_counter()-t_)}\n')
    
    print(f' Transferring Raspberry Pi Sqlite rows...')
    t_ = time.perf_counter()
    parse_tables(db_to=timeseries_database, db_dict=rbpi_dbs, t0=min_table_ts)
    print(f'\tParsed all rbpi sqlite databases in {1000 * (time.perf_counter() - t_):.0f}ms\n')
    timeseries_database.commit()
    timeseries_database.close()
    print(f' Transferred timeseries data in {fmt.delta_t(time.perf_counter()-timeseries_exe_start)} | '
          f'File size increased by {(os.path.getsize(path)-size_)/1000000:.1f}mb\n\n')
    

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
    # for next_item in ItemController(path, augment_items=False, table=table, table_name='itemdb').all_items():
    for next_item in con.execute("""SELECT id, item_id, item_name, members, alch_value, buy_limit, stackable, release_date, equipable, weight, update_ts FROM itemdb""").fetchall():
        items[next_item.get('item_id')] = next_item
    con.close()
    return items
    

def insert_items():
    """ Transfer all rows from idb to `db_path` """
    t_ = time.perf_counter()
    print(f' Updating item data...')
    src_db = rbpi_dbs.get('item')
    idb = ItemController(path=src_db.path, table_name=src_db.table, augment_items=False)
    rows = []
    for item_id, data in parse_item_data(path=src_db.path).items():
        i = idb.get_item(item_id)
        if i is None:
            continue
        if isinstance(i, Item):
            i.__dict__.update(data)
            rows.append(i.__dict__)
            # idb.update_item(i)
    idb.insert_rows(table_name=idb.table_name, rows=rows, replace=True)
    idb.close()
    print(f'\tUpdated item data in {fmt.delta_t(time.perf_counter()-t_)}\n\n')




if __name__ == "__main__":
    ...
    