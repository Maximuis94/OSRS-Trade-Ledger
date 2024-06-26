""" #TODO
This module contains every method exclusively involved in parsing the runelite exchange log.


This module contains all logic w.r.t. parsing and submitting exchange log data generated by the exchange log runelite
plugin. The plugin outputs its data to a .log file, this file is converted into classes defined in this project.

The assumed plugin configurations are that the log is exported as JSON data.

Exchange log data updates realtime and can be parsed frequently.

Each line in the log has the following data;
date: YYYY-MM-DD format
time: HH:MM:SS format
state: BUYING/SELLING/CANCELLED_SELL/CANCELLED_BUY/BOUGHT/SOLD/EMPY
slot: integer, 0-7
item: item_id
qty: Current progress of the offer
worth: gp spent/received so far
max: max quantity
offer: Price per item

The variable names listed above are exchange_log output. They are converted to 'familiar' variable names while creating
a LogTransaction object.

ExchangeLog pipeline simplified:
1. DONE Read log file and return all lines with a completed transaction (cancelled / completed)
2. DONE Convert each line to list of numbers (fixed order; such that it can be archived as a numpy array, but also converted
    to a dataframe or such)
3. DONE Upload transactions to sqlite database; process failed uploads
    > This should ONLY be done if ALL exchange logs have already been transferred to the queue file
    > Failed uploads: Store data somewhere, generate an error log, attempt to notify user somehow.
    > In a nutshell, thoroughly verify whether everything behaves like it should
4. DONE Compare exchange logs with runelite .json files to evaluate performance (generate csvs for both?)
    > export a runelite json and compare it with executing the log parser covering the same timespan
    > Compare the sqlite statements that would have been executed
 => Exchange log parser seems to miss fewer transactions, although some items still ended up having negative balances
5. TODO Implement an activity logger
    > Each session is logged; successful executions should produce no more than one line
    > Errors are logged in a concise manner, providing relevant information for debugging

TODO Evaluate performance of log parser in an experimental setting
EValuation should be done thoroughly. The goal is to run a log parser indefinitely without having to double check it.
Any bugs the script has at that point will result in consistently made errors that are likely to go unnoticed.

TODO Improve archiving protocol
    The current archiving protocol consists of simply renaming processed log files to 'data/exchange_log/archive/' after
     converting its contents to numpy arrays. Raw runelite logs are parsed and uploaded to databases. Theoretically, the
     raw data can be deleted. However, it may be desirable to archive it somewhere. Archived logs are saved for
    reference, but are (hopefully) no longer required. However, since the script will be running without always
    verifying its output, this will at least keep the source data accessible. The data is compressed into a ZIP
    archive and stored in json/txt/... format. Archiving should be focussed on once the processing part is properly
    developed.
    Design considerations include;
      Merged log files versus separate log files
      Archived log line format (What does the raw .log line string translate to)
      Kind of archive; raw table in current sqlite db, pickled dataframe, npy file, csv file
      Archive size; if no line is discarded, archive will be significantly larger than transaction db
        i.e. 21-12-2023 log yielded 25/350 to-be-submitted transactions
    Discarding protocol: Keep as much as information as possible?

TODO: Reduce amount of logged transactions (?)
    Merge adjacent transactions
    Adjacent transactions with the same transaction_type can be merged with little to no impact on the integrity of the
    results. When merging buy transactions for instance, it has no negative effect as long as the resulting quantity and
    buy price are identical to the quantity and buy price before the next sale if the merge would not occur.
    This can be verified by computing the resulting inventory and comparing the results.
    Note that this can also be implemented for all previously logged transactions.
    Keep in mind that this is likely to alter various representations of the transaction data, for instance when viewing
    results per day.
    Alternatively, the impact can be reduced by only merging transactions within certain parameters.


"""
import json
import os.path
import shutil
import sqlite3
import time
from collections.abc import Iterable, Sized

import numpy as np
import pandas as pd

import util.file as uf
from model.item import Item
from controller.item import remap_item, create_item

# import global_values
# import path
# import ts_util
# from ge_util import remap_item
# from global_values import exchange_log_go.exchange_log_states as go.exchange_log_states, \
#     exchange_log_archive_attribute_order
# from ledger import Transaction
# from path import f_runelite_exchange_log as f_log, \
#     load_data, save_data, f_exchange_log_queue as f_transaction_queue, dir_exchange_log_src as dir_log_src, \
#     dir_exchange_log, \
#     f_submitted_lines_log

import global_variables.path as gp
import global_variables.osrs as go
import global_variables.configurations as cfg

import util.unix_time as ut

from model.transaction import Transaction


# Log each line that is parsed during this session with the same timestamp
update_ts = int(time.time())
print(f'Using debug input data!')

# TODO: Implement stock correction logic using pending buy/sell transactions as described below

len_log_file = [len("exchange_yyyy-mm-dd.log")]

# Parsed log attributes that are not supposed to change over time
immutable_attributes = ('slot', 'item', 'max', 'offer', 'is_buy')
renamed_immutable_attributes = ('slot_id', 'item_id', 'max_quantity', 'price', 'is_buy')


def get_next_transaction_id() -> int:
    return sqlite3.connect(database=f'file:{gp.f_db_local}?mode=ro', uri=True)\
        .execute("SELECT MAX(transaction_id) FROM 'transaction'").fetchone()[0]+1


def parse_line(i: str) -> dict:
    """ Parse input line i from exchange_log file; rename and/or convert variables and return them as a dict """
    # input i can be converted into a json object
    if i[0] == '{':
        i = json.loads(i)
        ts = [int(t) for t in (i.get('date').replace('-', ' ') + ' ' + i.get('time').replace(':', ' ')).split(' ')]
        ts = int(time.mktime((ts[0], ts[1], ts[2], ts[3], ts[4], ts[5], 0, 0, 0)))
        state_id, slot_id = go.exchange_log_states.index(i.get('state')), int(i.get('slot'))
        is_buy, item_id, quantity = int(state_id < 3), int(i.get('item')), int(i.get('qty'))
        value, max_quantity = int(i.get('worth')), int(i.get('max'))
        
        # Preferably divide final value by quantity, else stick with offer
        try:
            price = round(value / quantity)
        except ZeroDivisionError:
            price = int(i.get('offer'))
    
    # elif False: # Tabulated output
    #     pass
    
    # elif False: # textual output
    #     pass
    
    else:
        raise ValueError(f"Line {i} could not be identified as json/text/tabulated")
    
    # Make sure the variables are named as they are throughout the project
    # Transaction()
    return {'timestamp': ts, 'is_buy': is_buy, 'item_id': item_id, 'quantity': quantity, 'price': price,
            'max_quantity': max_quantity, 'value': value, 'state_id': state_id, 'slot_id': slot_id}


"""
def process_current_log(src_file: str, out_dir: str, n_added: int = 0):
    # dt = ts_util.ts_to_dt(int(os.gp.getmtime(src_file)))
    # out_file = out_dir + f'archive/exchange_{dt.year}-{dt.month}-{dt.day}_.log'
    # queue_data = ['item_id', 'timestamp', 'is_buy', 'quantity', 'price']
    # queue = load_data(f_transaction_queue)
    # print(f'Loaded queue with {len(queue)} entries')
    # print('output file', out_file)
    # print('source file', src_file)
    # if not os.gp.exists(src_file):
    #     return

    try:
        submitted_lines = open(f_submitted_lines_log, 'r').readlines()
    except FileNotFoundError:
        submitted_lines = []



    # Rename the file to prevent concurrency errors; then stick with that file
    shutil.copy(src_file, src_file.replace('.log', '_.log'))
    # os.rename(src_file, src_file.replace('.log', '_.log'))
    src_file = src_file.replace('.log', '_.log')

    # Parse the output file lines, if it exists
    if os.gp.exists(out_file):
        with open(out_file, 'r') as log:
            parsed_lines = list(log.readlines())
        print(f'Resuming log file with {len(parsed_lines)} lines')
    else:
        parsed_lines = []
        print(f'Not resuming log file')

    # Add lines from the src_file
    with open(src_file, 'r') as log_file:
        new_lines = list(log_file.readlines())
    print(f'Read {len(new_lines)} lines from {log_file}')

    write_lines, as_npy = [], []
    for next_line in new_lines:
        e = ExchangeLogLine(parse_line(next_line))
        as_npy.append(e.to_list(list_dtype='npy_int'))
        if next_line in parsed_lines:
            continue
        as_dict = {k: i for k, i in e.__dict__.items() if k in queue_data}
        if e.status == 1 and e.quantity > 1:
            queue.append(as_dict)
            n_added += 1
        write_lines.append(next_line)

    print(f'Saving queue of length {len(queue)}')
    save_data(queue, f_transaction_queue)
    print(f'Saving npy arrays of length {len(as_npy)}')
    np.save(file=out_file.replace('.log', '.npy'), arr=np.array(as_npy))
    with open(out_file, 'a' if os.gp.exists(out_file) else 'w') as log:
        for next_line in write_lines:
            log.write(next_line)
            print(new_lines.index(next_line), next_line)
    return queue, as_npy
"""


def update_submitted_lines(log_file: str = gp.f_submitted_lines_log, ts_threshold: int = int(time.time()) - 86400 * 3):
    """ Remove all lines from the submitted lines log that are no longer relevant """
    submitted_lines, temp_file = open(log_file, 'r').readlines(), log_file.replace('.log', '_.log')
    filtered_lines, removed_lines = [], []
    print('Removing expired submitted lines...')
    
    # Get all lines that do not exceed threshold
    for next_line in submitted_lines:
        e = ExchangeLogLine(parse_line(next_line))
        if e.timestamp > ts_threshold:
            filtered_lines.append(next_line)
        else:
            # print(f'\t{next_line}')
            removed_lines.append(next_line)
    print(f'Removed {len(removed_lines)} expired lines')
    
    # Export lines to temp file
    with open(temp_file, 'w') as submission_log:
        for next_line in filtered_lines:
            submission_log.write(next_line)
    
    # Overwrite log file
    os.remove(log_file)
    os.rename(temp_file, log_file)


def process_logs(queue_file: str = gp.f_exchange_log_queue, elog_dir: str = gp.dir_exchange_log, add_current: bool = True):
    """
    Process all completed log files in log_dir. In chronological order, log files are parsed and converted into
    objects. All parsed lines are reduced to numerical lists and exported to an archive as such.
    Completed transactions are added to a queue for uploading them into the sqlite db.

    Parameters
    ----------
    queue_file : str, optional, gp.f_transaction_queue by default
        File that contains a list with completed parsed transactions that are to be submitted into the sqlite db
    elog_dir : str, optional, gp.dir_exchange_log by default
        Folder within the project in which files related to the exchange log are stored
    add_current : bool, optional, True by default
        If True, include the exchange.log in the to-do list as well
        

    Notes
    -----
    TODO: refine archiving method
    Ideally the archive refers to a zipped archive with logs in it. For now, implement it using a folder instead. As of
    now, processing is limited to completed log files. This could be extended to the currently active log file, although
     that would require a slightly different approach if the log should be archived as well.


    """
    # Length the file name after completing the log, i.e. exchange_2023-12-21.log
    to_do = [gp.dir_exchange_log_src + f for f in gp.get_files(src=gp.dir_exchange_log_src, extensions=['log']) if len(f) in len_log_file]
    if len(to_do) > 1:
        to_do.sort()
    if add_current:
        to_do.append(gp.f_runelite_exchange_log)
    queue_data = ['item_id', 'timestamp', 'is_buy', 'quantity', 'price']
    
    if not os.path.exists(queue_file):
        queue = []
        uf.save(queue, queue_file)
    else:
        queue = uf.load(queue_file)
        if not isinstance(queue, Sized):
            raise TypeError
        print(f'Loaded queue of length {len(queue)}')
        # print(queue)
    
    # If the queue is empty and there are no new completed log files, abort execution
    if len(queue) == 0 and len(to_do) == 0:
        print(f'No new transactions were found to submit, aborting...')
        return False
    
    if not os.path.exists(gp.f_submitted_lines_log):
        submitted_lines = []
    else:
        submitted_lines = open(gp.f_submitted_lines_log, 'r').readlines()
    new_subs = []
    for log_file in to_do:
        incomplete_log = log_file == gp.f_runelite_exchange_log
        # This is a list with all lines that have been parsed and will be archived at some point.
        with open(log_file, 'r') as log:
            n_added, as_npy = 0, []
            dst_file, ext = elog_dir + 'archive/' + log_file.split('/')[-1], '.' + log_file.split('.')[-1]
            
            for next_line in log.readlines():
                entry = ExchangeLogLine(parse_line(next_line))
                if entry.status == 1 and (entry.quantity > 1 or entry.item_id == 13190) and \
                        next_line not in submitted_lines:
                    queue.append({k: i for k, i in entry.__dict__.items() if k in queue_data})
                    if incomplete_log:
                        new_subs.append(next_line)
                    n_added += 1
                as_npy.append(entry.to_list(list_dtype='npy_int'))
            if n_added > 0:
                uf.save(queue, gp.f_exchange_log_queue)
                print(f'Saved queue with {n_added} new entries from {os.path.split(log_file)[1]}')
            
            # TODO: Implement robust archiving system
            # Archive parsed lines as npy arrays and by simply moving the log to an archive dir
        
        # If file is exchange.log, save submitted lines to prevent duplicate subs
        if incomplete_log:
            with open(gp.f_submitted_lines_log, 'a' if os.path.exists(gp.f_submitted_lines_log) else 'w') as sub_log:
                for next_line in new_subs:
                    sub_log.write(next_line)
        
        # Only save npy arrays / logs if the log is completed
        else:
            np.save(file=dst_file.replace(ext, '.npy'), arr=np.array(as_npy))
            os.rename(log_file, dst_file)
    return True


def submit_transaction_queue(queue_file: str = gp.f_exchange_log_queue, submit_data: bool = True, min_ts: int = None,
                             csv_file: str = 'output/exchange_log_submissions.csv'):
    """
    Submit all transactions that have been queued so far in the queue file in chronological order to the sqlite
    database. The queue is inspected first, checking for duplicate files and such. Upon completion, the database is
    updated, followed by the queue file, from which the submitted transactions are removed.

    Parameters
    ----------
    queue_file : str
        File in which the submission queue is saved.
    submit_data : bool, optional, True by default
        True if submissions should be saved; flag for debugging purposes to verify results without altering local files
    min_ts : int, optional, None by default
        Ignore transactions with a timestamp lower than `min_ts`, if specified
    csv_file : str, optional, None by default
        After submitting transactions to sqlite db, also submit them to this csv file for manual inspection
    """
    
    # Only process if all completed log data has been parsed and transferred!
    logs = [f for f in gp.get_files(src=gp.dir_exchange_log_src, extensions=['log']) if len(f) in len_log_file]
    if len(logs) > 0:
        raise AssertionError(f"{len(logs)} log file{'s' if len(logs) > 1 else ''} should be processed before "
                             f"processing the transaction queue...")
    
    update_ts = int(time.time())
    q = pd.DataFrame(gp.load_data(queue_file))
    
    q['item_id'] = q['item_id'].apply(lambda r: r if not isinstance(r, Item) else r.item_id)
    n = len(q)
    q = q.sort_values(by=['timestamp', 'item_id', 'is_buy'], ascending=[True, True, False]).drop_duplicates()
    if n != len(q):
        print(f"{n - len(q)}/{n} duplicate entries have been removed from the queue")
    
    con = sqlite3.connect(gp.f_db_local)
    c = con.cursor()
    t_id = c.execute("""SELECT MAX(transaction_id) FROM 'transaction' """).fetchone()[0]
    sql_exe = "INSERT INTO 'transaction'(transaction_id, item_id, timestamp, is_buy, quantity, price, " \
              "status, tag, update_ts) VALUES(:transaction_id, :item_id, :timestamp, :is_buy, " \
              ":quantity, :price, :status, :tag, :update_ts)"
    
    q = q.to_dict('records')
    submitted, queue, queue_vars = [], [], list(q[0].keys())
    for t in q:
        if isinstance(min_ts, int) and t.get('timestamp') < min_ts:
            print(f'Skipped {t}')
            continue
        t_id += 1
        
        # TODO add method to convert parsed line to Transaction
        t = Transaction(transaction_id=get_next_transaction_id(),
                        item_id=t.get('item_id'),
                        timestamp=t.get('timestamp'),
                        is_buy=t.get('is_buy'),
                        quantity=t.get('quantity'),
                        price=t.get('price'),
                        status=1,
                        tag='e',
                        update_ts=update_ts)
        t.item_id, t.price, t.quantity = remap_item(item=create_item(item_id=t.item_id), price=t.price, quantity=t.quantity)
        t.transaction_id = t_id
        try:
            if isinstance(t.item_id, Item):
                t.item = create_item(t.item_id.item_id)
                t.item_id = t.item.item_id
            c.execute(sql_exe, t.__dict__)
            # t = l.submit_transaction(t.__dict__, commit_transaction=False, con=con, update_ts=update_ts)
            submitted.append(t)
        # TODO: think of and implement relevant exceptions
        except OSError:
            # Something went wrong; add the entry to queue.
            print(f'\t*** Failed to submit {t if isinstance(t, dict) else t.__dict__} ***')
            queue.append({k: t.__dict__.get(k) for k in queue_vars})
    
    # Done; commit sqlite database and overwrite the queue file.
    if submit_data:
        con.commit()
        con.close()
        uf.save(data=queue, path=queue_file)
        
        try:
            backup_db = time.time() - os.path.getmtime(uf.get_newest_file(gp.dir_backup_localdb)) > cfg.localdb_backup_cooldown
        except ValueError:
            backup_db = True
            
        if backup_db:
            shutil.copy2(gp.f_db_local, gp.dir_backup_localdb+f'localdb_{int(time.time())}.db')
            
            # Max backups exceeded -> Remove oldest backup
            while len(uf.get_files(gp.dir_backup_localdb)) > max(3, cfg.max_localdb_backups):
                print(f'Removing backup {uf.get_oldest_file(uf.get_files(gp.dir_backup_localdb))}...')
                os.remove(uf.get_oldest_file(uf.get_files(gp.dir_backup_localdb)))
        
    # Export submissions to a readable csv file
    try:
        if csv_file is not None:
            dat_file = csv_file.replace('.csv', '.dat')
            if os.path.exists(csv_file):
                df = pd.concat([pd.read_pickle(dat_file), pd.DataFrame([t.__dict__ for t in submitted])])
            else:
                df = pd.DataFrame([t.__dict__ for t in submitted])
            df['item_name'] = df.apply(lambda r: r.item_name if isinstance(r, Item) else go.id_name[r.get('item_id')], axis=1)
            df['datetime'] = df.apply(lambda r: ut.loc_unix_dt(r.get('timestamp')), axis=1)
            df['parse_time'] = df.apply(lambda r: ut.loc_unix_dt(r.get('update_ts')), axis=1)
            df = df.sort_values(by='transaction_id', ascending=True).drop_duplicates(
                subset=['timestamp', 'item_id', 'is_buy', 'price', 'quantity'])
            if len(df) > go.exchange_max_csv_transactions:
                df = df.iloc[-go.exchange_max_csv_transactions:]
            df.to_pickle(dat_file)
            df.to_csv(csv_file, index=False)
    except PermissionError:
        print(f'Failed to export the transactions to the csv file')
    finally:
        print(f"Added {len(submitted)}/{len(q)} new transactions to the transactions db")


def to_int(v):
    """ Function that can be passed as callable used for casting a variable to int """
    return int(v)


def to_float(v):
    """ Function that can be passed as callable used for casting a variable to float """
    return float(v)


def to_str(v):
    """ Function that can be passed as callable used for casting a variable to str """
    return str(v)


def to_list(v):
    """ Function that can be passed as callable used for casting an Iterable to a list """
    return list(v)


def to_npy_int(v):
    """ Function that can be passed as callable used for casting an Iterable to an integer numpy array """
    return np.array(v, dtype=int)


cast_to = {
    'int': to_int,
    'float': to_float,
    'string': to_str,
    'npy_int': to_npy_int,
    'list': to_list
}


class ExchangeLogLine(Transaction):
    def __init__(self, p_l: dict):
        """
        Generate a Transaction object based on a parsed line `p_l` from the log files. The ExchangeLogLine object has
        slightly more features that were extracted from the exchange log.

        Parameters
        ----------
        p_l : dict
            A parsed line from an exchange log file.

        Methods
        -------
        to_list()
            Method for converting the specified attributes of this object into specific datatypes and returning them as
            a specific list. Note that None is returned as -1 instead (values should be >=0 anyway)
        """
        super().__init__(transaction_id=get_next_transaction_id(),
                         item_id=p_l.get('item_id'), timestamp=p_l.get('timestamp'), is_buy=p_l.get('is_buy'),
                         quantity=p_l.get('quantity'), price=p_l.get('price'),
                         status=int(not p_l.get('state_id') % 3 == 0),
                         tag='e', update_ts=update_ts)
        self.state_id = p_l.get('state_id')
        self.slot_id = p_l.get('slot_id')
        self.value = p_l.get('value')
        self.max_quantity = p_l.get('max_quantity')
    
    def to_list(self, attribute_order: Iterable = go.exchange_log_archive_attribute_order,
                var_dtype: str = 'int', list_dtype: str = 'list'):
        """ Convert the specified list of attributes to the specified datatypes and return them """
        return cast_to.get(list_dtype)([cast_to.get(var_dtype)(self.__dict__.get(a))
                                        if self.__dict__.get(a) is not None else -1 for a in attribute_order])
    
    def get_key(self, attribute_order: Iterable = renamed_immutable_attributes):
        """ Return a tuple that can be used as an identifier for this transaction as the offer updates over time. """
        return tuple([int(self.__dict__.get(a)) for a in attribute_order])


def compare_results(df_1: pd.DataFrame, df_2: pd.DataFrame, out_dir: str = 'output/'):
    """
    Compare resulting dataframes from parsing submitted transactions, input dataframes should be a list of transactions
     as it is right before submitting it to the sqlite database. """
    overlap, differences = [], []
    
    for t in df_1.to_dict('records'):
        matching_rows = df_2.loc[(df_2['item_id'] == t.get('item_id')) & (df_2['price'] == t.get('price')) & \
                                 (df_2['quantity'] == t.get('quantity')) & (df_2['is_buy'] == t.get('is_buy'))]
        if len(matching_rows) > 0:
            overlap += matching_rows.to_dict('records')
        else:
            differences.append(t)
    pd.DataFrame(overlap).to_csv(out_dir + 'overlapping_rows.csv', index=False)
    pd.DataFrame(differences).to_csv(out_dir + 'differences_elog.csv', index=False)
    pd.DataFrame([t for t in df_2.to_dict('records') if t not in overlap]).to_csv(out_dir + 'differences_runelite.csv',
                                                                                  index=False)


def update_transaction_ids(t_id: int, to_db_file: str = gp.f_db_local.replace('.db', '_new.db')):
    """ Load all transactions with transaction_id > `t_id` and ensure that transaction_id is equal to the row index """
    
    # Safeguard in case of messing up
    shutil.copy(gp.f_db_local, to_db_file)
    con = sqlite3.connect(to_db_file)
    c = con.cursor()
    transactions = pd.read_sql(sql="SELECT * FROM 'transaction'", con=con)
    to_do = transactions.loc[transactions['transaction_id'] > t_id].sort_values(by='transaction_id', ascending=True)
    sql_exe = "UPDATE 'transaction' SET transaction_id = :new_id WHERE transaction_id = :transaction_id"
    for values_dict in to_do.to_dict('records'):
        c.execute(sql_exe, {'transaction_id': values_dict.get('transaction_id'), 'new_id': t_id})
        t_id += 1
    con.commit()
    con.close()
    print(f'db file {to_db_file} has been updated. Note that this is a temporary copy that should be renamed if the '
          f'updates are ok.')


def parse_transaction_thread_call():
    process_logs()
    submit_transaction_queue(submit_data=True)
    update_submitted_lines()
    
    
def parse_logs_background():
    if not process_logs(add_current=False):
        exit(1)
    submit_transaction_queue(submit_data=True)
    update_submitted_lines()
    
    
def parse_logs():
    if not process_logs(add_current=True):
        exit(1)
    submit_transaction_queue(submit_data=True)
    update_submitted_lines()
    


if __name__ == '__main__':
    # process_current_log(f_log, out_dir=dir_exchange_log)
    # submit_transaction_queue(submit_data=True)
    # exit(123)
    t = time.time()
    parse_logs_background()
    # submit_transaction_queue(submit_data=True)
    # update_submitted_lines()
    input(f"Execution complete! Time taken: {1000 * (time.time() - t):.0f}ms\nPress ENTER to close this screen")
