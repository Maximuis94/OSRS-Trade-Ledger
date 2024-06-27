"""
This module contains variables and methods related to accessing local files, both path/folder locations and methods for
interacting with them.

Variable prefixes indicate what the variable refers to;
Absolute file paths are named with prefix 'f_'
Absolute folder paths are named with prefix 'dir_'.
LocalFile objects are named with prefix 'dat_'

Folders outside the project's folder structure require to be specified by the user through a txt file.

# TODO: Remove unused paths
    Should be evaluated once the project is up and running
"""
import os
import pickle
import time
from typing import Dict

import setup.user_config as user_config
from file.file import File, _get_protocol

t_ = time.perf_counter()

setup_start = time.perf_counter()

# These are hard-coded folders outside of this project's folder structure that are to be specified by the user
dir_rbpi = ''
dir_exchange_log_src = ''
dir_archive = ''
dir_downloads = ''
exe_setup_template_db = ''


##########################################################
# Folders
##########################################################
# This is the full path to the project root, change the working directory to this folder.
project_dir = str(os.getcwd()).replace('\\', '/')
while project_dir.split('/')[-1] != 'py' and len(project_dir) > 0:
    project_dir = project_dir[:-len(project_dir.split('/')[-1])-1]
project_dir += '/'
print(f'Project root was set to {project_dir}')

# Below are full paths to folders used throughout the project
dir_root = project_dir
dir_data = project_dir + 'data/'
dir_exe = project_dir + 'executables/'
dir_output = project_dir + 'output/'
dir_logs = dir_output + 'log/'
dir_temp = dir_data + 'temp/'
dir_template = project_dir + 'template/'
dir_resources = dir_data + 'resources/'
dir_exchange_log = dir_data + 'exchange_log/'
dir_npy_arrays = dir_data + 'arrays/'
dir_np_archive_league_III = dir_data + 'np_archive_league_III/'
dir_plot_archive = dir_output + 'plots/'
dir_logger = dir_output + 'log/'

dir_batch = dir_data + 'batches/'
dir_batch_merged = dir_batch + 'merged/'
dir_backup = dir_data + 'backups/'
dir_backup_localdb = dir_backup + 'localDB/'
dir_ledger_backups = dir_backup + 'ledger/'

##########################################################
# Files
##########################################################
# Below are full paths to files used throughout the project
# f_db_local = dir_data + 'local_database.db
f_db_local: File = dir_data + 'local.db'

# Sqlite database with external data
f_db_timeseries: File = dir_data + 'timeseries.db'
f_db_item = dir_data + 'local.db'
f_db_transaction = dir_data + 'local.db'
f_db_entity: File = dir_data + 'template.db'
f_db_npy: File = dir_data + 'npy.db'
f_npy_column: File = dir_data + 'npy_columns.dat'
f_prices_listbox: File = dir_data + 'prices_listbox.dat'
f_production_submissions: File = dir_data + 'production_submissions.dat'
f_exchange_log_queue: File = dir_data + 'exchange_log/transaction_queue.dat'
f_submitted_lines_log: File = dir_data + 'exchange_log/submitted_lines.log'
f_stock_corrections: File = dir_data + 'stock_corrections.dat'
f_tracked_items_csv: File = dir_resources + 'tracked_items.csv'
f_tracked_items_listbox: File = dir_resources + 'tracked_items_listbox.dat'
f_user_config: File = dir_resources + 'config.txt'
f_exception_strings: File = dir_resources + 'exception_strings.csv'

# f_db = dir_data + 'sqlite_database.db
f_db_npy_array_data = dir_data + 'npy_timeseries.db'
f_db_npy_augmented = dir_data + 'npy_augmented.db'
# f_inventory_export: File = dir_data + 'inventory_export.dat'
# f_whitelist: File = dir_resources + 'item_whitelist.dat'
# f_whitelist_manual = dir_resources + 'item_whitelist_manual.xlsx'
# f_exchange_log_transactions: File = dir_resources + 'runelite_exchange_log.dat'
# f_exchange_log_merged: File = project_dir + 'output/merged.log'
# f_exchange_log_errors: File = dir_output + 'exchange_log_errors.txt'
# f_exchange_log_parsed: File = project_dir + 'temp/parsed_log.dat'
# f_exchange_log_np_export: File = dir_data + 'exchange_log.npy'
# f_exchange_log_backup: File = dir_output + 'exchange_log_backup.log'
# f_scheduled_data_transfer_log: File = dir_output + 'data_transfer_log.txt'
# f_stock_count_form: File = dir_resources + 'stock_counts.csv'
# f_transaction_log: File = dir_data + 'transaction_log.dat'
# f_tracked_items_list: File = dir_resources + 'tracked_items.dat'
# f_tracker_values: File = dir_resources + 'tracker_values.dat'
# f_item_list_csv: File = dir_output + 'item_list.csv'
# f_outliers_csv: File = dir_output + 'outliers.csv'
# f_dropped_items_csv: File = dir_output + 'dropped_items.csv'
# f_df_itemdb: File = dir_resources + 'df_itemdb.dat'
# f_df_avg5m: File = dir_resources + 'df_avg5m.dat'
# f_df_rtdb: File = dir_resources + 'df_rtdb.dat'
# f_df_wikidb: File = dir_resources + 'df_wikidb.dat'
# f_update_prices_listbox_flag = project_dir + 'temp/update_listbox_entries.now'


# Paths defined below are used by LocalFiles/FlagFiles and should be accessed through these classes
local_file_rt_prices: File = dir_resources + 'realtime_prices.dat'
local_file_wiki_mapping: File = dir_resources + 'wiki_mapping.dat'

flag_transaction_parser: File = dir_temp + 'parsing_transactions.now'
flag_npy_updater: File = dir_temp + 'updating_arrays.now'
flag_importing_data: File = dir_temp + 'importing_data.now'
flag_interrupt_db_migration: File = dir_temp + 'interrupt_migration.now'
flag_db_transfer: File = dir_temp + 'transfer.now'

if not os.path.exists(f_user_config):
    if not user_config.generate_config_file(f_user_config):
        print(input(f'Please configure the config.txt file at {f_user_config} before running the script again...\n'
                    f'Press ENTER to close this screen '))
        exit(-1)
    else:
        print('Config file was modified! Verifying its contents...')
        

if os.path.exists(f_user_config):
    if not user_config.verify_config_file(f_user_config):
        _ = input('Press ENTER to close this screen')
        raise FileNotFoundError(f'Reconfigure {f_user_config} and rerun the script')
    
    def variable_parser(name: str, value: str) -> bool:
        """ Return True if `name` has prefix 'dir_' and `value` is an existing file/folder """
        if not name.split('_')[0] in ('f', 'dir', 'exe'):
            return False
        
        # A variable that is needed has an illegal value...
        if len(value) == 0 or not (os.path.exists(value) or value[:2] == '//'):
            raise ValueError(f"Illegal value '{value}' assigned to variable '{name}' in the config file...")
        return True
    globals().update(user_config.parse_config_file(f_user_config, variable_parser=variable_parser))


dir_batch_archive = dir_archive + 'npy_batches/'
dir_test_data = dir_archive + 'test_data/'
dir_timeseries_backup = dir_data + 'backup_timeseries/'

dir_rbpi_dat = dir_rbpi + 'data/'
# dir_rbpi_res = dir_rbpi + 'resources/'
# dir_rbpi_temp = dir_rbpi + 'temp/'

# f_rbpi_transfer_flag = dir_rbpi + 'resources/transfer.now
f_rbpi_rt: File = dir_rbpi + 'resources/realtime_prices.dat'
f_rbpi_merge_minutes: File = dir_rbpi + 'resources/rt_batch_merge_times.dat'
f_rbpi_transfer_log: File = dir_rbpi + 'resources/transfer_log.dat'
f_rbpi_rt_prices: File = dir_rbpi + 'temp/realtime.dat'

f_runelite_exchange_log: File = dir_exchange_log_src + 'exchange.log'
f_runelite_json_downloaded: File = dir_downloads + 'grand-exchange.json'

f_db_archive: File = dir_archive + 'archive.db'
f_db_sandbox: File = dir_archive + 'test_database.db'
f_db_rbpi_avg5m: File = dir_rbpi + 'data/sql_avg5m.db'
f_db_rbpi_realtime: File = dir_rbpi + 'data/sql_realtime.db'
f_db_rbpi_wiki: File = dir_rbpi + 'data/sql_wiki.db'
f_db_rbpi_item: File = dir_rbpi + 'data/sql_item.db'

# Instantiate all vars with prefix f_ as File
for k, v in dict(locals()).items():
    if isinstance(v, str) and k[:2] == 'f_':
        try:
            _get_protocol(path=v)
        except ValueError:
            # print(f'Excluded {k}={v}')
            continue

        locals()[k] = File(v)

# f_db_scraped_src_missing = dir_resources + 'missing_prices.dat'

def load_data(path: str):
    """ Load data from the specified path """
    return pickle.load(open(path, 'rb'))


def get_files(src: str, add_src: bool = False, extensions: list = None) -> list:
    """ Return file names found in dir `src`. Return files with extensions specified and full paths if `add_src` """
    prefix = src if add_src else ''
    return [prefix+f for f in os.listdir(src) if extensions is None or f.split('.')[-1] in extensions]


def parse_non_existent_files(file_dict: Dict[str, str]):
    result = []
    for var_name, file_name in file_dict.items():
        if var_name[:2] == 'f_' and not os.path.exists(file_name):
            print(var_name, file_name)
            result.append(var_name)
    print('\n\n')
    return result


if __name__ == "__main__":
    # print(parse_non_existent_files(dict(globals())))
    ...
    print("C:/Users/Max Moons/Documents/GitHub/OSRS-Trade-Ledger/py/data/npy.npy" == f_db_npy.replace('.db', '.npy'))
