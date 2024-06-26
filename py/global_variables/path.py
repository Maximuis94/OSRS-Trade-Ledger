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
import time
import os
import pickle
from typing import Dict

import setup.user_config as user_config

import pandas as pd

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
# f_db_local = dir_data + 'local_database.db'
f_db_local = dir_data + 'local.db'

# Sqlite database with external data
# f_db = dir_data + 'sqlite_database.db'
f_db_item = dir_data + 'local.db'
f_db_transaction = dir_data + 'local.db'
f_db_timeseries = dir_data + 'timeseries.db'
f_db_entity = dir_data + 'template.db'
f_db_npy = dir_data + 'npy.db'
f_npy_column = dir_data + 'npy_columns.dat'
f_npy_array_data = dir_data + 'npy_timeseries.db'
f_db_npy_augmented = dir_data+'npy_augmented.db'
f_prices_listbox = dir_data + 'prices_listbox.dat'
f_production_submissions = 'data/production_submissions.dat'
f_inventory_export = dir_data + 'inventory_export.dat'
f_whitelist = dir_resources + 'item_whitelist.dat'
f_whitelist_manual = dir_resources + 'item_whitelist_manual.xlsx'
f_exchange_log_transactions = dir_resources + 'runelite_exchange_log.dat'
f_exchange_log_merged = project_dir + "output/merged.log"
f_exchange_log_errors = dir_output + 'exchange_log_errors.txt'
f_exchange_log_parsed = project_dir + 'temp/parsed_log.dat'
f_exchange_log_np_export = dir_data + 'exchange_log.npy'
f_exchange_log_backup = dir_output + 'exchange_log_backup.log'
f_exchange_log_queue = dir_data + 'exchange_log/transaction_queue.dat'
f_submitted_lines_log = dir_data + 'exchange_log/submitted_lines.log'
f_scheduled_data_transfer_log = dir_output + 'data_transfer_log.txt'
f_stock_corrections = dir_data + 'stock_corrections.dat'
f_stock_count_form = dir_resources + 'stock_counts.csv'
f_transaction_log = dir_data + 'transaction_log.dat'
f_tracked_items_csv = dir_resources + 'tracked_items.csv'
f_tracked_items_list = dir_resources + 'tracked_items.dat'
f_tracked_items_listbox = dir_resources + 'tracked_items_listbox.dat'
f_tracker_values = dir_resources + 'tracker_values.dat'
f_item_list_csv = dir_output + 'item_list.csv'
f_outliers_csv = dir_output + 'outliers.csv'
f_dropped_items_csv = dir_output + 'dropped_items.csv'
f_df_itemdb = dir_resources + 'df_itemdb.dat'
f_df_avg5m = dir_resources + 'df_avg5m.dat'
f_df_rtdb = dir_resources + 'df_rtdb.dat'
f_df_wikidb = dir_resources + 'df_wikidb.dat'
f_update_prices_listbox_flag = project_dir + 'temp/update_listbox_entries.now'
f_user_config = dir_resources + 'config.txt'
f_exception_strings = dir_resources + 'exception_strings.csv'


# Paths defined below are used by LocalFiles/FlagFiles and should be accessed through these classes
local_file_rt_prices = dir_resources + 'realtime_prices.dat'
local_file_wiki_mapping = dir_resources + 'wiki_mapping.dat'

flag_transaction_parser = dir_temp + 'parsing_transactions.now'
flag_npy_updater = dir_temp + 'updating_arrays.now'
flag_importing_data = dir_temp + 'importing_data.now'
flag_interrupt_db_migration = dir_temp + 'interrupt_migration.now'
flag_db_transfer = dir_temp + 'transfer.now'

if not os.path.exists(f_user_config):
    if not user_config.generate_config_file(cfg_file=f_user_config):
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
    parsed_vars = user_config.parse_config_file(cfg_file=f_user_config, variable_parser=variable_parser)
    globals().update(parsed_vars)


dir_batch_archive = dir_archive + 'npy_batches/'
dir_test_data = dir_archive + 'test_data/'
dir_timeseries_backup = dir_data + 'backup_timeseries/'

dir_rbpi_dat = dir_rbpi + 'data/'
dir_rbpi_res = dir_rbpi + 'resources/'
dir_rbpi_temp = dir_rbpi + 'temp/'

f_db_archive = dir_archive + 'archive.db'
f_db_sandbox = dir_archive + 'test_database.db'

f_rbpi_transfer_flag = dir_rbpi + 'resources/transfer.now'
f_rbpi_db_avg5m = dir_rbpi + 'data/sql_avg5m.db'
f_rbpi_db_realtime = dir_rbpi + 'data/sql_realtime.db'
f_rbpi_db_wiki = dir_rbpi + 'data/sql_wiki.db'
f_rbpi_db_item = dir_rbpi + 'data/sql_item.db'
f_rbpi_rt = dir_rbpi + 'resources/realtime_prices.dat'
f_rbpi_merge_minutes = dir_rbpi + 'resources/rt_batch_merge_times.dat'
f_rbpi_transfer_log = dir_rbpi + 'resources/transfer_log.dat'
f_rbpi_rt_prices = dir_rbpi + 'temp/realtime.dat'

f_runelite_exchange_log = dir_exchange_log_src + 'exchange.log'
f_runelite_json_downloaded = dir_downloads + 'grand-exchange.json'




# f_db_scraped_src_missing = dir_resources + 'missing_prices.dat'

d = pd.DataFrame()

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
    print(parse_non_existent_files(dict(globals())))
