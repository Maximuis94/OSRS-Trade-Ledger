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
import sys
import time

from venv_auto_loader.active_venv import *
from file.file import File, Root
from file.local_file import parse_roots_config, roots_config_error
from setup.user_config import setup_roots_json

__t0__ = time.perf_counter()

t_ = time.perf_counter()

setup_start = time.perf_counter()
dir_root = os.path.commonpath([sys.prefix, __file__]).replace('\\', '/') + '/'
# dir_root = os.path.commonpath([os.getcwd(), __file__]).replace('\\', '/') + '/'

_cfg = None
try:
    _cfg = parse_roots_config(verbose=True)
    dir_root = _cfg['pc_dir_root']
    dir_rbpi = _cfg['dir_rbpi']
    dir_exchange_log_src = _cfg['dir_exchange_log_src']
    dir_archive = _cfg['dir_archive']
    dir_downloads = _cfg['dir_downloads']
except KeyError as e:
    roots_config_error(e, _cfg)
    raise e
del _cfg

# These are hard-coded folders outside of this project's folder structure that are to be specified by the user
exe_setup_template_db = None


##########################################################
# Folders
##########################################################
# This is the full path to the project root, change the working directory to this folder.
print(f'Project root was set to {dir_root}')

# Below are full paths to folders used throughout the project
dir_data = dir_root + 'data/'
dir_exe = dir_root + 'executables/'
dir_output = dir_root + 'output/'
dir_logs = dir_output + 'log/'
dir_temp = dir_data + 'temp/'
dir_template = dir_root + 'template/'
dir_resources = dir_data + 'resources/'
dir_exchange_log = dir_data + 'exchange_log/'
dir_npy_arrays = dir_data + 'arrays/'
dir_np_archive_league_III = dir_data + 'np_archive_league_III/'
dir_plot_archive = dir_output + 'plots/'
dir_compare_npy_analysis = dir_output + 'compare_npy_analysis/'
dir_db_verification = dir_output + 'db_verification/'
dir_logger = dir_output + 'log/'
dir_npy_import = dir_data + 'npy_imports/'
dir_item_production = dir_data + 'item_production/'
dir_bank_memory = dir_data + 'bank_memory/'

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
f_db_local: File = File(dir_data + 'local.db')

# Sqlite database with external data
f_db_timeseries: File = File(dir_data + 'timeseries.db')
f_db_item: File = File(dir_data + 'local.db')
f_db_transaction: File = File(dir_data + 'local.db')
f_db_entity: File = File(dir_data + 'template.db')
f_db_npy: File = File(dir_data + 'npy.db')
f_npy_column: File = File(dir_data + 'npy_columns.dat')
f_prices_listbox: File = File(dir_data + 'prices_listbox.dat')
f_production_submissions: File = File(dir_data + 'production_submissions.dat')
f_exchange_log_queue: File = File(dir_data + 'exchange_log/transaction_queue.dat')
f_submitted_lines_log: File = File(dir_data + 'exchange_log/submitted_lines.log')
f_stock_corrections: File = File(dir_data + 'stock_corrections.dat')
f_tracked_items_csv: File = File(dir_resources + 'tracked_items.csv')
f_tracked_items_listbox: File = File(dir_resources + 'tracked_items_listbox.dat')
f_user_config: File = File(dir_resources + 'config.txt')
f_exception_strings: File = File(dir_resources + 'exception_strings.csv')
f_small_batch_log: File = File(dir_resources + 'small_batch_log.dat')
f_dup_transactions: File = File(dir_data+'duplicate_transactions.txt')

# f_db = dir_data + 'sqlite_database.db
f_db_npy_array_data: File = File(dir_data + 'npy_timeseries.db')
f_db_npy_augmented: File = File(dir_data + 'npy_augmented.db')
# f_inventory_export: File = File(dir_data + 'inventory_export.dat'
# f_whitelist: File = File(dir_resources + 'item_whitelist.dat'
# f_whitelist_manual = dir_resources + 'item_whitelist_manual.xlsx'
# f_exchange_log_transactions: File = File(dir_resources + 'runelite_exchange_log.dat'
# f_exchange_log_merged: File = project_dir + 'output/merged.log'
# f_exchange_log_errors: File = File(dir_output + 'exchange_log_errors.txt'
# f_exchange_log_parsed: File = project_dir + 'temp/parsed_log.dat'
# f_exchange_log_np_export: File = File(dir_data + 'exchange_log.npy'
# f_exchange_log_backup: File = File(dir_output + 'exchange_log_backup.log'
# f_scheduled_data_transfer_log: File = File(dir_output + 'data_transfer_log.txt'
# f_stock_count_form: File = File(dir_resources + 'stock_counts.csv'
# f_transaction_log: File = File(dir_data + 'transaction_log.dat'
# f_tracked_items_list: File = File(dir_resources + 'tracked_items.dat'
# f_tracker_values: File = File(dir_resources + 'tracker_values.dat'
# f_item_list_csv: File = File(dir_output + 'item_list.csv'
# f_outliers_csv: File = File(dir_output + 'outliers.csv'
# f_dropped_items_csv: File = File(dir_output + 'dropped_items.csv'
# f_df_itemdb: File = File(dir_resources + 'df_itemdb.dat'
# f_df_avg5m: File = File(dir_resources + 'df_avg5m.dat'
# f_df_rtdb: File = File(dir_resources + 'df_rtdb.dat'
# f_df_wikidb: File = File(dir_resources + 'df_wikidb.dat'
# f_update_prices_listbox_flag = project_dir + 'temp/update_listbox_entries.now'


# Paths defined below are used by LocalFiles/FlagFiles and should be accessed through these classes
local_file_rt_prices: File = File(dir_resources + 'realtime_prices.dat')
local_file_wiki_mapping: File = File(dir_resources + 'wiki_mapping.dat')

flag_transaction_parser: File = File(dir_temp + 'parsing_transactions.now')
flag_npy_updater: File = File(dir_temp + 'updating_arrays.now')
flag_importing_data: File = File(dir_temp + 'importing_data.now')
flag_interrupt_db_migration: File = File(dir_temp + 'interrupt_migration.now')
flag_db_transfer: File = File(dir_temp + 'transfer.now')

# if not os.path.exists(f_user_config):
#     if not user_config.generate_config_file(f_user_config):
#         print(input(f'Please configure the config.txt file at {f_user_config} before running the script again...\n'
#                     f'Press ENTER to close this screen '))
#         exit(-1)
#     else:
#         print('Config file was modified! Verifying its contents...')
#         
# 
# if os.path.exists(f_user_config):
#     if not user_config.verify_config_file(f_user_config):
#         _ = input('Press ENTER to close this screen')
#         raise FileNotFoundError(f'Reconfigure {f_user_config} and rerun the script')
#     
#     def variable_parser(name: str, value: str) -> bool:
#         """ Return True if `name` has prefix 'dir_' and `value` is an existing file/folder """
#         if not name.split('_')[0] in ('f', 'dir', 'exe'):
#             return False
#         
#         # A variable that is needed has an illegal value...
#         if len(value) == 0 or not (os.path.exists(value) or value[:2] == '//'):
#             raise ValueError(f"Illegal value '{value}' assigned to variable '{name}' in the config file...")
#         return True
#     globals().update(user_config.parse_config_file(f_user_config, variable_parser=variable_parser))


dir_batch_archive = dir_archive + 'npy_batches/'
dir_df_archive = dir_archive + 'timeseries_dataframe_archive/'
dir_test_data = dir_archive + 'test_data/'
dir_timeseries_backup = dir_data + 'backup_timeseries/'

dir_rbpi_dat = dir_rbpi + 'data/'
dir_rbpi_exports = dir_rbpi + 'exports/'
# dir_rbpi_res = dir_rbpi + 'resources/'
# dir_rbpi_temp = dir_rbpi + 'temp/'

# f_rbpi_transfer_flag = dir_rbpi + 'resources/transfer.now)
f_rbpi_rt: File = File(dir_rbpi + 'resources/realtime_prices.dat')
f_rbpi_merge_minutes: File = File(dir_rbpi + 'resources/rt_batch_merge_times.dat')
f_rbpi_transfer_log: File = File(dir_rbpi + 'resources/transfer_log.dat')
f_rbpi_rt_prices: File = File(dir_rbpi + 'temp/realtime.dat')

f_runelite_exchange_log: File = File(dir_exchange_log_src + 'exchange.log')
f_runelite_json_downloaded: File = File(dir_downloads + 'grand-exchange.json')

f_db_archive: File = File(dir_archive + 'archive.db')
f_db_sandbox: File = File(dir_archive + 'test_database.db')
f_db_rbpi_avg5m: File = File(dir_rbpi + 'data/sql_avg5m.db')
f_db_rbpi_realtime: File = File(dir_rbpi + 'data/sql_realtime.db')
f_db_rbpi_wiki: File = File(dir_rbpi + 'data/sql_wiki.db')
f_db_rbpi_item: File = File(dir_rbpi + 'data/item.db')


def get_files(src: str, add_src: bool = False, extensions: list = None) -> list:
    """ Return file names found in dir `src`. Return files with extensions specified and full paths if `add_src` """
    prefix = src if add_src else ''
    return [prefix+f for f in os.listdir(src) if extensions is None or f.split('.')[-1] in extensions]


if __name__ == "__main__":
    
    # print(parse_non_existent_files(dict(globals())))
    # setup_roots_json(**{root.var: root for root in (
    #     Root(key='pc_rbpi_root', path=None, var='dir_rbpi'),
    #     Root(key='pc_archive_root', path=None, var='dir_archive'),
    #     Root(key='pc_downloads_root', path=None, var='dir_downloads'),
    #     Root(key='pc_exchange_log_root', path=None, var='dir_exchange_log_src'))})
    ...
