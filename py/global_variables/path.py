"""
This module contains variables and methods related to accessing local files,
both path/folder locations and methods for interacting with them.

Variable prefixes indicate what the variable refers to;
Absolute file paths are named with prefix 'f_'
Absolute folder paths are named with prefix 'dir_'.
LocalFile objects are named with prefix 'dat_'

Folders outside the project's folder structure require to be specified by the user through a txt file.

# TODO: Remove unused paths
    Should be evaluated once the project is up and running
"""

from venv_auto_loader.active_venv import *
from file.file import File
from global_variables._initialize_path import parse_roots_config, roots_config_error

__t0__ = time.perf_counter()
t_ = time.perf_counter()
setup_start = time.perf_counter()

##########################################################
# Base configuration & roots (from config file)
##########################################################
# Determine project root based on current environment
dir_root = os.path.commonpath([sys.prefix, __file__]).replace('\\', '/') + '/'

_cfg = None
try:
    _cfg = parse_roots_config(verbose=True)
    # print(_cfg)
    # Override some directories from the config file
    dir_root = _cfg['pc_dir_root']
    dir_rbpi = _cfg.get('dir_rbpi', "NON-EXISTENT_DIRECTORY")
    dir_runelite_root = _cfg['dir_runelite_src']
    dir_runelite_profile_src = os.path.join(dir_runelite_root, 'profiles2/')
    dir_exchange_log_src = os.path.join(dir_runelite_root, 'exchange-logger/')
    dir_flipping_utilities_src = os.path.join(dir_runelite_root, 'flipping/')
    dir_archive = _cfg['dir_archive']
    dir_downloads = _cfg['dir_downloads']
    dir_databases = _cfg['dir_databases']
except KeyError as e:
    print(_cfg)
    roots_config_error(e, _cfg)
    raise e
del _cfg

# These are hard-coded folders outside of this project's folder structure
exe_setup_template_db = None

##########################################################
# Core Directories (project structure)
##########################################################
# Base directories for the project
print(f'Project root was set to {dir_root}')
dir_data = dir_root + 'data/'
dir_exe = dir_root + 'executables/'
dir_output = dir_root + 'output/'
dir_template = dir_root + 'template/'

##########################################################
# Data Directories (all under dir_data)
##########################################################
# Temporary files and resources
dir_temp = dir_data + 'temp/'
dir_resources = dir_data + 'resources/'

# Exchange logs and related archives
dir_exchange_log = dir_data + 'exchange_log/'
dir_exchange_log_archive = dir_exchange_log + 'archive/'

# Array and numpy related directories
dir_npy_arrays = dir_data + 'arrays/'
dir_np_archive_league_III = dir_data + 'np_archive_league_III/'
dir_npy_import = dir_data + 'npy_imports/'

# Other data-related directories
dir_item_production = dir_resources + 'production_rules/'
dir_bank_memory = dir_data + 'bank_memory/'
dir_runelite_ge_export = dir_data + 'runelite_ge_export/'
dir_runelite_ge_export_raw = dir_runelite_ge_export + 'raw/'
dir_export_parser_temp = dir_data + 'export_parser_temp/'
dir_flipping_utilities = dir_data + 'flipping_utilities/'
dir_flipping_utilities_raw = dir_flipping_utilities + 'raw/'
dir_runelite_profile = dir_data + 'runelite_profile_data/'
dir_runelite_profile_raw = dir_runelite_profile + 'raw/'

# Batch and backup related directories
dir_batch = dir_data + 'batches/'
dir_batch_merged = dir_batch + 'merged/'
dir_backup = dir_data + 'backups/'
dir_backup_localdb = dir_backup + 'localDB/'
dir_ledger_backups = dir_backup + 'ledger/'
dir_timeseries_backup = dir_data + 'backup_timeseries/'

##########################################################
# Output Directories (all under dir_output)
##########################################################
dir_logs = dir_output + 'log/'
dir_plot_archive = dir_output + 'plots/'
dir_compare_npy_analysis = dir_output + 'compare_npy_analysis/'
dir_db_verification = dir_output + 'db_verification/'

##########################################################
# Archive Directories (external archive paths)
##########################################################
# Note: dir_archive comes from the configuration and is external to the project root.
dir_batch_archive = dir_archive + 'npy_batches/'
dir_df_archive = dir_archive + 'timeseries_dataframe_archive/'
dir_test_data = dir_archive + 'test_data/'

##########################################################
# Rbpi Directories (from configuration)
##########################################################
# rbpi has its own substructure; here we keep data and exports together
dir_rbpi_dat = dir_rbpi + 'data/'
dir_rbpi_exports = dir_rbpi + 'exports/'
# (Other rbpi directories such as resources or temp could be added similarly)

##########################################################
# Files (paths remain unchanged)
##########################################################
f_db_local: File = File(dir_data + 'local.db')
f_db_timeseries: File = File(dir_databases + 'timeseries.db')
f_db_item: File = File(dir_data + 'local.db')
f_db_transaction: File = File(dir_data + 'local.db')
f_db_transaction_new: File = File(dir_data + "transaction_database.db")
f_db_entity: File = File(dir_data + 'template.db')
f_db_npy: File = File(dir_databases + 'npy.db')
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
f_dup_transactions: File = File(dir_data + 'duplicate_transactions.txt')

f_db_npy_array_data: File = File(dir_data + 'npy_timeseries.db')
f_db_npy_augmented: File = File(dir_data + 'npy_augmented.db')

# Paths for LocalFiles/FlagFiles
local_file_rt_prices: File = File(dir_resources + 'realtime_prices.dat')
local_file_wiki_mapping: File = File(dir_resources + 'wiki_mapping.dat')

flag_transaction_parser: File = File(dir_temp + 'parsing_transactions.now')
flag_npy_updater: File = File(dir_temp + 'updating_arrays.now')
flag_importing_data: File = File(dir_temp + 'importing_data.now')
flag_interrupt_db_migration: File = File(dir_temp + 'interrupt_migration.now')
flag_db_transfer: File = File(dir_temp + 'transfer.now')

# dir_batch_archive = dir_archive + 'npy_batches/'
# dir_df_archive = dir_archive + 'timeseries_dataframe_archive/'
# dir_test_data = dir_archive + 'test_data/'
# dir_timeseries_backup = dir_data + 'backup_timeseries/'

# dir_rbpi_dat = dir_rbpi + 'data/'
# dir_rbpi_exports = dir_rbpi + 'exports/'

f_rbpi_rt: File = File(dir_rbpi + 'resources/realtime_prices.dat')
f_rbpi_merge_minutes: File = File(dir_rbpi + 'resources/rt_batch_merge_times.dat')
f_rbpi_transfer_log: File = File(dir_rbpi + 'resources/transfer_log.dat')
f_rbpi_rt_prices: File = File(dir_rbpi + 'temp/realtime.dat')

f_runelite_profile_properties: File = File(dir_runelite_profile_src + '$rsprofile--1.properties')
f_runelite_exchange_log: File = File(dir_exchange_log_src + 'exchange.log')
f_runelite_json_downloaded: File = File(dir_downloads + 'grand-exchange.json')
f_runelite_ge_export_df_merged: File = File(dir_data + 'runelite-ge-export-dataframe.dat')
f_runelite_ge_export_df_merged_csv: File = File(dir_output + 'runelite-ge-export-dataframe.csv')

f_db_archive: File = File(dir_archive + 'archive.db')
f_db_sandbox: File = File(dir_archive + 'test_database.db')
f_db_rbpi_avg5m: File = File(dir_rbpi + 'data/sql_avg5m.db')
f_db_rbpi_realtime: File = File(dir_rbpi + 'data/sql_realtime.db')
f_db_rbpi_wiki: File = File(dir_rbpi + 'data/sql_wiki.db')
f_db_rbpi_item: File = File(dir_rbpi + 'data/item.db')

##########################################################
# Utility Functions
##########################################################
def get_files(src: str, add_src: bool = False, extensions: list = None) -> list:
    """Return file names found in dir `src` with specified extensions and full paths if `add_src` is True."""
    prefix = src if add_src else ''
    return [prefix + f for f in os.listdir(src) if extensions is None or f.split('.')[-1] in extensions]


##########################################################
# Debug / Verification
##########################################################
# Print out directories (those starting with "dir_") that do not exist yet.
if __debug__:
    _locals = dict(locals())
    non_existing_dirs = {}
    for tag, _dir in _locals.items():
        if tag.startswith("dir_"):
            if not os.path.exists(_dir):
                print(tag, _dir)
                non_existing_dirs[tag] = _dir
    print(f"A total of {len(non_existing_dirs)} directories do not exist")
