"""
This module is filled with constant configuration values.

Values listed in this module are to be used as default parameter values in method signatures.

"""
from collections import namedtuple

from venv_auto_loader.active_venv import *
import global_variables.path as gp
__t0__ = time.perf_counter()

# If True
debug = True


# If debug is True, it will print something, else it will pass
dbg_prt = lambda msg: print(msg) if debug else lambda msg: ...


# Timespan in seconds of one rbpi batch
rbpi_batch_timespan = 14400

# Frequency in seconds for how often the npy items list should be updated
npy_list_update_frequency = 86400 * 7

# Coverage of npy database in days
# t0 of NpyArray is equal to unix_time - npy_array_timespan_days*86400, rounded down to 12 am utc
# npy_db_timespan_days = 120
npy_db_timespan_days = 456

# Timestamp cut-off values for npy db
npy_round_t0 = 86400
npy_round_t1 = 3600

# Amount of days coverage the prices listbox should have
prices_listbox_days = 56

# Timespan of each listbox column in seconds
listbox_column_timespan = 14400

# If the number of localdb backups exceeds this amount, remove the oldest backup. Db is relatively small
max_localdb_backups = 10

# Minimum time between 2 localdb backups (s). Prevents all existing backups from being made in a very short timeframe.
localdb_backup_cooldown = 10800

# Filter very small transactions and price probes
db_quantity_threshold = 3
db_value_threshold = 50000

# Max amount of lines in the backup file; delete older lines if this value is exceeded
max_exchange_log_backup_size = 10000


###########################################################################
# Item-related configurations
###########################################################################


# Amount of wiki volumes used for computing daily volume average.
n_days_volume_avg = 7

timespan_target_prices_eval = 3




###########################################################################
# Graph-related configurations
###########################################################################


# Timeseries plots fixed intervals in seconds; distance in seconds between vertical grey lines
vplot_intervals = (300, 900, 3600, 14400, 43200, 86400, 259200, 604800, 1209600, 2419200, 7257600)


dow_colors = [
    (200, 140, 230-30*dow_id) for dow_id in range(7)
]

# TODO add default order of colors to use for graphs
default_graph_colors = [
    (),
]


###########################################################################
# Local file configurations
###########################################################################
rt_update_frequency = 180
rt_rbpi_update_frequency = 90
entity_db_update_frequency = 86400


###########################################################################
# Database updater / migration configurations
###########################################################################


RowTransfer = namedtuple('RowTransfer', ['src_db', 'src_t', 'dst_db', 'dst_t', 'convert'])

# This is a namedtuple that is designed for migrating rows within a database, e.g. to redesign a table.
tt_args = ['db', 'table', 'create', 'sql', 'n_iter', 'indices', 'factory']
TableTransform = namedtuple('TableTransform', tt_args)


_src_ts, _src_local = gp.f_db_timeseries, gp.f_db_transaction
_dst = gp.f_db_archive

# Transfer from active db to archive db
archive_transfer = {
    'item': RowTransfer(src_db=_src_ts, src_t='itemdb', dst_db=_dst, dst_t='item', convert=None),
    'transaction': RowTransfer(src_db=_src_local, src_t='transactions',
                               dst_db=_dst, dst_t='transaction', convert=None),
    'avg5m': RowTransfer(src_db=_src_ts, src_t='avg5m', dst_db=_dst, dst_t='avg5m', convert=None),
    'realtime': RowTransfer(src_db=_src_ts, src_t='realtime',
                            dst_db=_dst, dst_t='realtime', convert=None),
    'wiki': RowTransfer(src_db=_src_ts, src_t='wiki', dst_db=_dst, dst_t='wiki', convert=None),
}
archive_transfer_local = ['item', 'transaction']
archive_transfer_timeseries = ['avg5m', 'realtime', 'wiki']


###########################################################################
# NpyArray updater configurations
###########################################################################
# For updater / row migration, frequency (s) for which an updated should be printed/commit should be made;
data_transfer_print_frequency = 5
data_transfer_commit_frequency = 180

# Cut-off value of timestamp upper bound in seconds (-> t1 = time.time()-int(time.time())%timestamp_cutoff)
np_ar_cfg_timestamp_cutoff = 3600 * 4

# When searching for items to include in NpyArray updates, daily trade value should be at least this
np_ar_cfg_min_daily_value = 50000000

# Update the NpyArray item using the frequency (seconds) below
np_ar_cfg_update_frequency_item_list = 86400 * 7

# [OVERRIDE] List of item_ids and/or item_names to forcibly exclude
np_ar_cfg_exclude_items = []

# [OVERRIDE] List of item_ids and/or item_names to forcibly include
np_ar_cfg_include_items = []

# npy db indexes
npy_db_index = {
    'avg5m': [('avg5m_ts_itemid', ['timestamp', 'item_id'])],
    'realtime': [('realtime_ts_itemid', ['timestamp', 'item_id']),
                 ('realtime_ts_itemid_buy', ['timestamp', 'item_id', 'is_buy'])],
    'wiki': [('wiki_ts_itemid', ['timestamp', 'item_id'])],
    'transaction': [('transaction_item_id', 'item_id'), ('transaction_ts_item_id', ['timestamp', 'item_id'])]
}


def _debug_print(msg: str):
    print(msg)


# def np_ar_include_lifetime_profit(profit: int, min_lifetime_profit: int = 5 * pow(10, 7), **kwargs) -> bool:
#     """ Include an item in array updates if its total profits exceed `min_lifetime_profit` """
#     return profit >= min_lifetime_profit

if __name__ == "__main__":
    dbg_prt('hoi')
    ...