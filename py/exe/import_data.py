"""
Executable module for data import + update pipeline.

When running this module, take extra care to avoid unexpected interruptions. Interrupting the script introduces a
serious risk of corrupting the database. This risk can be reduced by not interrupting the script and by creating
back-ups

"""
import sys
import os
import sqlite3
import time

from import_parent_folder import recursive_import
import global_variables.path as gp
import util.str_formats as fmt
from tasks.data_transfer import insert_items, timeseries_transfer_merged
from model.database import ROConn
del recursive_import

import_start = time.perf_counter()


def import_data(generate_arrays: bool = False, vacuum_threshold_mb: int = 3, vacuum_threshold_seconds: int = 90,
                multithreaded_npy_update: bool = True):
    """ Import data from the Raspberry Pi and subsequently update npy arrays + prices listbox """
    _time = time.perf_counter()
    # timeseries_transfer()
    insert_items()
    timeseries_transfer_merged()
    

    # Generate + VACUUM the npy db and compute listbox entries for GUI
    delta_size = gp.f_db_npy.fsize()
    delta_t = time.perf_counter()
    if multithreaded_npy_update:
        from backend.npy_db_updater_threaded import UpdaterThreadManager
        UpdaterThreadManager()
        delta_t = time.perf_counter()-delta_t
        delta_size = gp.f_db_npy.fsize() - delta_size
    else:
        from backend.npy_db_updater import NpyDbUpdater
        db = NpyDbUpdater(execute_update=True, add_arrays=generate_arrays)
        delta_t = time.perf_counter()-delta_t
        delta_size = db.fsize() - delta_size
    
    # 21-09-24 DISABLED post-update VACUUM since the database size is constrained by n_days and n_items
    # if delta_t > vacuum_threshold_seconds and delta_size < vacuum_threshold_mb * pow(10, 6):
    #     t_vacuum = time.perf_counter()
    #     print('VACUUMing db...', end='\r')
    #     sqlite3.connect(gp.f_db_npy).execute("VACUUM")
    #     print(f'Db was vacuumed in {fmt.delta_t(time.perf_counter()-t_vacuum)}', end='\n\n')
    print(f'Done! Total time taken: {fmt.delta_t(int(time.perf_counter()-import_start))} | This screen will close in 30s')
    # _ = input('  Press ENTER to close')
    print('')
    time.sleep(10)


if __name__ == '__main__':
    import_data(multithreaded_npy_update=True)
    print('')
    