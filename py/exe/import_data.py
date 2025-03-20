"""
Executable module for data import + update pipeline.

When running this module, take extra care to avoid unexpected interruptions. Interrupting the script introduces a
serious risk of corrupting the database. This risk can be reduced by not interrupting the script and by creating
back-ups

"""

from venv_auto_loader.active_venv import *

__t0__ = time.perf_counter()

from tasks.util import finish_execution


def import_data(generate_arrays: bool = False, vacuum_threshold_mb: int = 3, vacuum_threshold_seconds: int = 90,
                multithreaded_npy_update: bool = True):
    """ Import data from the Raspberry Pi and subsequently update npy arrays + prices listbox """
    _time = time.perf_counter()
    # timeseries_transfer()
    
    from tasks.parse_transactions import parse_logs
    parse_logs(post_exe_print=False)
    
    from tasks.data_transfer import insert_items, timeseries_transfer_merged
    insert_items(__t0__)
    timeseries_transfer_merged(start_time=__t0__)
    
    # Generate + VACUUM the npy db and compute listbox entries for GUI
    if multithreaded_npy_update:
        from backend.npy_db_updater_threaded import UpdaterThreadManager
        UpdaterThreadManager(start_time=__t0__)
    else:
        from backend.npy_db_updater import NpyDbUpdater
        db = NpyDbUpdater(execute_update=True, add_arrays=generate_arrays)
        
    finish_execution()
    

if __name__ == '__main__':
    import_data(multithreaded_npy_update=True)
    print('')
    