"""
Executable module for data import + update pipeline

"""
import time

import util.str_formats as fmt
from backend.npy_db_updater import NpyDbUpdater
from tasks.data_transfer import insert_items, timeseries_transfer


def import_data(generate_arrays: bool = False):
    """ Import data from the Raspberry Pi and subsequently update npy arrays """
    _time = time.perf_counter()
    timeseries_transfer()
    insert_items()

    # Generate + VACUUM the npy db and compute listbox entries for GUI
    db = NpyDbUpdater(execute_update=True, add_arrays=generate_arrays)
    
    print('VACUUMing db...')
    db.execute("VACUUM")
    
    print(f'\n\nDone! Total time taken: {fmt.delta_t(int(time.perf_counter()-_time))}')
    _ = input('  Press ENTER to close')
    print('')


if __name__ == '__main__':
    import_data()
    