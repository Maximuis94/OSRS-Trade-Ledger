"""


"""
import sqlite3
import time

import global_variables.path as gp
import util.str_formats as fmt
from tasks.backup_timeseries import create_backup
from util.sql import vacuum_into


if __name__ == '__main__':
    backup=False
    print(f'[{fmt.unix_(time.time())}] VACUUMING DATABASE...')
    start_time = time.perf_counter()
    vacuum_into(gp.f_db_timeseries, gp.dir_timeseries_backup + 'timeseries.db')
    print(f'\tDone in {fmt.delta_t(time.perf_counter()-start_time)}')
    if backup:
        create_backup(backup_directory=gp.dir_data + 'backup_test/')
    print('Done!')
    _ = input('')
    exit(1)
    
    
    