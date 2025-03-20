"""
Module for setting up roots.json file, a file with several paths that are frequently used throughout the project.
"""

import os.path
import pandas as pd

from venv_auto_loader.active_venv import *
from file.file import Root
from global_variables._initialize_path import generate_roots_config
__t0__ = time.perf_counter()

t_setup_start = time.perf_counter()

# This is a dict with variables that will be listed in the config file by default.
config_variables = {
    'dir_rbpi': '# Path to the project root of the scraper on the raspberry pi',
    'dir_exchange_log_src': '# Output folder of the runelite exchange logger',
    'dir_archive': '# Folder in which files will be archived for long-term storage',
    'dir_downloads': '# Folder in which downloaded files are placed by default',
    'exe_template_db': '# Path to file that can be used to generate template db'
}


def setup_exception_resource_file(path: str):
    """ Generate a resource file for user-defined hard-coded exception strings """
    if os.path.exists(path):
        raise FileExistsError(f"Exception resource file already exists at {path}")
    print(f'Generating exceptions string resource file at {path}')
    dtypes = {'msg_idx': 'int64', 'exception_class': 'string', 'message': 'string'}
    pd.DataFrame(columns=list(dtypes.keys())).astype(dtype=dtypes).to_csv(path, index_label='msg_idx')


def setup_roots_json(dir_rbpi: Root, dir_archive: Root, dir_downloads: Root,
                     dir_exchange_log_src: Root, *roots):
    """ Call this method to create the roots.json file, which can be used to determine several roots """
    roots = (dir_rbpi, dir_archive, dir_downloads, dir_exchange_log_src, *roots)
    generate_roots_config(*roots, output_file='data/resources/roots.json')
