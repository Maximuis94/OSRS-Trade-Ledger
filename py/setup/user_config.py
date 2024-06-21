"""
This module contains methods for generating files that allow the user to override project settings without explicitly
coding these settings.

For certain configurations that do not have a default value this is mandatory, like certain hard-coded folder/file paths

"""
import os.path
import time
from warnings import warn

import pandas as pd

t_setup_start = time.perf_counter()

# This is a dict with variables that will be listed in the config file by default.
config_variables = {
    'dir_rbpi': '# Path to the project root of the scraper on the raspberry pi',
    'dir_exchange_log_src': '# Output folder of the runelite exchange logger',
    'dir_archive': '# Folder in which files will be archived for long-term storage',
    'dir_downloads': '# Folder in which downloaded files are placed by default',
    'exe_template_db': '# Path to file that can be used to generate template db'
}


def generate_config_file(cfg_file: str) -> bool:
    """
    Generate a config file in which the user can define certain variables or override values. Print the contents
    
    
    Parameters
    ----------
    cfg_file : str
        Path to the config file that is to be generated.
    
    Returns
    -------
    bool
        True if the file was written, False if it already exists

    """
    if os.path.exists(cfg_file):
        return False
    with open(cfg_file, 'w') as cfg:
        print(f'Writing user config file at {cfg_file}')
        for var, value in config_variables.items():
            line = f'{var}='
            cfg.write(f'{value}\n{line}\n\n')
            print(f'\t{line}')
    print(f'Config file was written and it will be opened. Please specify the missing values at {cfg_file} and save '\
            'the file.')
    print(f'The config.txt file will open now, fill in the paths, and save the script.')
    os.system(f"start notepad.exe {cfg_file}")
    time.sleep(5)
    initial_mtime = os.path.getmtime(cfg_file)
    max_wait_time_sec = 600
    time_threshold = os.path.getmtime(cfg_file) + max_wait_time_sec

    print(f'The script will resume as soon as the file has been modified...')
    while os.path.getmtime(cfg_file) == initial_mtime and time.time() < time_threshold:
        time.sleep(2)
    if time.time() >= time_threshold:
        print(f"Max waiting time of [{max_wait_time_sec // 60:0>2}:{max_wait_time_sec % 60:0>2}] was exceeded, "
              f"terminating script...")
    
    return os.path.getmtime(cfg_file) != initial_mtime


def parse_config_file(cfg_file: str, variable_parser: callable) -> dict:
    """
    Parse global_variables.path.f_user_config and parse all variables that meet requirements specified by `variable_parser`
    
    Parameters
    ----------
    cfg_file : str
        Path to the config file that is to be parsed
    variable_parser : callable
        A method that accepts a variable name and/or its respective value and returns a boolean indicating if the value
        should be parsed or not
        E.g. def parse_variable(name: str, value): return name[:4] == 'dir_' and os.path.exists(value)

    Returns
    -------
    dict
        A dict with parsed variable names and their respective values

    Raises
    ------
    
    """
    parsed = {}
    if os.path.exists(cfg_file):
        with open(cfg_file, 'r') as cfg:
            for line in cfg.readlines():
                try:
                    if not line[0].islower():
                        continue
                    var, value = line[:-1].split('=')
                    if not variable_parser(name=var, value=value):
                        continue
                    parsed[var] = value
                except ValueError:
                    pass
                except IndexError:
                    pass
    return parsed


def verify_config_file(p: str):
    """ Return True if all the paths listed in `p` are existing paths """
    
    _404 = []
    with open(p) as cfg_file:
        lines = [l.replace('\n', '') for l in cfg_file.readlines() if len(l) > 3 and l[0] != '#']
        print(lines)
        _len = 0
        for file in lines:
            var, path = file.split('=')
            _len = max(len(var), _len)
            if path[:2] == '//' or var[:2] == 'f_':
                continue
            if not os.path.exists(path) or var[:4] == 'dir_' and not os.path.isdir(path):
                _404.append((var, path))
    if len(_404) > 0:
        print(' Encountered one or more invalid paths; note that if the variable has prefix dir_ the path should refer'
              ' to an existing directory')
        print(f' The following non-existent paths are listed in config file {p};\n')
        for idx, el in enumerate(_404):
            print(f" {idx+1} {el[0]:.<{_len+2}}{el[1]}")
        print(f'\n Please set valid paths at the listed lines and rerun the script...')
    return len(_404) == 0


def setup_exception_resource_file(path: str):
    """ Generate a resource file for user-defined hard-coded exception strings """
    if os.path.exists(path):
        raise FileExistsError(f"Exception resource file already exists at {path}")
    print(f'Generating exceptions string resource file at {path}')
    dtypes = {'msg_idx': 'int64', 'exception_class': 'string', 'message': 'string'}
    pd.DataFrame(columns=list(dtypes.keys())).astype(dtype=dtypes).to_csv(path, index_label='msg_idx')
    

if __name__ == '__main__':
    # generate_config_file()
    pass