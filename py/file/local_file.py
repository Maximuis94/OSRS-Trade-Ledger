"""
This module contains the controller class of the LocalFile model

"""
import os
import warnings
from collections.abc import Container
from typing import List, Dict, Tuple

from .file import File, Root
from .util import load, save


_config_file: str = '/data/resources/roots.json'
_cwd = str(os.getcwd()).replace('\\', '/')

while not os.path.exists(_cwd+os.path.split(_config_file)[0]):
    _cwd = os.path.split(_cwd)[0]
os.chdir(_cwd)
_config_file = _cwd + _config_file
_cwd = _cwd + '/'

_vars_needed: tuple = ('pc_dir_root', 'dir_rbpi', 'dir_exchange_log_src', 'dir_archive', 'dir_downloads')


def roots_config_error(e: Exception, cfg):
    """ Method that raises a specific exception related to loading paths from the roots.json config file """
    print(e)
    if cfg is not None:
        s = f"""While the file does exist, it appears one or more keys are missing. Either regenerate the file, or
                modify it. """
    else:
        s = f"""The file does not exist. It can be generated via global_variables.path.setup_roots_json(). """
    raise RuntimeError(
        f"""Error while attempting to load root folders from the root.json config file.\n{s}"""
    )


def parse_roots_config(path: str = None, vars_needed: Tuple[str] = None, verbose: bool = False) -> Dict[str, str]:
    """ Parse the roots json file and extract appropriate variables from it """
    output: Dict[str, str] = {}
    path = _config_file if path is None else path
    vars_needed = _vars_needed if vars_needed is None else vars_needed
    
    if verbose:
        print(f"The following global_variables.path attributes are updated using data from {path};")
    raise_exception = False
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"roots.json config file does not exist! Make sure the json file is saved at {path}")
    for key, root in load(path).items():
        root = Root(**root)
        try:
            if root.var in vars_needed:
                if root.exists():
                    output[root.var] = root.path
                    if verbose:
                        print(f"\tgp.{root.var}={root.path}")
                else:
                    raise_exception = 'rbpi' not in root.path
                    w = f"Loaded root for path variable {root.var} does not exist (path={root.path})"
                    warnings.warn(w)
        except KeyError:
            continue
    if raise_exception:
        raise FileNotFoundError(f"One or more roots that were loaded do not exist. Update the roots.config file or "
                                f"create the directories.")
    return output


def generate_roots_config(*roots, output_file: str = None):
    """ Generate a json file with roots that can be used throughout the project. """
    output_file = _config_file if output_file is None else output_file
    
    save({root.key: root._asdict() for root in roots if root.exists()}, path=output_file)
    print(f"Config file was saved at {output_file}.")
    