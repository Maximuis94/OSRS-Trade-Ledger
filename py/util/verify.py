"""
Module with various verification methods. All methods return either True or False, indicating whether the operation can
proceed or not.

Module should be imported as 'import util.verify as verify'

"""
import os
import shutil
import sqlite3
import warnings
from collections.abc import Callable
from typing import Tuple


def disk_space(path: str, space_needed: int) -> bool:
    """ Return True if `space_needed` does not exceed free disk space on the disk of `path` """
    return space_needed < shutil.disk_usage(os.path.split(path)[0]).free


def db_dataclass(db_columns: Tuple[str], dataclass: Callable or Tuple[str]) -> bool:
    """ Returns True if the database columns (derived from) `c_db` are identical to (the columns of) `dataclass` """
    if isinstance(dataclass, Callable):
        try:
            dataclass = dataclass.__match_args__
        except AttributeError:
            dataclass = dataclass._fields
        finally:
            if dataclass is None:
                warnings.warn(f"Unable to determine (and verify) dataclass columns in verify_dataclass() via "
                              f"_fields or __match_args__ attribute")
                return False
            
    if db_columns == dataclass:
        return True
    else:
        warnings.warn(f"Mismatch between database_columns and dataclass_columns;\n"
                      f"database_columns: {db_columns}\n"
                      f"dataclass_columns: {dataclass}\n")
        return False
    