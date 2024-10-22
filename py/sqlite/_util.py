"""
Module with various sqlite-related util functions

References
----------
https://www.sqlite.org/datatype3.html
    Official SQLite documentation of datatypes
"""
from collections.abc import Callable
from typing import Dict

from multipledispatch import dispatch


_sqlite_py: Dict[str, Callable] = {
    "INTEGER": int,
    "TEXT": str,
    "REAL": float
}


_py_sqlite: Dict[Callable, str] = {
    bool: "INTEGER",
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    any: "BLOB"
}


@dispatch(str)
def convert_dtype(dtype: str) -> Callable:
    """ Convert a Python dtype to its SQLite counterpart, or vice versa. Src: https://www.sqlite.org/datatype3.html """
    try:
        return _sqlite_py[dtype]
    except KeyError:
        raise ValueError(f"Unable to find a Python dtype for SQLite dtype '{dtype}'")


@dispatch(Callable)
def convert_dtype(dtype: Callable) -> str:
    """ Convert a Python dtype to its SQLite counterpart, or vice versa. Src: https://www.sqlite.org/datatype3.html """
    try:
        return _py_sqlite[dtype]
    except KeyError:
        raise ValueError(f"Unable to find an SQLite dtype for Python dtype '{dtype}'")


