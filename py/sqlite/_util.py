"""
Module with various sqlite-related util functions

References
----------
https://www.sqlite.org/datatype3.html
    Official SQLite documentation of datatypes
"""
import sqlite3

import re

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


def generate_insert_statement(create_table_sql: str, tuple_input: bool = True) -> str:
    """
    Generates an INSERT statement based on a CREATE TABLE SQL statement.

    Parameters
    ----------
    create_table_sql : str
        The CREATE TABLE SQL statement.
    tuple_input : bool
        If True, parameters are passed as a tuple of values.
        If False, parameters are passed as a dict

    Returns
    -------
    str
        An INSERT SQL statement with placeholders for values.

    Raises
    ------
    ValueError
        If the table name or column names cannot be extracted from the CREATE TABLE statement.
    """

    # Extract table name
    table_name_match = re.search(r'CREATE TABLE "(.*?)"', create_table_sql, re.IGNORECASE)
    if not table_name_match:
        raise ValueError(f"Could not extract table name from CREATE TABLE statement '{create_table_sql}'")
    table_name = table_name_match.group(1)

    # Extract column names
    column_names_match = re.findall(r'"(.*?)"\s+\w+', create_table_sql)
    if not column_names_match:
        raise ValueError("Could not extract column names from CREATE TABLE statement.")

    column_names_str = ", ".join(f'"{col}"' for col in column_names_match)

    if tuple_input:
        placeholders_str = ", ".join("?" for _ in column_names_match)
        insert_statement = f'INSERT INTO "{table_name}" ({column_names_str}) VALUES ({placeholders_str});'
    else:
        placeholders_str = ", ".join(f':{col.replace("\"", "")}' for col in column_names_match)
        insert_statement = f'INSERT INTO "{table_name}" ({column_names_str}) VALUES ({placeholders_str});'

    return insert_statement


def generate_insert_module(path: str, out_file: str, as_tuple: bool = True, as_dict: bool = True):
    """
    
    
    Parameters
    ----------
    path : str
        Path to the database
    out_file : str
        Output file the string is written to
    as_tuple : bool, True by default
        If True, add an insert statement that accepts a tuple
    as_dict : bool, True by default
        If True, add an insert statement that accepts a dict

    Returns
    -------

    """
    con = sqlite3.connect(path)
    output = [f'''"""\nModule with INSERT SQL statements extracted from the database at {path}\n"""''']
    for name, sql in con.execute("SELECT name, sql FROM sqlite_master WHERE type='table';").fetchall():
        try:
            if as_tuple:
                output.append(f'''sql_insert_{name}_tuple = \\\n\t"""{generate_insert_statement(sql, True)}"""''')
            if as_dict:
                output.append(f'''sql_insert_{name}_dict = \\\n\t"""{generate_insert_statement(sql, False)}"""''')
        except ValueError:
            ...
        
    with open(out_file, 'w') as out:
        out.write("\n\n".join(output))
