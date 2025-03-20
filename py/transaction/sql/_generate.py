"""
Methods for generating SQL statements. Rather than generating the same statement over and over again, recommended usage
is to generate the SQL statements once and paste them hard-coded into the codebase.
"""
from collections.abc import Sequence
from typing import Optional, Union, Tuple, Dict
import sqlite3
import re

from transaction.constants import TableList, transaction_db


def generate_insert_statement(table: str | TableList, tuple_input: bool = True) -> str:
    """
    Generates an INSERT statement based on a CREATE TABLE SQL statement.

    Parameters
    ----------
    table : str | TableList
        The table for which an INSERT statement should be generated
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
    con = sqlite3.connect(f"file:{transaction_db}?mode=ro", uri=True)
    table_str = str(table).strip('"')
    create_table_sql = con.execute(f"SELECT sql FROM main.sqlite_master WHERE type='table' AND name='{table_str}'") \
        .fetchone()[0]

    # Extract table name
    table_name_match = re.search(r'CREATE TABLE "(.*?)"', create_table_sql, re.IGNORECASE)
    if not table_name_match:
        raise ValueError(f"Could not extract table name from CREATE TABLE statement '{create_table_sql}'")

    # Extract column names
    column_names_match = re.findall(r'"(.*?)"\s+\w+', create_table_sql)
    if not column_names_match:
        raise ValueError("Could not extract column names from CREATE TABLE statement.")

    column_names_str = ", ".join(f'"{col}"' for col in column_names_match)

    if tuple_input:
        placeholders_str = ", ".join("?" for _ in column_names_match)
        insert_statement = f'INSERT INTO "{table_str}" ({column_names_str}) VALUES ({placeholders_str});'
    else:
        placeholders_str = ", ".join(f':{col.replace("\"", "")}' for col in column_names_match)
        insert_statement = f'INSERT INTO "{table_str}" ({column_names_str}) VALUES ({placeholders_str});'

    return insert_statement


def generate_select_statement(table: str, columns: Optional[Union[str, Sequence[str]]] = None,
                              where: Optional[Union[str, Sequence[str]]] = None,
                              order_by: Optional[Sequence[Union[str, Tuple[str, str]]]] = None) -> str:
    """
    Generate a SELECT statement based on the given (kw)args and return it. Recommended usage is to generate a SQL
    and insert that into the codebase, rather then regenerating it over and over.

    Parameters
    ----------
    table : str
        The name of the table to select from.
    columns : Optional[Union[str, Sequence[str]]], optional
        The columns to select. If None, selects all columns.
        Can be a single column name (str) or a sequence of column names.
    where : Optional[Union[str, Sequence[str]]], optional
        The WHERE clause. Can be a single condition (str) or a sequence of conditions.
    order_by : Optional[Sequence[Union[str, Tuple[str, str]]]], optional
        The ORDER BY clause. Can be a sequence of column names (str) or tuples of (column name, direction).

    Returns
    -------
    str
        The generated SELECT statement.
    """
    if columns is None:
        select_clause = "*"
    elif isinstance(columns, str):
        select_clause = columns
    else:
        select_clause = ", ".join(columns)
    
    if where is None:
        where_clause = ""
    elif isinstance(where, str):
        where_clause = f"WHERE {where}"
    else:
        where_clause = "WHERE " + " AND ".join(where)
    
    # Handle ORDER BY clause
    if order_by is None:
        order_by_clause = ""
    else:
        order_by_parts = []
        for item in order_by:
            if isinstance(item, str):
                order_by_parts.append(item)
            elif isinstance(item, tuple) and len(item) == 2:
                order_by_parts.append(f"{item[0]} {item[1]}")
            else:
                raise ValueError("Invalid order_by item: must be str or (column, direction) tuple")
        order_by_clause = "ORDER BY " + ", ".join(order_by_parts)
    
    return f"SELECT {select_clause} FROM {table} {where_clause} {order_by_clause};"


def generate_update_statement(table: str, set_values: Dict[str, Union[str, int, float]],
                              where: Optional[Union[str, Sequence[str]]] = None) -> str:
    """
    Generate an UPDATE statement based on the given parameters.

    Parameters
    ----------
    table : str
        The name of the table to update.
    set_values : Dict[str, Union[str, int, float]]
        A dictionary containing the column names and their new values.
    where : Optional[Union[str, Sequence[str]]], optional
        The WHERE clause. Can be a single condition (str) or a sequence of conditions.

    Returns
    -------
    str
        The generated UPDATE statement.
    """

    set_clause_parts = []
    for column, value in set_values.items():
        if isinstance(value, str):
            set_clause_parts.append(f'"{column}" = "{value}"') #add quotes for strings.
        else:
            set_clause_parts.append(f'"{column}" = {value}')
    set_clause = ", ".join(set_clause_parts)

    if where is None:
        where_clause = ""
    elif isinstance(where, str):
        where_clause = f"WHERE {where}"
    else:
        where_clause = "WHERE " + " AND ".join(where)

    update_statement = f"UPDATE {table} SET {set_clause} {where_clause};"

    return update_statement


# print(generate_select_statement(
#     table='"transaction"',
#     columns=None,
#     where="item_id=?",
#     order_by=["item_id", ("timestamp", "ASC")]
# ))


for table in TableList:
    table = table.value
    try:
        print(f'''insert_{table.strip('"')} = \\\n\t"""\n\t{generate_insert_statement(table=table, tuple_input=True)}\n\t"""\n''')
        print(f'''insert_{table.strip('"')}_dict = \\\n\t"""\n\t{generate_insert_statement(table=table, tuple_input=False)}\n\t"""\n''')
    except ValueError:
        ...
    except TypeError:
        ...
