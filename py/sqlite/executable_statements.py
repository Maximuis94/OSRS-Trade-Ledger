"""
This module contains various methods for generating executable sql statements, given a set of
varying inputs.

Although it may not always be the most efficient approach, methods are designed to enforce some
consistency across sql statements executed. Aside from that, this module also serves as a reference/cheatsheet.

This module contains only methods for generating executable sql statements. Method docstrings have additional examples
related to the sql statement / clause it generates.


References
----------
    https://www.sqlitetutorial.net/ was used extensively as a source of inspiration


See Also
--------
model.database, controller.database
    Model/wrapper class for a sqlite database.

setup.database
    Module with methods that generate the databases used in the project

"""
from collections import namedtuple
from typing import Iterable, Dict, Tuple

SqlWhere = namedtuple('SqlWhere', ['column_name', 'key', 'equal', 'identical', 'is_dict'], defaults=[False, False, True])


def _convert(var, value_reference: bool = False):
    """ Convert `var` to  """
    if value_reference and isinstance(var, str):
        return '?' if len(var) == 0 else f':{var}'
    elif isinstance(var, int):
        return var
    elif isinstance(var, Iterable):
        return str(tuple(var))
    

def order_by_clause(columns: str or Iterable[str] or Dict[bool]):
    """
    Return an ORDER BY clause that can be appended to a sqlite statement. Generated from a list of columns that, in the
    order that should be preserved.
    If passed as a string, it will simply return ORDER BY <columns>
    If passed as a list, it will return ORDER BY column_1 ASC, column_2 ASC, ..., column_n ASC
    If passed as a dict, it will return ORDER BY column_1 ASC if columns[column_1] else DESC
        -> Ergo, each dict entry is expected to be a boolean
    
    Parameters
    ----------
    columns : collections.abc.Iterable
        A list of columns that is to be ordered by

    Examples
    -------
    order_by(columns='timestamp ASC') -> 'ORDER BY timestamp ASC'
    order_by(columns=['timestamp', 'item_id']) -> 'ORDER BY timestamp ASC, item_id ASC'

    """
    if isinstance(columns, str):
        return f" ORDER BY {columns} "
    else:
        sql = " ORDER BY "
        # Order all columns ascending by default
        if isinstance(columns, Iterable):
            for c in columns:
                sql += f'{c} ASC, '
        elif isinstance(columns, dict):
            for column, ascending in columns.items():
                sql += f"{column} {'A' if columns.get(column) else 'DE'}SC, "
        else:
            raise TypeError(f'Unable to process columns arg of type {type(columns)}')
        return sql[:-2]


def where_clause_by_column(column_name: str, values) -> str:
    """ Return a single column value specifier for `column_name` as part of a WHERE clause. """
    if isinstance(values, Iterable) and not isinstance(values, str):
        return f'{column_name} IN {str(tuple(values))}'
    elif values is not None:
        return f'{column_name} = :{column_name}'
    else:
        return ''


def where_clause_between(lower_bound: int = None, upper_bound: int = None, column_name: str = 'timestamp') -> str:
    """
    Return the timestamp specifiers as part of a WHERE clause, depending on which timestamps are provided. Can also be
    used for columns other than timestamp.
    
    Parameters
    ----------
    lower_bound : int, optional, None by default
        Lower bound column value
    upper_bound : int, optional, None by default
        Upper bound column value
    column_name : str, optional, 'timestamp' by default
        column name for which the value range is specified.

    Returns
    -------

    """
    if lower_bound is not None or upper_bound is not None:
        if lower_bound is not None and upper_bound is not None:
            return f'{column_name} BETWEEN :{column_name}_0 AND :{column_name}_1'
        else:
            return f'{column_name} >= :{column_name}_0' if lower_bound is not None else \
                f'{column_name} <= :{column_name}_1'
    return ''


def where_clause(conditions: (str or Iterable)) -> str:
    """
    Generate a WHERE clause from the given list of conditions and verify column references while doing so.
    The condition(s) passed should be formatted as follows; ["column_name OPERATOR <value/:dict_key>", ...].
    E.g. conditions = ["transaction_id > :min_transaction_id", "quantity < 50", "price = :target_price"]. Note that
    dict key references should always be preceded by a ':'.
    
    Both the columns list and the parameters dict are optional. If passed, they will be used to verify the result to
    ensure the passed arguments are valid within the context provided. These checks *significantly* increase runtime,
    however.

    Parameters
    ----------
    conditions : str or collections.abc.Iterable
        A list of conditions that will be added to the WHERE clause
    """
    output = "WHERE "
    if isinstance(conditions, str):
        conditions = [conditions]
    for c in conditions:
        output += f"{c} AND "
    return output[:-5]


def limit_clause(row_count: int or str, offset: int or str = None) -> str:
    """
    Return a LIMIT clause that can be appended to a sql statement. Depending on how its added, it will have the
    following effect;
    LIMIT `row_count` -> return the first `row_count` rows
    LIMIT `offset`, `row_count` -> return `row_count` rows, starting at `offset`, i.e. rows[offset:offset+row_count]
    If variables are passed as str, the str is assumed to be a dict key and it will be added as ':key'
    
    Parameters
    ----------
    row_count : int or str
        The amount of rows that should be returned.
    offset : int or str, optional, None by default
        The offset that should be applied. Read as 'return ROW_COUNT rows after skipping the first OFFSET rows'

    Returns
    -------

    """
    if offset is None:
        return f""" LIMIT {_convert(row_count, True)} """
    else:
        return f""" LIMIT {_convert(offset, True)}, {_convert(row_count, True)} """


def create_sql():
    sql = f'CREATE TABLE "{table_name}"('
    for _, col in self.column_list.items():
        if isinstance(col, Column):
            if col.is_primary_key:
                self.primary_keys.append(col.name)
            sql += col.create_table() + ', '
    
    if len(self.primary_keys) > 1:
        sql = sql.replace(' PRIMARY KEY', ' NOT NULL')
        pk = 'PRIMARY KEY('
        for column_name in self.primary_keys:
            pk += column_name + ', '
        sql += pk[:-2] + '), '
    return sql[:-2] + ' )'


def select_sql(table: str, columns: str or Iterable = '*', where: str = None, order_by: str = None,
               group_by: str = None, limit: int or Tuple[int, int] = None) -> str:
    
    sql = f"""SELECT {columns if isinstance(columns, str) else str(tuple(columns))} FROM "{table}" """
    
    if where is not None:
        sql += where if isinstance(where, str) else where_clause(conditions=where)
    
    if order_by is not None:
        sql += order_by_clause(order_by)
    
    if group_by is not None:
        ...
    
    if limit is not None:
        sql += ''


def update_sql(row: dict, table: str, id_column: str) -> str:
    """
    Generate a sqlite update exe for updating the values as specified by `row` for table `table`, using `id_column` to
    identify the to-be-updated rows.
    
    Parameters
    ----------
    row : dict
        dict that is to be passed as sqlite params when executing the resulting statement
    table : str
        Name of the table that the resulting sqlite exe will be applied to
    id_column : str
        Name of the column that will be used to identify the specific row that is to be updated

    Returns
    -------
    str
        An executable sqlite statement for updating a row with values specified by `row` within table `table`

    """
    a, columns = '', list(row.keys())
    for column_name in columns:
        a += f'{column_name} = :{column_name}, '
    return f"""UPDATE "{table}" SET {a[:-2]} WHERE {id_column} = :{id_column}"""


def insert_sql_dict(row: dict, table: str, replace: bool = True) -> str:
    """
    Generate a sqlite insert exe for inserting the values as specified by `row` in table `table`.
    
    Parameters
    ----------
    row : dict
        dict that is to be passed as sqlite params when executing the resulting statement
    table : str
        Name of the table that the resulting sqlite exe will be applied to
    replace : bool, optional, True by default
        If True, return an INSERT OR REPLACE statement instead, allowing it to overwrite existing rows.

    Returns
    -------
    str
        An executable sqlite statement for inserting a row with values specified by `row` within table `table`
    
    """
    a = ''
    for column_name in list(row.keys()):
        a += f':{column_name}, '
    return f"""INSERT {'OR REPLACE ' if replace else ''}INTO "{table}" ({a[:-2].replace(':', '')}) VALUES ({a[:-2]})"""


def insert_sql_tuple(columns: Iterable, table: str, replace: bool = True) -> str:
    """
    Generate an executable sqlite insert statement for inserting the values as specified by `row` in table `table`.
    
    Parameters
    ----------
    columns : Iterable
        Ordered list of columns that is to be inserted
    table : str
        Name of the table that the resulting sqlite exe will be applied to
    replace : bool, optional, True by default
        If True, return an INSERT OR REPLACE statement instead, allowing it to overwrite existing rows.

    Returns
    -------
    str
        An executable sqlite statement for inserting a row with values specified by `row` within table `table`
    
    """
    a, b = str(columns)[1:-1], str(['?' for _ in columns])[1:-1]
    for column_name in columns:
        a += f'{column_name}, '
        b += f'{column_name}, '
    return f"""INSERT {'OR REPLACE ' if replace else ''}INTO "{table}" ({a}) VALUES ({b})"""


def delete_sql(table: str, where_conditions: str or Iterable, **kwargs) -> str:
    return f"""DELETE FROM "{table}" {where_clause(where_conditions)}"""


def create_index(index_name: str, table: str, column_names: str or Iterable) -> str:
    """ Create an executable sql statement for creating an index """
    if not isinstance(column_names, str):
        column_names = str(tuple(column_names))[1:-1]
    return f"""CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table}" ({column_names})"""


SqlExe = namedtuple('SqlExe', ['create', 'insert', 'update', 'select', 'delete'])
generate = SqlExe(select=select_sql, create=None, insert=insert_sql_dict, update=update_sql, delete=delete_sql)

sql_where = {
    'item_id': "item_id = :item_id",
    'v0': "COL >= :VAR0",
    'v1': "COL <= :VAR1",
    'v0_v1': "BETWEEN :VAR0 AND :VAR1"
}


def _sql_where(sql_where) -> str:
    """
     Generate a sql where clause that involves a comparison of `column` and `key`
    
    Parameters
    ----------
    sql_where : SqlWhere or Iterable[SqlWhere]
        One or more instances of SqlWhere tuples

    Returns
    -------
    str
        Where clause generated from the given SqlWhere tuples

    Examples
    --------
    _sql_where([SqlWhere(column_name='item_id', key='i0'),
                SqlWhere(column_name='timestamp', key='t0_t1'),
                SqlWhere(column_name='price', key='5000')])
        -> "WHERE 'item_id' > :i0 AND 'timestamp' BETWEEN :t0 AND :t1 AND 'price' > 5000"
    
    Raises
    ------
    TypeError
        A TypeError is raised if sql_where is not an instance of SqlWhere or if any of its elements are not
    """
    if isinstance(sql_where, SqlWhere):
        sql_where = [sql_where]
    output = 'WHERE '
    
    for el in sql_where:
        if not isinstance(el, SqlWhere):
            raise TypeError(f'One of the elements of sql_where is not an instance of SqlWhere')
        key = str(el.key)
        col = f"'{el.column_name}'"
        if isinstance(key, (list, tuple)):
            output += f"""{col} IN {str(tuple(key))} AND """
        if '_' in key:
            v1, v2 = key.split('_')
            output += f"""{col} BETWEEN :{v1} AND :{v2} AND """
            continue
        if key != '?' and not key.isdigit():
            key = f':{key}' if el.is_dict else key
        operator = '<' if key[-1] == '1' else '>'
        if el.identical:
            output += f"""{col} = {key} AND """
            continue
        output += f"""{col} {operator}{'=' if el.equal else ''} {key} AND """
    return output[:-4]


def generate_foreign_key(name: str, column: str, reference_table: str, reference_column: str = None):
    """ Generate a foreign key. If `reference_column` is not passed, it is equal to `column` """
    if reference_column is None:
        reference_column = column
    return f'CONSTRAINT "{name}" FOREIGN KEY ({column}) REFERENCES {reference_table}({reference_column})'
    
# class SQL:
#
#     def __init__(self):
#         super().__init__()
#
#     def add_where(self, column: str or list or tuple, operator: str, value):
#         s = column[0] if isinstance(column, (list, tuple)) else column
#         s += f' {operator} {value}'




if __name__ == '__main__':
    print()
    ...
