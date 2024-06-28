"""
This module contains a representation for a Row of any sqlite table used throughout the project.

The base Row class provides an interface to the database model. Its specific instances represent are implemented by
sqlite tables. Note that they represent rows in an abstract sense; not specific, individual rows, although they do
provide the means to generate such rows.

The less abstract definition of an individual row can be found in sqlite.row; this module contains namedtuple
definitions for all databases used. Classes below also refer to these tuple templates for row_factories applied when
parsing data from sqlite tables.

The classes in this module can also be used for generating sqlite SELECT and INSERT statements, given a restricted set
of parameters. This is covered in more detail in the base Row class.

The base class Row is also implemented by Table, which acts as the controller of a sqlite table.
"""
import sqlite3
from typing import Type, Tuple, Callable, Dict, List, Iterable

import pandas as pd

import util.data_structures as ud
from file.file import File
from global_variables import variables as var
from global_variables.data_classes import *
from sqlite.executable_statements import insert_sql_dict
from util.sql import get_db_contents


#######################################################################################################################
# Column base class
#######################################################################################################################


class Column:
    """
    Class that represents a sqlite column. Can return a sqlite statement that can be inserted in a table CREATE
    statement.
    """
    
    def __init__(self, name: str, is_primary_key: bool = False, is_unique: bool = False, is_nullable: bool = False,
                 parent_table: str = None, db_file: File = None, add_check: bool = False, **kwargs):
        """

        Parameters
        ----------
        name : str
            Name of the variable
        is_primary_key : bool, optional, False by default
            If True, this variable will be assigned as primary key
        is_unique : bool, optional, False by default
            True if this value has to be unique within the table. If True, default value for `is_nullable` is False.
        is_nullable : bool, optional, True by default
            True if this variable is allowed to have no data within a row.
        add_check : bool, optional, False by default
            If True, add a CHECK clause equal to global_variables.variables.get_check(column_name=`name`)
        """
        self.name = name
        self.dtype = var.get_dtype(name)
        try:
            self.def_val = self.dtype.default if kwargs.get('default_value') is None else kwargs.get('default_value')
        except AttributeError:
            self.def_val = None
            
        self.add_check = add_check
        
        
        self.is_primary_key = is_primary_key is not None and is_primary_key
        # self.constraints = global_variables.variables.sqlite_constraints.get(name)
        
        if self.is_primary_key:
            self.is_unique, self.is_nullable = False, False
        else:
            self.is_unique = is_unique
            
            # Being unique implies no default value
            self.is_nullable = is_nullable
            self.def_val = None
        # print(self.create())
        if parent_table is not None and db_file is not None:
            self.from_table(db_file=db_file, parent_table=parent_table)
            
    def from_table(self, db_file: File, parent_table: str):
        """ Extract column attributes from `parent_table` """
        if db_file.exists():
            try:
                _con = sqlite3.connect(database=f'file:{db_file}?mode=ro', uri=True)
            except sqlite3.OperationalError:
                _con = sqlite3.connect(db_file)
            t = \
                [t for t in get_db_contents(_con, get_tables=True, get_indices=False) if t.get(parent_table) is not None][
                    0].get(parent_table).get('sql').split(f'"{parent_table}"(')[1]
            # print(t)
            try:
                t, pk = t.split('PRIMARY KEY(')
                # print(pk.split(')')[0].split(', '))
                self.is_primary_key = self.name in pk.split(')')[0].split(', ')
            except ValueError:
                self.is_primary_key = False
            for c in t.replace('"', '').split(', '):
                print(c)
                column_properties = c.split(' ')
                # print(column_properties)
                if column_properties[0] == self.name:
                    self.dtype = var.get_dtype(column_properties[1])
                    self.is_primary_key = 'PRIMARY KEY' in c or self.is_primary_key
                    self.is_unique = 'UNIQUE' in c or self.is_primary_key
                    self.is_nullable = 'NOT NULL' not in c and not self.is_primary_key and not self.is_unique
                    self.def_val = int(column_properties[c.index('DEFAULT')+1]) if 'DEFAULT' in c else None
                    self.add_check = 'CHECK' in c
                    
        else:
            raise FileNotFoundError("Unable to extract column attributes from an existing column within a table if the "
                                    "db file that should hold said table does not exist...")
    
    def create(self):
        """ Returns the sqlite statement that can be used to add this column to a sqlite table create statement """
        try:
            def_val = self.dtype.default if self.dtype.default is not None and len(str(self.dtype.default)) > 0 else None
        except AttributeError:
            def_val = None
        # has_default = True not in [self.is_primary_key, self.is_unique]
        _dt = var._dtype_ui32 if self.dtype is None else self.dtype
        return f'"{self.name}" {_dt.sql}' \
               f'{" PRIMARY KEY" if self.is_primary_key else ""}' \
               f'{" UNIQUE" if self.is_unique and not self.is_primary_key else ""}' \
               f'{" NOT NULL" if not self.is_nullable and not self.is_primary_key else ""}' \
               f'{f" DEFAULT {def_val}" if def_val is not None and not self.is_unique else ""}' \
               f'{f" {var.get_check(self.name)}" if self.add_check else ""}'   # \
        # f'{f" CHECK ({self.constraints})" if isinstance(self.constraints, str) else ""}'


#######################################################################################################################
# Row base class
#######################################################################################################################
class Row:
    """
    Template class for a row within any sqlite table. Its columns should be identical to the columns in the table the
    Row is derived from.
    The Row class provides methods for converting data structures into corresponding tuples/dicts and methods for
    getting executable SQLite statements.
    For all methods that generate an SQLite statement, except for create_index, if something is passed as print_result
    as keyword arg, the resulting sqlite statement and its parameters (if applicable) will be printed.
    
    Attributes
    ----------
    name: str
        Name of the row/table
    row_tuple: Type[tuple]
        namedtuple class that is mapped to this Row
    column_list: List[str]
        Ordered list with columns this Row is composed of
    column_tuple: Tuple[str]
        Alphabetically sorted tuple with column_names from column_list
    columns: Dict[str, Column]
        Dict with Column instances this Row is composed of
    primary_keys: Tuple[str]
        Tuple with columns that are primary keys of this Row
    
    Methods
    -------
    Row.to_tuple(row_dict: dict) -> tuple:
        Method to convert a row_dict into a ordered tuple, following the ordering of `column_list`
    Row.to_row_tuple(*args, **kwargs) -> Type[tuple]:
        Converts args/keyword args into the labelled tuple class assigned to this Row
    Row.sql_insert(replace: bool = True, **kwargs) -> str:
        Get an executable SQLite INSERT statement for this Row. If `replace`, use INSERT OR REPLACE
    Row.sql_select(**kwargs) -> Tuple[str, tuple]:
        Get an executable SQLite SELECT statement for this row, along with the parameters needed to execute it. The
        SELECT statement is specified using input parameters, see its docstring for more details
    Row.sql_update(row: dict, id_column: dict) -> Tuple[str, tuple]:
        Get an executable SQLite UPDATE statement for updating rows that meet criteria set by `id_column` to values
        in `row`
    Row.sql_create_table() -> str:
        Get an executable SQLite CREATE TABLE statement that can be executed to create the table, specified by the
        Row.columns dict.
    Row.create_index(index_name: str, column_names: str or Iterable)) -> str:
        Get an executable SQLite CREATE INDEX statement that can be executed to add an index to this table.
        
        
    """
    name: str
    column_list: List[str]
    column_tuple: Tuple[str]
    columns: Dict[str, Column]
    primary_keys: Tuple[str]
    
    insert_dict: str
    insert_replace_dict: str
    select: str
    select_item: str
    select_item_ts: str
    update: str
    create_table: str
    index: List[IndexTuple]
    
    def __init__(self, row_tuple: Callable, db_file: str = None, **kwargs):
        self.db_file = db_file
        self.model = dict
        
        try:
            if isinstance(kwargs.get('columns'), Iterable) and kwargs.get('table_name') is not None:
                self.name = kwargs.get('table_name')
                self.column_list = [c.name for c in kwargs.get('columns')]
                self.row_tuple = namedtuple(str(self.name), self.column_list)
            else:
                # print('row tuple', row_tuple, )
                self.name = row_tuple.__name__[:-5].lower()
                self.row_tuple = row_tuple
                try:
                    self.column_list = list(self.row_tuple.__match_args__)
                except AttributeError:
                    self.column_list = list(self.row_tuple._fields)

            self.column_tuple = ud.get_sorted_tuple(self.column_list)
            try:
                self.columns, columns = {}, kwargs.get('columns')
                if isinstance(columns, list) and isinstance(columns[0], Column):
                    columns = {col.name: col for col in columns if col.name}
                for col in self.column_list:
                    if columns is not None and isinstance(columns.get(col), Column):
                        self.columns[col] = columns.get(col)
                    else:
                        self.columns[col] = Column(name=col, parent_table=self.name, db_file=db_file)
                self.primary_keys = tuple([col.name for _, col in self.columns.items() if col.is_primary_key])
            except IndexError:
                # print(kwargs)
                if kwargs.get('n_primary_keys') is None:
                    kwargs['n_primary_keys'] = sum([int(col.is_primary_key) for col in kwargs.get('columns')])
                # print(kwargs.get('n_primary_keys'))
                self.columns = {col: Column(col, is_primary_key=idx<kwargs.get('n_primary_keys')) for idx, col in enumerate(self.column_list)}
        except AttributeError as e:
            if kwargs.get('allow_auto_tuple_factory'):
                # self.__init__(row_tuple=namedtuple(self.name()))
                # self.t
                ...
            for k, v in self.__dict__.items():
                print(k, v)
            print(f"Row.row_tuple should be the result of a call to collections.namedtuple()'")
            print(f"column_class should be the Column model class")
            raise e

        # These strings can be executed while passing the this Row as a tuple/dict
        self.insert_dict = insert_sql_dict(row={col: '' for col in self.column_list}, table=self.name, replace=False)
        self.insert_replace_dict = 'INSERT OR REPLACE' + self.insert_dict[6:]
        self.insert_tuple = self.insert_dict.split(' VALUES ')[0] + ' VALUES ' + \
                            str(tuple(['?' for _ in self.column_list])).replace("'", "")
        self.insert_replace_tuple = 'INSERT OR REPLACE' + self.insert_tuple[6:]
        self.update = f"""UPDATE "{self.name}" SET """

        self.select = f"""SELECT * FROM "{self.name}" """
        self.select_item = f"""SELECT * FROM "{self.name}" WHERE __item_id__ """

        if 'timestamp' in self.column_list:
            self.select_item_ts = f"""SELECT * FROM "{self.name}" WHERE __item_id__ AND __timestamp__"""

        self.create_table = self.sql_create_table()
        # print(self.create_table)
        
    def to_tuple(self, row_dict: dict) -> tuple:
        """ Return the values of `row_dict` as a tuple, it being ordered exactly like this Row's column_list """
        return tuple([row_dict.get(column) for column in self.column_list])
    
    def to_row_tuple(self, *args, **kwargs):
        """
        Convert the input into a namedtuple that is assigned to its corresponding sqlite table. If passed as a list of
        args, make sure the order of the elements corresponds to the template column order. Alternatively, it can be
        passed as a set of keyword args, in which case the ordering will be taken care of.

        Returns
        -------
        Type[tuple]
            A namedtuple instance that corresponds to the subclass' sqlite table rows.
        """
        return self.row_tuple(*args, **kwargs)


class Table(Row):
    """
    Class that represents a table within a sqlite database. Row is a model class for Table, as a table usually is
    composed of 0 or more rows that are formatted similarly.
    """

    def __init__(self, table_name: str, db_file: File, row_tuple: Type[tuple] = None, **kwargs):
        """
        
        Parameters
        ----------
        file: str
            The database file that this table resides in
        table_name : str
            The name of this table
        columns : model.table.Column or Iterable
            One or more columns to add to this Table
        foreign_keys : dict or Iterable, optional, None by default
        """
        self.db_file = db_file
        
        if row_tuple is None:
            column_list = [c.name for c in kwargs.get('columns')]
            row_tuple = namedtuple(table_name+'Tuple', column_list)
        super().__init__(table_name=table_name, row_tuple=row_tuple, db_file=db_file, **kwargs)
        
    def insert_row(self, row: tuple, c: (sqlite3.Connection, sqlite3.Cursor) = None, replace: bool = True):
        """ Insert values of `row` into this table """
        if c is None:
            if self.db_file is None or not self.db_file.exists():
                raise FileNotFoundError(f'No db file was configured for table {self.name}, not was a connection passed')
            c = sqlite3.connect(self.db_file)
            self.insert_row(row=row, c=c, replace=replace)
            c.commit()
            c.close()
        else:
            c.execute(self.sql_insert(replace=replace, row=row), row)
    
    def sql_update_rows(self, c: sqlite3.Cursor or sqlite3.Connection, columns, values, suffix):
        # TODO
        raise NotImplementedError
        
    def modify_table(self, columns: Column or Iterable, **kwargs):
        """ Add Column(s) `columns` to this Table and re-assess sql statements, primary keys and such """
        if isinstance(columns, Column):
            columns = [columns]
        for col in columns:
            if len(col) < 2:
                continue
            if isinstance(col, Column):
                self.columns[col.name] = col
            else:
                self.columns[col] = Column(col)
        self.primary_keys = tuple([col.name for _, col in self.columns.items() if col.is_primary_key])
        
        # TODO
        raise NotImplementedError
    
    def insert_rows(self, rows: Iterable, con: sqlite3.Connection, replace: bool = True):
        """ Insert `row` into this database table through `con` by executing `sql` """
        sql = self.insert_replace_dict if replace else self.insert_dict
        for row in rows:
            con.execute(sql, row)
    
    def as_df(self) -> pd.DataFrame:
        """ Return this table as an empty dataframe with the same columns and appropriate dtypes """
        return pd.DataFrame().astype(dtype={c: var.get_dtype(c).df for c in list(self.columns.keys())})
    
    def select_(self, where: (str or Iterable), parameters: list = None, order_by: str = None) -> str:
        """ Return an executable SELECT statement using the WHERE clause. Verify result if parameters are passed """
        # sql = self.select
        # if parameters is None:
        #     sql += f" {where if isinstance(where, str) else where_clause(conditions=where)}"
        # else:
        #     # Verify resulting where_clause before returning it. Note that this significantly increases runtime.
        #     where = where_clause(conditions=where)
        #     sql += f" {where}"
        # if order_by is not None:
        #     sql += f" {order_by}"
        # return sql
        # TODO
        raise NotImplementedError
    
    def sql_insert(self, row, replace: bool = True, **kwargs) -> Tuple[str, tuple or dict]:
        """ Return an executable sql insert (or replace) statement for a Row subclass """
        if kwargs.get('print_result') is not None:
            print(self.__dict__.get(f"insert_{'replace_' if replace else ''}"
                                 f"{'dict' if isinstance(row, dict) else 'tuple'}"), row)
        return self.__dict__.get(f"insert_{'replace_' if replace else ''}"
                                 f"{'dict' if isinstance(row, dict) else 'tuple'}"), row
    
    def sql_select(self, item_id: (int, Iterable[int]) = None, t0: int = None, t1: int = None, **kwargs) -> Tuple[str, tuple]:
        """
        Return the appropriate sql select statement, given input configurations. If a parameter is passed as None, it
        will not be included in the returned sql select statement.
        
        Parameters
        ----------
        item_id : (int, Iterable[int]), optional, None by default
            0, one or multiple item_ids to select from the table
        t0 : int, optional, None by default
            The inclusive lower bound timestamp to query
        t1 : int, optional, None by default
            The inclusive upper bound timestamp to query
            
        Other Parameters
        ----------------
        suffix: str
            String to append to the final sql statement generated
        parameters: list or tuple
            Additional parameters referred to in `suffix` can be passed as parameters.

        Returns
        -------
        Tuple[str, tuple]
            Executable sql select statement based on given parameters, along with the parameters needed to execute it.
        """
        suffix = '' if kwargs.get('suffix') is None else kwargs.get('suffix')
        parameters = []
        if item_id is not None:
            if isinstance(item_id, Iterable):
                item = f"item_id IN {str(tuple(item_id))} "
            elif isinstance(item_id, int):
                item = f"item_id=? "
                parameters.append(item_id)
                print(parameters)
            else:
                raise TypeError(f'Invalid type {type(item_id)} passed as item_id; should be None, int or an Iterable')
        else:
            item = ""
        
        kw_params = list(kwargs.get('parameters') if kwargs.get('parameters') is not None else ())
        if t0 is None and t1 is None or 'timestamp' not in self.column_list:
            # All signature parameters are None; remove WHERE and append suffix.
            return (self.select_item.replace('__item_id__', item) if item_id is not None else self.select) + \
                   suffix, tuple(kw_params)
        elif t0 is None:
            sql = self.select_item_ts.replace('__timestamp__', 'timestamp<=? ')
            parameters.append(t1)
        elif t1 is None:
            sql = self.select_item_ts.replace('__timestamp__', 'timestamp>=? ')
            parameters.append(t0)
        else:
            sql = self.select_item_ts.replace('__timestamp__', 'timestamp BETWEEN ? AND ? ')
            parameters += [t0, t1]
        sql = sql.replace('__item_id__', item)
        
        if kwargs.get('group_by') is not None:
            sql += f" GROUP BY {kwargs.get('group_by')} "
        if kwargs.get('order_by') is not None:
            sql += f"ORDER BY {kwargs.get('order_by')} "
        if kwargs.get('n_rows') is not None:
            sql += f"LIMIT ?, ? "
            # parameters += [kwargs.get('limit'), 0 if kwargs.get('offset') is None else kwargs.get('offset')]
            parameters += [0 if kwargs.get('offset') is None else kwargs.get('offset'), kwargs.get('n_rows')]
        if kwargs.get('print_result') is not None:
            print(sql.replace('__item_id__', item) + suffix, tuple(parameters+kw_params))
        return sql.replace('__item_id__', item) + suffix, tuple(parameters+kw_params)
    
    def sql_update(self, row: dict, id_column: dict, **kwargs) -> Tuple[str, tuple]:
        """
        Generate a sqlite update exe for updating the values as specified by `row` for the row table, using the value of
         `id_column` to identify the to-be-updated rows.

        Parameters
        ----------
        row : dict
            dict that is to be passed as sqlite params when executing the resulting statement
        id_column : dict
            Name of the column and its value that will be used to identify the to-be updated rows

        Returns
        -------
        str, tuple
            An executable sqlite statement for updating a row with values specified by `row` within table `table`, along
            with the parameters needed to execute it

        """
        a, columns, parameters = '', list(row.keys()), []
        for column_name in columns:
            a += f'{column_name}=?, '
            parameters.append(row.get(columns))
        parameters.append(id_column.get(list(id_column.keys()))[0])
        if kwargs.get('print_result') is not None:
            print(f"""UPDATE "{self.name}" SET {a[:-2]} WHERE {id_column}=?""", tuple(parameters))
        return f"""UPDATE "{self.name}" SET {a[:-2]} WHERE {id_column}=?""", tuple(parameters)
    
    def sql_create_table(self, **kwargs) -> str:
        """ Return an executable sqlite statement for creating this table """
        try:
            return self.create_table
        except AttributeError:
            ...
        sql = f'CREATE TABLE "{self.name}"('
        self.primary_keys = tuple([c.name for _, c in self.columns.items() if c.is_primary_key])
        for _, col in self.columns.items():
            sql += col.create() + ', '
        
        if len(self.primary_keys) > 1:
            sql = sql.replace(' PRIMARY KEY', ' NOT NULL')
            pk = 'PRIMARY KEY('
            for column_name in self.primary_keys:
                pk += column_name + ', '
            sql += pk[:-2] + '), '
        if kwargs.get('print_result') is not None:
            print(sql[:-2] + ' )')
            
        return sql[:-2] + ' )'
    
    def sql_create_index(self, index_name: str, column_names: str or Iterable) -> str:
        """
        Return a sql statement for creating an Index within this table with columns `column_names`.

        Parameters
        ----------
        index_name : str
            A label to assign to this index
        column_names : str or Iterable
            One or more columns the index should apply to

        Returns
        -------
        str
            An executable sql statement for creating an index for this table for columns `column_names`

        """
        return f"""CREATE INDEX IF NOT EXISTS "{index_name}" ON "{self.name}" """ \
               f"""{f'({column_names})' if isinstance(column_names, str) else str(tuple(column_names))}"""
    
    def sql_count_rows(self, **kwargs):
        sql, parameters = self.sql_select(**kwargs)
        group_by = kwargs.get('group_by')
        
        if kwargs.get('item_id') is None:
            sql = sql.replace('  ', ' ').replace('WHERE AND ', 'WHERE ')
        
        if group_by is not None:
            return sql.replace('SELECT *', f'SELECT *, COUNT(*)'), parameters
        else:
            return sql.replace('SELECT *', 'SELECT COUNT(*)'), parameters

    def to_csv(self, path: str = None):
        """ Convert this Table to a csv file and save it """
        if path is None:
            path = gp.dir_resources + ('loc_' if self.name in var.tables_local else 'ts_') + self.name + '_table.csv'
        print(self.columns.items())
        csv_table = {c: var.get_dtype(c).df for c in self.column_list}
        print([self.columns.get(c).__dict__ for c in list(self.columns.keys()) if self.columns.get(c) is not None])
        dts = {}
        result = []
        keys = None
        for el in [self.columns.get(c).__dict__ for c in list(self.columns.keys()) if self.columns.get(c) is not None]:
            # el['column'] = el.get('name')
            # del el['name']
            el['dtype'] = str(el.get('dtype').py).split("'")[1]
            result.append(el)
            keys = list(el.keys())
        # exit(123)
        print(keys)
        pd.DataFrame(result).astype({c: dt for c, dt in zip(keys, ['string', 'string', 'UInt8', 'UInt8', 'UInt8'])}).to_csv(path, index=False)
        print(f'Saved .csv file at {path}')
