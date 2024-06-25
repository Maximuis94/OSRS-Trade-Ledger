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
import os
from collections.abc import Iterable
from typing import Type, Tuple, Callable, Dict, List

import global_variables.configurations as cfg
import sqlite.row_factories as factories
import util.data_structures as ud
from global_variables import variables as var
from global_variables.classes import SingletonMeta
from global_variables.data_classes import *
from global_variables.osrs import npy_items
from global_variables.path import f_db_local, f_db_timeseries
from model.item import Item
from model.transaction import Transaction
from sqlite.executable_statements import insert_sql_dict
from util.sql import get_db_contents, get_tables


#######################################################################################################################
# Column base class
#######################################################################################################################


class Column:
    """
    Class that represents a sqlite column. Can return a sqlite statement that can be inserted in a table CREATE
    statement.
    """
    
    def __init__(self, name: str, is_primary_key: bool = False, is_unique: bool = False, is_nullable: bool = False,
                 parent_table: str = None, db_file: str = None, add_check: bool = False, **kwargs):
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
            
    def from_table(self, db_file: str, parent_table: str):
        """ Extract column attributes from `parent_table` """
        if os.path.exists(db_file):
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
    
    # def verify_value(self, value) -> bool:
    #     """ Check if `value` falls within the expected value range. Try to cast it if its typing is different. """
    #     return self.verify(value) if isinstance(value, self.dtype.py) else self.verify(self.dtype.py(value))


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
    
    def __init__(self, row_tuple: Callable, db_file: str = None, column_list=None, name=None, **kwargs):
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
                if Row.__subclasscheck__(type(row_tuple)):
                    self.row_tuple = row_tuple.row_tuple
                else:
                    self.row_tuple = row_tuple
                self.column_list = list(self.row_tuple.__match_args__)
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
    
    @staticmethod
    @abstractmethod
    def row_tuple_factory(c: sqlite3.Cursor, row: tuple):
        """ Return the row as a labeled tuple. Can be set as sqlite3.Connection.row_factory """
        ...
    
    @staticmethod
    @abstractmethod
    def row_model_factory(c: sqlite3.Cursor, row: tuple):
        """ Return the row as a labeled tuple. Can be set as sqlite3.Connection.row_factory """
        ...


#######################################################################################################################
# Row subclasses
#######################################################################################################################

class ItemRow(Row, metaclass=SingletonMeta):
    """ Template class for an entry of the item table in the sqlite db """
    name = 'item'
    row_tuple = Item
    column_list = row_tuple.__match_args__
    model = Item
    
    def __init__(self, **kwargs):
        super().__init__(row_tuple=self.row_tuple)
    
    @staticmethod
    @override
    def row_tuple_factory(c: sqlite3.Cursor, row: tuple) -> Item:
        return ItemRow.row_tuple(*row)
    
    @override
    def sql_select(self, item_id: (int, Iterable[int]) = None, **kwargs) -> str:
        """ Generate a sql select statement for an item_row. """
        if item_id is None:
            return self.select
        elif isinstance(item_id, int):
            return self.select_item.replace('__item_id__', f'item_id={item_id} ')
        elif isinstance(item_id, Iterable):
            return self.select_item.replace('__item_id__', f'item_id IN {str(tuple(item_id))} ')
        raise TypeError(F'Sql select only accepts item_id as None, int or an Iterable...')


class TransactionRow(Row, metaclass=SingletonMeta):
    """ Template class for an entry of the transaction table in the sqlite db """
    name = 'transaction'
    
    row_tuple = Transaction
    column_list = row_tuple.__match_args__
    model = Transaction
    
    def __init__(self, **kwargs):
        super().__init__(row_tuple=self.row_tuple)
    
    @staticmethod
    @override
    def row_tuple_factory(c: sqlite3.Cursor, row: tuple) -> Transaction:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return TransactionRow.row_tuple(*row)
    
    @staticmethod
    @override
    def row_model_factory(c: sqlite3.Cursor, row: tuple) -> Transaction:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return TransactionRow.model(*row)


class Avg5mRow(Row, metaclass=SingletonMeta):
    """ Template class for an entry of the avg5m table in the sqlite db """
    name = 'avg5m'
    row_tuple = Avg5mDatapoint
    column_list = row_tuple.__match_args__
    # model = Avg5m
    
    def __init__(self, **kwargs):
        super().__init__(row_tuple=self.row_tuple, **kwargs)
    
    @staticmethod
    @override
    def row_tuple_factory(c: sqlite3.Cursor, row: tuple) -> Avg5mDatapoint:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return Avg5mRow.row_tuple(*row)
    
    @staticmethod
    @override
    def row_model_factory(c: sqlite3.Cursor, row: tuple) -> Avg5mDatapoint:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return Avg5mRow.row_tuple(*row)


class RealtimeRow(Row, metaclass=SingletonMeta):
    """ Template class for an entry of the realtime table in the sqlite db """
    name = 'realtime'
    row_tuple = RealtimeDatapoint
    column_list = row_tuple.__match_args__
    # model = Realtime
    
    def __init__(self, **kwargs):
        super().__init__(row_tuple=self.row_tuple, **kwargs)
    
    def sql_select(self, **kwargs) -> Tuple[str, tuple]:
        """
        Generate a sql select statement for querying realtime rows, with an additional `is_buy` parameter.
        
        Parameters
        ----------
        kwargs :

        Returns
        -------
        
        Notes
        -----
        When querying for realtime rows, keep in mind that realtime rows are scraped every minute (as opposed to avg5m
        which is once per 5 minutes, or the once per day for the wiki). If an item is frequently traded, a select query
        can yield up to ~2800 rows per day for that item.

        """
        if kwargs.get('suffix') is not None:
            suffix = kwargs.get('suffix')
            del kwargs['suffix']
        else:
            suffix = ""
        is_buy = f" AND is_buy={int(kwargs.get('is_buy'))} " if isinstance(kwargs.get('is_buy'), (int, bool)) else " "
        
        return super().sql_select(suffix=is_buy+suffix, **kwargs)
    
    @staticmethod
    @override
    def row_tuple_factory(c: sqlite3.Cursor, row: tuple) -> RealtimeDatapoint:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return RealtimeRow.row_tuple(*row)
    
    @staticmethod
    @override
    def row_model_factory(c: sqlite3.Cursor, row: tuple) -> RealtimeDatapoint:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return RealtimeRow.row_tuple(*row)


class WikiRow(Row, metaclass=SingletonMeta):
    """ Template class for an entry of the wiki table in the sqlite db """
    name = 'wiki'
    row_tuple = WikiDatapoint
    column_list = row_tuple.__match_args__
    # model = Wiki
    
    def __init__(self, **kwargs):
        super().__init__(row_tuple=self.row_tuple, **kwargs)
    
    @staticmethod
    @override
    def row_tuple_factory(c: sqlite3.Cursor, row: tuple) -> WikiDatapoint:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return WikiRow.row_tuple(*row)
    
    @staticmethod
    @override
    def row_model_factory(c: sqlite3.Cursor, row: tuple) -> WikiDatapoint:
        """ Returns a tuple parsed from the sqlite db as a labelled tuple """
        return WikiRow.row_tuple(*row)


class NpyAvg5mRow(Avg5mRow):
    name = 'npy_avg5m'
    row_tuple = NpyAvg5mTuple
    # model = Avg5m
    
    def __init__(self):
        super().__init__(n_primary_keys=2, row_tuple=self.row_tuple)


class NpyRealtimeRow(RealtimeRow):
    name = 'npy_realtime'
    row_tuple = NpyRealtimeTuple
    
    def __init__(self):
        super().__init__(n_primary_keys=3, row_tuple=self.row_tuple)


class NpyWikiRow(WikiRow):
    name = 'npy_wiki'
    row_tuple = NpyWikiTuple
    
    def __init__(self):
        super().__init__(n_primary_keys=2, row_tuple=self.row_tuple)


def get_row_template(table_name: str):
    """ Given a `table_name`, return the corresponding Row template class """
    return {
        'item': ItemRow,
        'itemdb': ItemRow,
        'transaction': TransactionRow,
        'avg5m': Avg5mRow,
        'realtime': RealtimeRow,
        'wiki': WikiRow,
        'npy_avg5m': NpyAvg5mRow,
        'npy_realtime': NpyRealtimeRow,
        'npy_wiki': NpyWikiRow,
        'npyarray': NpyTable
    }.get(table_name)



class NpyTable(Row):
    """"""
    row_tuple = NpyDatapoint
    name = row_tuple.__name__[:-5].lower()
    column_list = row_tuple.__match_args__
    db_file = gp.f_db_timeseries
    model = None
    
    
    def __init__(self):
        super().__init__(row_tuple=self.row_tuple)
        _t = int(time.time())
        self.t1 = _t - _t % 14400
        self.t0, self.t1 = _t - _t % 86400 - cfg.np_ar_cfg_total_timespan_d * 86400, _t - _t % 14400
        self.min_t = self.t0 - 86400 * 7
        self.max_ts_by_item = f"""SELECT MAX(timestamp) FROM "{self.name}" WHERE item_id=?"""
        print(self.column_list)
    
    @staticmethod
    def remove_expired_rows(self):
        """ Remove the rows from the database with a timestamp smaller than the lower bound """
        con = sqlite3.connect(self.db_file)
        con.row_factory = factories.factory_single_value
        
        _del = tuple(frozenset(con.execute("SELECT DISTINCT item_id FROM npyarray").fetchall()).difference(npy_items))
        
        if len(_del) > 0:
            print(f'Removing rows with item_id in {_del}')
            con.execute(f"""DELETE FROM "{self.name}" WHERE item_id IN {str(_del)[1:-1]}""")
        con.execute(f"""DELETE FROM "{self.name}" WHERE timestamp < ?""", (self.min_t,))
        con.commit()
        con.close()
    
    def add_new_rows(self):
        """ Extend the table with entries for each item_id in the item_id list. If the item_id was previously unlogged,
         create a full timespan from min_ts to t1, else, only add rows spanning from MAX(timestamp) to t1."""
        ts_con = sqlite3.connect(database=f'file:{self.db_file}?mode=ro', uri=True)
        ts_con.row_factory = factories.factory_single_value
        sql = f"""INSERT INTO "{self.name}"(item_id, timestamp, is_buy, price) SELECT item_id, timestamp, is_buy, """\
                                        """price FROM realtime WHERE """
        for item_id in npy_items:
            try:
                # Extend array
                cur_t1 = ts_con.execute(self.max_ts_by_item, (item_id,)).fetchall()[0] + 300
            except IndexError:
                # Reset / construct array
                cur_t1 = self.t0
        ts_con.row_factory = factories.factory_tuple
        for ts in range(cur_t1, self.t1, 300):
            ...
        
        
    
    @staticmethod
    def extend_array(npy_ar: np.ndarray, new_rows: np.ndarray):
        """ Extend npy_array npy_ar with rows `new_rows` """
        if npy_ar.shape[1] == new_rows.shape[1]:
            return np.append(npy_ar, new_rows, 0)
        else:
            raise ValueError("Mismatch between n_columns of `npy_ar` and `new_rows`")
    
    def create_table(self):
        sql = f"""CREATE TABLE IF NOT EXISTS "{self.name}"("timestamp"   """
        con = sqlite3.connect(self.db_file)
        con.execute(sql)
        con.commit()
        con.close()

def verify_rows():
    """
    Check if the hard-coded column lists are identical to the columns extracted from used sqlite dbs.
    Raise a ValueError if this is not the case
    """
    
    for _db in (f_db_timeseries, f_db_local):
        if not os.path.exists(_db):
            raise FileNotFoundError(f"Database file {_db} does not exist. It can be created via setup.database")
        _tables = get_tables(get_db_contents(sqlite3.connect(database=f'file:{_db}?mode=ro', uri=True),
                                             get_indices=False)[0])
        for key, table_list in _tables.items():
            # if tuple(row_prefix.get(key) + table_list) != tuple(row_classes.get(key).columns):
            if tuple(table_list) != get_row_template(key).column_list:
                print(f'\n\n\n *** Mismatch between parsed and hard-coded column lists! ***\n'
                      f'To fix this, set the `{get_row_template(key)}.column_list={table_list}`')
                raise ValueError(f"Mismatch between columns of {key};",
                                 tuple(table_list), tuple(get_row_template(key).column_list))
    return True


# verify_rows()
