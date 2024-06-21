"""
Model for a sqlite database.

In this module various components of the database are defined;
Column: which is a single Column within a sqlite Table
    Representation of a single value. Consists mostly of attributes and a method for exporting the create statement for
    this particular value into a table create statement.

Table: which is a single Table within a sqlite database
    Representation of a table. Can be used to generate sqlite statements that involve this table, given a set of
    arguments. Each table has a Column for each value it holds.

Database: which is the database
    Consists of multiple tables. Methods are designed such that interactions with any table can be made via these
    methods, which should result in only having to interact with the Database.
    The Database is a subclass of sqlite3.Connection, it can also be used as such instead.

Upon establishing a connection, it can automatically derive the sqlite tables and their respective columns using this
connection.


See Also
--------
setup.database
    For the hard-coded database, table and column definitions see the setup.database module. All databases used within
    the project should be created using the instances defined there.
util.data_structures
    Methods for generating (parts of) sqlite statements mostly originate from this module. Methods within this module
    also contain a more in-depth explanation regarding the input that is expected.
controller.timeseries
    Controllers for fetching timeseries data. Each controller is a subclass of Database.

# TODO Make the abstract base class always read-only and establish a separate connection for write operations.

# TODO Create subclasses of Database;
    Timeseries: Database for interacting with timeseries data
    Transaction: Database for parsing/adding transactions
    Item: Database for getting Item data
        Due to the relatively small size, maybe stick with a dict {item_id: Item(item_id=item_id)} instead?

# TODO Extensively test all methods

"""
import shutil
import sqlite3
import warnings
from collections.abc import Sequence, Iterable, Container, Collection
from typing import Any

import pandas as pd
from overrides import override

import global_variables.data_classes
import global_variables.path as gp
import sqlite.row_factories as factories
import util.verify as verify
from model.table import Row, get_row_template, Column
from sqlite.executable_statements import where_clause
from util.data_structures import *
from util.sql import *


class Table(Row):
    """
    Class that represents a table within a sqlite database.
    """
    def __init__(self, table_name: str, db_file: str, **kwargs):
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
        # print(table_name)
        
        row_template = get_row_template(table_name=table_name)
        row_tuple = []
        if row_template is None:
            if '_' in table_name:
                row_template = get_row_template(table_name.split('_')[1])
                
            else:
                column_list = [c.name for c in kwargs.get('columns')]
                row_tuple = namedtuple(table_name+'Tuple', column_list)
        if row_template is None:
            cols = kwargs.get('columns')
            # print(cols)
            if cols is not None:
                if isinstance(cols, dict):
                    cols = tuple(cols.keys())
                elif isinstance(cols, Iterable) and isinstance(cols[0], Column):
                    cols = tuple([col.name for col in cols])
            # print(cols)
            row_template = namedtuple(table_name, cols)
        try:
            super().__init__(table_name=table_name, row_tuple=row_template if row_template is not None else row_tuple, db_file=db_file, **kwargs)
        except AttributeError:
            super().__init__(table_name=table_name, row_tuple=row_tuple, db_file=db_file, **kwargs)

        # self.__dict__.update(row_template.__dict__)
        
        # for k, v in self.__dict__.items():
        #     print(k, v)
        # exit(123)
        
    def insert_row(self, row: tuple, c: (sqlite3.Connection, sqlite3.Cursor) = None, replace: bool = True):
        """ Insert values of `row` into this table """
        if c is None:
            if self.db_file is None or not os.path.exists(self.db_file):
                raise FileNotFoundError(f'No db file was configured for table {self.name}, not was a connection passed')
            c = sqlite3.connect(self.db_file)
            self.insert_row(row=row, c=c, replace=replace)
            c.commit()
            c.close()
        else:
            c.execute(self.sql_insert(replace=replace, row=row), row)
    
    def sql_update_rows(self, c: sqlite3.Cursor or sqlite3.Connection, columns, values, suffix):
        ...
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
        sql = self.select
        if parameters is None:
            sql += f" {where if isinstance(where, str) else where_clause(conditions=where)}"
        else:
            # Verify resulting where_clause before returning it. Note that this significantly increases runtime.
            where = where_clause(conditions=where)
            sql += f" {where}"
        if order_by is not None:
            sql += f" {order_by}"
        return sql

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


class Database(sqlite3.Connection):
    """
    Class that represents a sqlite database.
    
    For class methods that involve executing sqlite statements; additional information can be found in probably be found
     in util.data_structures (e.g. input format and other details.)
    
    The Database model class also contains some native sqlite methods for getting certain properties of the connected
    database, e.g. count_rows, get_min/get_max
    """
    def __init__(self, path: str, tables: Table or Iterable = None, row_factory: Callable = dict_factory,
                 parse_tables: bool = True, read_only: bool = False, **kwargs):
        # Open db in read-only mode
        self.db_path = path
        self.database_arg = f"file:{path}?mode=ro" if read_only else path
        super().__init__(database=self.database_arg, uri=read_only)
        self.row_factory = row_factory
        self.tables, self.cursors = {}, {}

        self.default_factory = lambda c, row: row
        for key in (0, tuple, dict):
            self.add_cursor(key)
        
        # Automatically extract tables and columns from the connected database
        self.sql_tables, self.sql_indices = get_db_contents(c=self.cursor())
        self.sqlite_master = self.execute("SELECT * FROM sqlite_master", factory=var.SqliteSchema).fetchall()
        for el in self.sqlite_master:
            # print(el)
            ...
        if parse_tables:
            # print(self.db_path)
            self.extract_tables()
        
        if isinstance(tables, Table):
            self.tables[tables.name] = tables
        elif isinstance(tables, dict):
            self.tables = tables
        elif isinstance(tables, Iterable):
            for t in tables:
                self.tables[t.name] = t
                
        self.read_only = read_only
        self.aggregation_protocols = {}
        
    @override
    def execute(self, *args, **kwargs) -> sqlite3.Cursor:
        """
        Execute a sql statement with the parameters provided. If `factory` is passed, execute it with a cursor that has
         the factory fetched by key `factory`. See Examples section for factory args added by default.

        Returns
        -------
        sqlite3.Cursor
            Returns the cursor object after using it to execute the sql statement with the parameters provided
            
        Raises
        ------
        sqlite3.Error
            If a sqlite3 Error occurs while executing the statement, print the
        
        Examples
        --------
        The following factories are implemented by default;
        0 -> return element at the first index of the queried row
        tuple -> return row as a tuple of values
        dict -> return row as a dict, using column names as keys
        
        For specific named_tuple factories, see the sqlite.row_factories module
        

        """
        try:
            if kwargs.get('factory') is not None:
                factory = kwargs.get('factory')
                # print(factory)
                del kwargs['factory']
                try:
                    return self.cursors.get(factory).execute(*args, **kwargs)
                except AttributeError:
                    c = self.cursor()
                    c.row_factory = factories.get_row_factory(factory)
                    self.cursors[factory] = c
                    return c.execute(*args, **kwargs)
            else:
                return super().execute(*args, **kwargs)
        except sqlite3.Error as e:
            if len(args) == 0:
                args = [kwargs.get(k) for k in ['sql', 'parameters']]
                
            print(f'Error while executing {args[0]}')
            if len(args) >= 2:
                print('Parameters:', args[1])
            else:
                print('Parameters:', ())
            raise e
        
    def execute_select(self, table: str, n_rows: int = None, **kwargs) -> List[any] or any:
        """
        Execute a select statement, chosen based on input args.
        
        Parameters
        ----------
        table : str
            The name of the table the SELECT should be executed on
        n_rows: int, optional, None by default
            If set, limit the amount of resulting rows to `n_rows` rows. If it is equal to 1, return as a single element
            
        Other Parameters
        ----------------
        item_id: int, optional, None by default
            The item_id for the resulting selected rows.
        t0: int, optional, None by default
            Lower bound timestamp value. Does not apply to item table queries.
        t1: int, optional, None by default
            Upper bound timestamp value. Does not apply to item table queries.
        group_by: str, optional, None by default
            If set, group the results by this column
        order_by: str, optional, None by default
            If set, order the results by this column.
        offset: int, optional, 0 by default
            If set, skip the first `offset` rows of the query result. Only has an effect if `n_rows` is not None
        row_factory: Callable, optional, None by default
            If set, use `row_factory` as row_factory instead of the labelled tuple factory.
        
        
        Returns
        -------
        List[tuple]
            If n_rows is None or >1, return a list of labeled tuples
        tuple
            if n_rows is 1, return as a labelled tuple

        """
        if kwargs.get('action') is None:
            c = self.execute(*self.tables.get(table).sql_select(**kwargs, n_rows=n_rows),
                             factory=self.tables.get(table).row_tuple)
        elif kwargs.get('action') == 'count':
            c = self.execute(*self.tables.get(table).sql_count_rows(**kwargs, n_rows=n_rows),
                             factory=dict)
        n_rows = kwargs.get('n_rows')
        if n_rows is None or n_rows > 1:
            return c.fetchall()
        elif n_rows == 1:
            return c.fetchone()
        else:
            raise ValueError(f"Unable to process query with `n_rows`={n_rows}")
        
    def execute_insert(self, table: str, **kwargs):
        return self.execute(*self.tables.get(table).sql_insert(**kwargs))
    
    def con_exe_com(self, sql: str, parameters: Iterable = (), execute_many: bool = False):
        """ Establish a writeable connection, execute `sql` with parameters `parameters`, commit and close.
        
        Parameters
        ----------
        sql : str
            An executable sql statement that alters the database
        parameters : Iterable, optional, () by default
            A set of parameters that is to be supplied with the sqlite3.Connection.execute()/execute_many() call
        execute_many : bool, optional, False by default
            If True, invoke sqlite3.Connection.execute_many() instead of *.execute()

        """
        _con = sqlite3.connect(self.db_path)
        try:
            # Only execute_many if explicitly stated to do so rather than attempting to do both
            if execute_many:
                _con.executemany(sql, parameters)
            else:
                _con.execute(sql, parameters)
        except sqlite3.ProgrammingError as e:
            # if execute_many:
            #     _con.executemany(sql, parameters)
            # else:
            #     raise sqlite3.ProgrammingError(f'{e}\nDid not attempt sqlite3.Connection.executemany() as the '
            #                                    f'`allow_exe_many` parameter was disabled')
            raise sqlite3.ProgrammingError(e)
        _con.commit()
        _con.close()
        
    def reconnect(self):
        """ Reconnect with the database file """
        self.__init__(path=self.database_arg, tables=[t for _, t in self.tables.items()], row_factory=self.row_factory,
                      read_only=self.read_only)
    
    def write_con(self) -> sqlite3.Connection:
        """ Return a sqlite3 connection to this database that can be used for writing operations """
        return sqlite3.connect(self.db_path)
    
    def add_table(self, table: Table):
        """ Add Table `table` to this Database. An existing table with the same name will be overwritten. """
        self.tables[table.name] = table
    
    def create_table(self, sql_create: str, table_name: str = None):
        """ Create a table named `table_name` by executing `sql_create`. `table_name` is to be passed as alias """
        if sql_create[:12].upper() != 'CREATE TABLE':
            raise ValueError(f'Sql statement {sql_create} is not a statement for creating a table')
        if table_name is not None:
            self.con_exe_com(f"""CREATE TABLE "{table_name}"{sql_create[len(sql_create.split('(')[0]):]}""")
        else:
            self.con_exe_com(sql_create)
        self.close()
        super().__init__(self.db_path)
    
    def create_tables(self, tables: str or Table or Iterable = None, hush: bool = False, if_not_exists: bool = False,
                      sqls: Iterable[str] = None):
        """
        Create the tables passed as `tables`. If a table in `tables` refers to an existing table, execute that create
        statement. If it is a Table, insert that table into the tables dict, and then create it.
        
        Parameters
        ----------
        tables : str or Table or Iterable, optional, None by default
            One or more tables that are to be created
        hush : bool, optional, False by default
            If True, do not print create statements
        """
        if self.read_only:
            raise sqlite3.OperationalError(f'Unable to create tables if the connection with db is set as read-only')
        
        # If sqls is passed, exe
        if sqls is not None:
            for s in sqls:
                if not s[:12] != 'CREATE TABLE':
                    raise ValueError(f"Executable {s} is not a CREATE TABLE statement")
            con = sqlite3.connect(self.db_path)
            for sql in sqls:
                con.execute(sql)
            con.commit()
            con.close()
            return True
        
        if tables is None:
            tables = [t for _, t in self.tables.items()]
        elif isinstance(tables, str) or isinstance(tables, Table):
            tables = [tables]
        
        for t in tables:
            if isinstance(t, str):
                t = self.tables.get(t)
            elif isinstance(t, Table):
                self.tables[t.name] = t
            if not isinstance(t, Table):
                raise TypeError("Elements of tables should either refer to an existing table in self.tables or it "
                                "should be a Table")
            create = t.sql_create_table().replace(' TABLE "', ' TABLE IF NOT EXISTS "') if if_not_exists else t.create_table
            if not hush:
                print(create)
            # print(create.replace(', ', ',\n'))
            self.execute(create)
        self.commit()
    
    def add_cursor(self, key: Any, rf: Callable = None):
        """ Generate a cursor, set `rf` as its row_factory and add it to the Cursor dictionary """
        if rf is None:
            rf = factories.get_row_factory(key)
        c = self.cursor()
        c.row_factory = rf
        self.cursors[key] = c
    
    def extract_tables(self, table_filter: str or Container = None):
        """ Generate Table and Column objects from the database by parsing its CREATE statements """
        self.row_factory, add_all = dict_factory, False
        parse_all = table_filter is None
        if isinstance(table_filter, str):
            table_filter = [table_filter]
        
        self.sql_tables, self.sql_indices = get_db_contents(c=self.cursor())
        for el in self.sqlite_master:
            if el.type != 'table':
                continue
            columns = []
            if not parse_all and el.name not in table_filter:
                continue
                    
            # print(el.name, t)
            sqlite = el.sql
            # print(el.name)
            for column in self.execute(f"PRAGMA table_info('{el.name}')").fetchall():
                if len(column) < 2:
                    continue
                name = column.get('name')
                column['is_nullable'] = bool(abs(int(column.get('notnull'))-1))
                column['is_primary_key'] = column.get('pk') > 0
                column_sql = sqlite.split(f'"{name}"')[-1].split(',')[0]
                # print(column_sql)
                if 'CHECK (' in column_sql:
                    check = column_sql.split('CHECK (')[-1].split(')')[0]
                else:
                    check = None
                columns.append(Column(
                    name=column.get('name'),
                    is_nullable=bool(abs(int(column.get('notnull'))-1)),
                    is_primary_key=column.get('pk') > 0
                ))
                # print(columns[-1].create())
            table = Table(table_name=el.name, columns=columns, foreign_keys=[], db_file=self.db_path)
            self.add_table(table)
            if el.name == 'npyarray':
                # print(self.db_path)
                raise ''
                # exit(1)
    
    def as_df(self, table_name: str) -> pd.DataFrame:
        """ Convert table `table_name` to a pandas DataFrame and return it """
        return self.tables.get(table_name).as_df()
    
    def insert_rows(self, table_name: str, rows: dict or list, replace: bool = True, prt: bool = False):
        """ Insert `rows` into table `table_name`. If `replace`, allow overwriting them. """
        if isinstance(rows, dict):
            rows = [rows]
        table = self.tables.get(table_name)
        con = sqlite3.connect(database=self.db_path)
        sql = table.insert_replace_dict if replace else table.insert_dict
        if prt:
            print(sql)
        failed = []
        for row in rows:
            row = {col: row.get(col) for col in global_variables.data_classes.Item.__match_args__}
            try:
                con.execute(sql, row)
                if prt:
                    print(sql)
                    print(row)
            except sqlite3.Error as e:
                if prt:
                    print(sql)
                    print(row)
                failed.append(row)
                print(f'An sqlite3 Error occurred while trying attempting to insert row {row}\n\tError: {e}')
        con.commit()
        con.close()
        return failed
    
    def validate_rows(self, rows: Iterable, table: str):
        """ Validate each row in `rows` iteratively, using validation configurations from `table` """
        return self.tables.get(table).validate_rows(rows)
    
    def configure_validation_settings(self, table: str or Table, enable_validation: bool = None,
                                      columns: Sequence or None = ()) -> Table:
        """
        Modify validation configs for `table_name`
        
        Parameters
        ----------
        table : str
            The name of the table for which the validation settings should be altered
        enable_validation : bool, optional, None by default
            If passed, this flag dictates whether row validation should be enabled or disabled
        columns : Sequence or None, optional, () by default
            If passed as None, validate rows by including all columns, if passed as Iterable, restrict validation to
            these columns.

        """
        if isinstance(table, str):
            table = self.tables.get(table)
        table.set_validation_config(columns=columns, enable_validation=enable_validation)
        self.tables[table.name] = table
        # print(self.tables.get('transactions').__dict__)
        return table
    
    def get_table_by_columns(self, columns: Collection[str]) -> Table:
        """ Given Collection `columns`, return the table with exactly these columns, if present. If no table could be
        identified, return None.
        
        Parameters
        ----------
        columns : Collection
            A Collection of column names

        Returns
        -------
        Table
            A table listed in this database with exactly the same columns as `columns`
        """
        columns = get_sorted_tuple(columns)
        for _, table in self.tables.items():
            if table.column_tuple == columns:
                return table
    
    def insert_df(self, df: pd.DataFrame, columns: Collection[str] = None, con: sqlite3.Connection = None) -> bool:
        """
        Insert rows of DataFrame `df` into a table of the database for which the columns match the columns of `df`.
        If `column_subset` is passed, restrict the to-be submitted columns to this subset.
        
        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame that is to be inserted into this sqlite database
        columns : Collection[str], optional, None by default
            Subset of columns that is to be inserted into the database. By default, it is equal to `df`.columns.
        con : sqlite3.Connection, optional, None by default
            A connection to submit the rows to. If not passed, establish one and commit upon completion. If `con` is
            passed, the executed sql statements will *not* be committed upon completion

        Returns
        -------
        bool
            True if the DataFrame was submitted to the database, False if not.
        """
        if columns is None:
            columns = list(df.columns)
        else:
            for delete_column in frozenset(df.columns).difference(columns):
                del df[delete_column]
        columns = get_sorted_tuple(columns)
        # print(self.get_table_by_columns(columns=columns).insert_replace_dict)
        try:
            if con is None:
            #     con.executemany()
            # else:
                self.con_exe_com(sql=self.get_table_by_columns(columns=columns).insert_replace_dict,
                                 parameters=df.to_dict('records'),
                                 execute_many=True)
            else:
                con.executemany(self.get_table_by_columns(columns=columns).insert_replace_dict, df.to_dict('records'))
            return True
        except AttributeError:
            return False
        
    def get_min(self, table: str, column: str, suffix: str = '', factory=None):
        """ Return the smallest value of `column` in `table` """
        if factory is None:
            return self.execute(f"""SELECT MIN({column}) FROM {table} """+suffix, factory=0).fetchone()
        else:
            return self.execute(f"""SELECT MIN({column}) FROM {table} """+suffix, factory=factory).fetchall()
    
    def get_max(self, table: str, column: str, suffix: str = '', factory=None):
        """ Return the largest value of `column` in `table` """
        if factory is None:
            return self.execute(f"""SELECT MAX({column}) FROM {table} """+suffix, factory=0).fetchone()
        else:
            return self.execute(f"""SELECT MAX({column}) FROM {table} """+suffix, factory=factory).fetchall()

    def count_rows(self, table: str, parameters: dict = None, **kwargs):
        """ Count the number of rows listed in `table` """
        if len(kwargs) == 0:
            return self.cursors.get(0).execute(f"""SELECT COUNT(*) FROM {table}""").fetchone()
        else:
            suffix = ''
            for k in ['suffix', 'where', 'group_by']:
                a = kwargs.get(k)
                if a is not None:
                    if k == 'suffix':
                        suffix = a
                        break
                    suffix += f"{k.replace('_', ' ').upper()} {a.replace('GROUP BY', '').strip()} "
            return self.cursors.get(0).execute(f"""SELECT COUNT(*) FROM {table} {suffix}""",
                                               () if parameters is None else parameters).fetchall()
    
    def vacuum(self, temp_file: str = None, verify_vacuumed_db: bool = True, remove_temp_file: bool = True):
        """
        VACUUM this database.
        First, execute VACUUM INTO using `temp_file` as target file. After successfully executing it, verify if `verify`
         is True, then replace the old db file with the vacuumed db file.
        
        Parameters
        ----------
        temp_file : str, optional, None by default
            Temporary file to use as a target to VACUUM the database into. By default, add a _ to the file name.
        verify_vacuumed_db : bool, optional, True by default
            If True, compare the rows per table in the vacuumed db with the rows per table in the source db. A failed
            verification will prevent the old db file to be overwritten.
        remove_temp_file : bool, optional, True by default
            Remove the vacuumed db file after overwriting the original db file.
            
        Returns
        -------
        True
            If operation fully completed.
        False
            If operation ended prematurely (insufficient disk space available / row verification failed)
        
        Notes
        -----
        Depending on the size of the database file, the VACUUM operation can be quite demanding in terms of disk space,
        as the database is temporarily duplicated.
        If disk space is an issue on the original disk, `temp_file` can be used to bypass this problem without having to
         move the db before executing VACUUM.
        
        References
        ----------
        https://sqlite.org/lang_vacuum.html
            For more information on VACUUM, consult this website.
        """
        t_ = time.perf_counter()
        
        if temp_file is None:
            f, ext = os.path.splitext(self.db_path)
            temp_file = f'{f}_{ext}'

        # First check if there is sufficient disk space available
        if not verify.disk_space(temp_file, os.path.getsize(self.db_path)):
            warnings.warn(RuntimeWarning(f'Did not start Database VACUUM operation as there appears to be insufficient '
                                         f'free disk space. Consider setting `temp_file` to a different disk or freeing'
                                         f' up some space.'))
            return False
        
        self.execute(f"""VACUUM main INTO "{temp_file}" """)
        
        identical, mismatches = True, []
        if verify_vacuumed_db:
            db = Database(temp_file, tables=self.tables, parse_tables=False)
            for table in tuple(db.tables.keys()):
                if self.count_rows(table) != db.count_rows(table):
                    identical = False
                    mismatches.append(table)
            db.close()
        
        # VACUUM succeeded; close this db -> remove old file -> rename VACUUMed db -> re-initialize this object
        if identical:
            self.close()
            os.remove(self.db_path)
            shutil.copy2(temp_file, self.db_path)
            if remove_temp_file:
                os.remove(temp_file)
            self.__init__(path=self.db_path, parse_tables=False, **{k: v for k, v in self.__dict__.items()
                                                if k in ('tables', 'row_factory', 'read_only')})
            print(f'Database at {self.db_path} was vacuumed in {fmt.delta_t(time.perf_counter()-t_)}')
        
        # VACUUM failed somehow, remove resulting file
        else:
            print(f'Mismatch in row counts for tables {str(mismatches)[1:-1]}')
            print('Database VACUUM was not completed successfully...')
            os.remove(temp_file)
        return identical
        

RbpiDb = namedtuple('RbpiDb', ['path', 'table'])

rbpi_dbs = {
    'avg5m': RbpiDb(gp.f_rbpi_db_avg5m, 'avg5m'),
    'realtime': RbpiDb(gp.f_rbpi_db_realtime, 'realtime'),
    'wiki': RbpiDb(gp.f_rbpi_db_wiki, 'wiki'),
    'item': RbpiDb(gp.f_rbpi_db_item, 'itemdb')
}

if __name__ == '__main__':
    db = Database(gp.f_db_npy, read_only=True)
    for k, v in db.tables.get('item00002').columns.items():
        print(k, v.__dict__)
    print(db.tables.get('item00002').create_table)
    exit(1)
    
    
    for table in tuple(db.tables.keys()):
        for row in db.execute(f"SELECT * FROM {table}", factory=dict):
            b, s = row.get('buy_price'), row.get('sell_price')
            if b is not None and b>0 and s is not None and s>0:
                row['avg5m_price'] = (b+s)/2
            if b is not None and b>0:
                row['avg5m_price'] = b
            elif s is not None and s>0:
                row['avg5m_price'] = s
            else:
                row['avg5m_price'] = 0
            row['avg5m_value'] = row.get('avg5m_price') * row.get('avg5m_volume')
            db.execute(f'INSERT OR REPLACE INTO {table}')
