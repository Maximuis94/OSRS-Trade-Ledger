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
from collections.abc import Container, Collection
from typing import Any

from overrides import override

import global_variables.data_classes
import sqlite.row_factories as factories
import util.verify as verify
from model.table import Column, Table
from util.data_structures import *
from util.sql import *


class Database(sqlite3.Connection):
    """
    Class that represents a sqlite database.
    
    For class methods that involve executing sqlite statements; additional information can be found in probably be found
     in util.data_structures (e.g. input format and other details.)
    
    The Database model class also contains some native sqlite methods for getting certain properties of the connected
    database, e.g. count_rows, get_min/get_max
    """
    def __init__(self, path: str, tables: Table or Iterable = None, row_factory: Callable = dict_factory,
                 parse_tables: bool = None, read_only: bool = True, **kwargs):
        # Open db in read-only mode
        self.db_path = path
        self.database_arg = f"file:{path}?mode=ro" if read_only else path
        super().__init__(database=self.database_arg, uri=read_only)
        self.row_factory = row_factory
        self.tables, self.cursors = {}, {}

        self.default_factory = row_factory
        for key in (0, tuple, dict):
            self.add_cursor(key)
        
        # Automatically extract tables and columns from the connected database
        self.sql_tables, self.sql_indices = get_db_contents(c=self.cursor())
        self.sqlite_master = self.execute("SELECT * FROM sqlite_master", factory=var.SqliteSchema).fetchall()
        for el in self.sqlite_master:
            # print(el)
            ...
        if parse_tables or parse_tables is None:
            # print(self.db_path)
            self.extract_tables(parse_full=isinstance(parse_tables, bool))
        
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
    
    def con_exe_com(self, sql: str, parameters: Iterable = (), execute_many: bool = False):
        """
        Establish a writeable connection, execute `sql` with parameters `parameters`, commit and close. Can be used to
        submit something to a Database that is loaded as read-only
        
        Parameters
        ----------
        sql : str
            An executable sql statement that alters the database
        parameters : Iterable, optional, () by default
            A set of parameters that is to be supplied with the sqlite3.Connection.execute()/execute_many() call
        execute_many : bool, optional, False by default
            If True, invoke sqlite3.Connection.execute_many() instead of *.execute()

        """
        _con = self.write_con()
        try:
            if execute_many:
                _con.executemany(sql, parameters)
            else:
                _con.execute(sql, parameters)
        except sqlite3.ProgrammingError as e:
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
    
    def extract_tables(self, table_filter: str or Container = None, parse_full: bool = True):
        """ Generate Table and Column objects from the database by parsing its CREATE statements """
        self.row_factory, add_all = dict_factory, False
        if isinstance(table_filter, str):
            table_filter = [table_filter]
        
        self.sql_tables, self.sql_indices = get_db_contents(c=self.cursor())
        
        parse_full = parse_full and table_filter is None
        
        # Used to be able to auto-parse databases with a large amount of tables that are more or less the same
        parse_one = not parse_full and len(self.sqlite_master) > 10 and table_filter is None
        
        for _table in self.sqlite_master:
            if _table.type != 'table':
                continue
            columns = []
            
            if not parse_full and table_filter is not None and _table.name not in table_filter:
                continue
                
            for column in self.execute(f"PRAGMA table_info('{_table.name}')").fetchall():
                if len(column) < 2:
                    continue
                column['is_nullable'] = bool(abs(int(column.get('notnull'))-1))
                column['is_primary_key'] = column.get('pk') > 0
                columns.append(Column(
                    name=column.get('name'),
                    is_nullable=bool(abs(int(column.get('notnull'))-1)),
                    is_primary_key=column.get('pk') > 0
                ))
            if parse_one:
                s = ''
                for _char in _table.name:
                    if not _char.isdigit():
                        s += _char
                    else:
                        s += '_'
            else:
                s = _table.name
            self.tables[_table.name] = Table(table_name=s, columns=columns, foreign_keys=[],
                                             db_file=self.db_path)
            if parse_one:
                return
    
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
        raise NotImplementedError("Added @ 26-06")
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


if __name__ == '__main__':
    ...
