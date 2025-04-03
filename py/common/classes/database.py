"""
Model for a sqlite database.

In this module various components of the database are defined;
Column, which is a single Column within a sqlite Table
    Representation of a single value. Consists mostly of attributes and a method for exporting the create statement for
    this particular value into a table create statement.

Table, which is a single Table within an sqlite database
    Representation of a table. Can be used to generate sqlite statements that involve this table, given a set of
    arguments. Each table has a Column for each value it holds.

Database, which is the database
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
import os.path
import sqlite3
from collections import namedtuple
from collections.abc import Container, Collection
from typing import Any, Optional

from overrides import override

import common.classes.item
import common.row_factories as factories
# import util.verify as verify
from file.file import File, IFile
from common.classes.table import Column, Table
from util import verify
from util.data_structures import *
from util.sql import *
__t0__ = time.perf_counter()


def sql_create_timeseries_item_table(item_id: int, check_exists: bool = True) -> str:
    """ Returns an executable SQL statement for creating a timeseries table for a specific item """
    return \
        f"""CREATE TABLE {"IF NOT EXISTS " if check_exists else ""}"item{item_id:0>5}"(
            "src" INTEGER NOT NULL CHECK (src BETWEEN 0 AND 4),
            "timestamp" INTEGER NOT NULL,
            "price" INTEGER NOT NULL DEFAULT 0 CHECK (price>=0),
            "volume" INTEGER NOT NULL DEFAULT 0 CHECK (volume>=0),
            PRIMARY KEY(src, timestamp) )"""


def create_timeseries_db(db_file: str, item_ids: Iterable):
    con = sqlite3.connect(db_file)
    
    for i in item_ids:
        con.execute(sql_create_timeseries_item_table(item_id=i))
    con.commit()
    con.close()


class ROConn(IFile):
    """ Class of which the instances contain a read-only connection with the associated sqlite database """
    def __init__(self, path: str, allow_wcon: bool = False):
        self.file = File(path)
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"Unable to establish a connection with a non-existent file at path={path}")
        try:
            self.con = sqlite3.connect(database=f"file:{path}?mode=ro", uri=True)
        except sqlite3.OperationalError as e:
            if allow_wcon and 'invalid uri authority' in str(e):
                self.con = sqlite3.connect(path)
            else:
                raise e
    
    def __str__(self):
        return self.path
    
    def __repr__(self):
        return self.path


class Database(sqlite3.Connection, IFile):
    """
    Class that represents a sqlite database.
    
    For class methods that involve executing sqlite statements; additional information can be found in probably be found
     in util.data_structures (e.g. input format and other details.)
    
    The Database model class also contains some native sqlite methods for getting certain properties of the connected
    database, e.g. count_rows, get_min/get_max
    """
    file: File
    """The file of this Database"""
    
    def __init__(self, path: str or File, tables: Table or Iterable = None, row_factory: Callable = dict_factory,
                 parse_tables: bool = None, read_only: bool = True, **kwargs):
        if isinstance(path, File):
            self.file = path
        else:
            self.file = File(path)
        
        # Open db in read-only mode
        try:
            self.database_arg = f"file:{path}?mode=ro" if read_only else path
            super().__init__(database=self.database_arg, uri=read_only)
        except sqlite3.OperationalError as e:
            if 'invalid uri authority' in str(e):
                self.database_arg, read_only = str(path), False
                super().__init__(database=self.database_arg, uri=False)
        # self.__dict__.update(File(path).__dict__)
        self.row_factory = row_factory
        self.tables, self.cursors = {}, {}

        self.default_factory = row_factory
        for key in (0, tuple, dict):
            self.add_cursor(key)
        
        sqlite_schema_factory = factories.get_row_factory(var.SqliteSchema)
        
        # Automatically extract tables and columns from the connected database
        self.sql_tables: List[var.SqliteSchema] = self.execute(
            "SELECT type, name, tbl_name, rootpage FROM sqlite_master WHERE type='table'",
            factory=sqlite_schema_factory).fetchall()
        self.sql_indices: List[var.SqliteSchema] = self.execute(
            "SELECT type, name, tbl_name, rootpage FROM sqlite_master WHERE type='index'",
            factory=sqlite_schema_factory).fetchall()
        
        if parse_tables is None or parse_tables:
            # print(self.db_path)
            self.extract_tables(parse_full=True)
        
        if isinstance(tables, str):
            tables = Table(tables, self.file, **kwargs)
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
    def execute(self, sql: str, params: Optional[Tuple[any, ...] | Dict[str, any]] = tuple([]), factory: any = None, read_only: bool = True,
                row_factory: Optional[Callable[[sqlite3.Cursor, tuple], any]] = None, **kwargs) -> sqlite3.Cursor:
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
            c = kwargs.get("con", self.read_con if read_only else self.write_con)
            if factory is not None or row_factory is not None:
                c.row_factory = factories.get_row_factory(factory) if row_factory is None else row_factory
            
            return c.execute(sql, params)
        except sqlite3.OperationalError as e:
            if 'no such table: item' in str(e) and isinstance(params, dict) and kwargs.get('recursive') is None:
                create_timeseries_db(self.path, [params.get('item_id') if isinstance(params, dict) else params])
                kwargs['recursive'] = True
                return self.execute(sql, params, factory, read_only, **kwargs)
            raise e
            
        # except WindowsError as e:
        except sqlite3.Error as e:
            print(f'Error while executing {sql} in db {self.path}')
            if len(params) > 0:
                print('Parameters:', params)
            else:
                print('Parameters:', ())
            raise e
    
    def con_exe_com(self, sql: str, parameters: Optional[Tuple[any, ...] | Dict[str, any]] = tuple([]),
                    execute_many: bool = False):
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
        _con = self.write_con
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
    
    @property
    def write_con(self) -> sqlite3.Connection:
        """ Return a sqlite3 connection to this database that can be used for writing operations """
        return sqlite3.connect(self.path)
    
    @property
    def read_con(self) -> sqlite3.Connection:
        """Read-only sqlite3 connection to the database"""
        return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
    
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
            con = sqlite3.connect(self.path)
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
        parse_one = not parse_full and len(self.tables) > 10 and table_filter is None
        
        for _table in self.tables:
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
                                             db_file=File(self.path))
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
        con = sqlite3.connect(database=self.path)
        sql = table.insert_replace_dict if replace else table.insert_dict
        if prt:
            print(sql)
        failed = []
        for row in rows:
            row = {col: row.get(col) for col in common.classes.item.Item.__match_args__}
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
            # print(table)
            return self.cursors.get(0).execute(f"""SELECT COUNT(*) FROM '{table}' """).fetchone()
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
        
    def count_all_rows(self, csv_file: str = None) -> List[Dict[str, str or int]]:
        """ Count rows within the database on a per-table basis and export the results to a csv file, if applicable. """
        _count = namedtuple('_count', ['name', 'count'])
        counts, tables = [], self.execute("PRAGMA TABLE_LIST").fetchall()
        n, summed = len(tables), 0
        for idx, t in enumerate(tables):
            c = _count(t['name'], self.count_rows(t['name']))
            summed += c.count
            print(f"Current: {c} ({idx} / {n} | rows cumulative: {summed})", end='\r')
            counts.append(c._asdict())
        print(f"\n\n")
        
        if csv_file is not None:
            import pandas as pd
            df = pd.DataFrame(counts)
            try:
                df.to_csv(csv_file, index=False)
            except PermissionError:
                file_id, done = 0, False
                while not done:
                    file_id += 1
                    try:
                        df.to_csv(csv_file, index=False)
                    except PermissionError:
                        ...
        return counts
    
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
            temp_file = File(f'{self.split_ext()[0]}_{self.extension}')

        # First check if there is sufficient disk space available
        if not verify.disk_space(temp_file, self.fsize()):
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
            self.delete()
            shutil.copy2(temp_file, self.path)
            if remove_temp_file:
                os.remove(temp_file)
            self.__init__(path=self.path, parse_tables=False, **{k: v for k, v in self.__dict__.items()
                                                                 if k in ('tables', 'row_factory', 'read_only')})
            print(f'Database at {self.path} was vacuumed in {fmt.delta_t(time.perf_counter() - t_)}')
        
        # VACUUM failed somehow, remove resulting file
        else:
            print(f'Mismatch in row counts for tables {str(mismatches)[1:-1]}')
            print('Database VACUUM was not completed successfully...')
            os.remove(temp_file)
        return identical
    
    @override
    def fsize(self) -> int:
        """ Returns the file size in bytes of the db file """
        return os.path.getsize(self.path)
    
    @property
    def path(self) -> str:
        """Absolute path to the database file"""
        return self.file.path
    
    
            


if __name__ == '__main__':
    import global_variables.path as gp
    Database(gp.f_db_timeseries).count_all_rows('data/row_counts_161024.csv')
