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
from typing import Container

from util.sql import *


class Index(NamedTuple):
    """ Class representation of an Index. """
    name: str
    Columns: Container[str]
    
    def exe_create(self):
        raise NotImplementedError


class ForeignKey(NamedTuple):
    """
    ForeignKey class. Consists of a name used to identify it, a referer Column (column the constraint applies to) and a
    referred column, which is the column used to evaluate the constraint

    """
    table_from: str
    column_from: str
    table_to: str
    column_to: str
    
    def __repr__(self):
        return f"""FOREIGN KEY({self.column_from}) REFERENCES {self.table_to}({self.column_to})"""
