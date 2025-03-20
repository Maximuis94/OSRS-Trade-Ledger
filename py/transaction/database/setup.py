"""
Logic for setting up database
"""
import sqlite3

import os

import global_variables.path as gp
from transaction.sql import sql_create_database

def import_transactions():
    """
    Transfer Transactions from an existing database to the newly created database. Properly categorizes each
    transaction and saves it in the appropriate table.
    """
    ...


def parse_production_rules():
    """Parses and uploads all production rules into the database"""
    ...


def setup_local_database(path: gp.f_db_local, **kwargs):
    """
    Create the local database at `path`, provided it does not exist.
    
    Parameters
    ----------
    path : str, optional, global_variables.path.f_db_local by default
        The path to the database that is to be created
        
    """
    if os.path.exists(path):
        raise FileExistsError
    
    conn = sqlite3.connect(path)
    for sql_group in sql_create_database:
        for sql in sql_group:
            print(sql)
            conn.execute(sql)


# setup_local_database(os.path.join(str(gp.dir_data), "test_database.db"))
    