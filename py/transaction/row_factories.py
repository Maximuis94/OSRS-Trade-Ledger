"""
Module with row_factories for the transaction database

"""
import sqlite3


def _factory_idx0(c: sqlite3.Cursor, row: tuple) -> any:
    return row[0]


def factory_dict(c: sqlite3.Cursor, row: tuple) -> dict:
    """ Return `row` as a dict, using column names as key """
    return {c[0]: row[i] for i, c in enumerate(c.description)}
