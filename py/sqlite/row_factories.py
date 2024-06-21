"""
This module contains all of the row factory methods used throughout the project. A row factory is a Callable that 
dictates how rows will be parsed from the database. For instance, factory_dict converts rows into dicts with column 
names as key.

Tables typically have their own tuple factory that produces a named tuple. Alternatively, there are also entity 
factories. Which factory works best depends on the query, although it is possible to simply restrict yourself to basic 
dict / tuple factories.

Each method in this module can be set as a row factory. A row factory should have the following signature;
def row_factory(c: sqlite3.Cursor, row: tuple)

See Also
--------
global_variables.variables
    This module contains all namedtuples produced by some of the factories
    
model.
    The model dir has several implementations of entities as classes, each with their own
    row factory. The


    

"""
import sqlite3
from collections.abc import Callable
from enum import Enum

from global_variables.data_classes import TimeseriesDatapoint
from global_variables.variables import types, SqliteSchema
from global_variables.data_classes import *


def factory_idx0(c: sqlite3.Cursor, row: tuple):
    """ Return the first element of `row` """
    return row[0]


def factory_tuple(c: sqlite3.Cursor, row: tuple) -> tuple:
    """ Return the `row` as a regular tuple, like it does by default """
    # TODO
    return row


def factory_dict(c: sqlite3.Cursor, row: tuple) -> dict:
    """ Return `row` as a dict, using column names as key """
    return {column[0]: value for column, value in zip(c.description, row)}


def factory_dict_2(c: sqlite3.Cursor, row: tuple) -> dict:
    """ Return `row` as a dict, using column names as key """
    return {c[0]: row[i] for i, c in enumerate(c.description)}


def factory_dict_cast(c: sqlite3.Cursor, row: tuple) -> dict:
    """ Return `row` as a dict, using column names as key """
    try:
        return {c[0]: types.get(c[0]).py(row[i]) for i, c in enumerate(c.description)}
    except TypeError:
        d = {}
        for i, c in enumerate(c.description):
            try:
                d[c[0]] = types.get(c[0]).py(row[i])
            except TypeError:
                d[c[0]] = types.get(c[0]).default
                # print(c[0], types.get(c[0]).default)
        # print(d, '\n')
        return d


def factory_item_tuple(c: sqlite3.Cursor, row: tuple) -> Item:
    """  """
    # TODO
    return factory_named_tuple(c, row, Item)
        

def factory_transaction_tuple(c: sqlite3.Cursor, row: tuple) -> Transaction:
    """ Row factory that returns rows as TransactionTuple """
    return factory_named_tuple(c, row, Transaction)


def factory_avg5m(c: sqlite3.Cursor, row: tuple) -> Avg5mDatapoint:
    """ Row factory that returns rows as Avg5mTuple """
    return factory_named_tuple(c, row, Avg5mDatapoint)


def factory_realtime(c: sqlite3.Cursor, row: tuple) -> RealtimeDatapoint:
    """ Row factory that returns rows as RealtimeTuple """
    # TODO
    return factory_named_tuple(c, row, RealtimeDatapoint)


def factory_wiki(c: sqlite3.Cursor, row: tuple) -> WikiDatapoint:
    """ Row factory that returns rows as WikiTuple """
    # TODO
    return factory_data_class(c, row, WikiDatapoint)


def factory_datapoint(c: sqlite3.Cursor, row: tuple) -> TimeseriesDatapoint:
    return factory_data_class(c, row, TimeseriesDatapoint)


def factory_npy_row(c: sqlite3.Cursor, row: tuple) -> NpyDatapoint:
    return factory_data_class(c, row, NpyDatapoint)


def tuple_factory_exception(c: sqlite3.Cursor, named_tuple) -> ValueError:
    """
    Exception handler for if a TypeError occurs in a named_tuple row factory, which typically happens if insufficient
    values are supplied
    
    Parameters
    ----------
    c : sqlite3.Cursor
        The cursor that was passed to the row factory
    named_tuple
        The class of the named tuple
    
    Raises
    ------
    ValueError
        Raises a ValueError after printing some information

    """
    columns = [column[0] for column in c.description]
    print(f'Unable to create tuple {named_tuple.__name__} with columns {columns}.\n'
          f'Make sure to supply the following values as well;')
    for c in frozenset(named_tuple._fields).difference(columns):
        print(f'\t{c}')
    print(f'Alternatively, just use the {named_tuple.__name__} factory with "SELECT * FROM ..." ')
    return ValueError(f'Please provide all {named_tuple.__name__} values...')


def data_class_factory_exception(c: sqlite3.Cursor, named_tuple) -> ValueError:
    """
    Exception handler for if a TypeError occurs in a named_tuple row factory, which typically happens if insufficient
    values are supplied
    
    Parameters
    ----------
    c : sqlite3.Cursor
        The cursor that was passed to the row factory
    named_tuple
        The class of the named tuple
    
    Raises
    ------
    ValueError
        Raises a ValueError after printing some information

    """
    columns = [column[0] for column in c.description]
    print(f'Unable to create tuple {named_tuple.__name__} with columns {columns}.\n'
          f'Make sure to supply the following values as well;')
    for c in frozenset(named_tuple.__match_args__).difference(columns):
        print(f'\t{c}')
    print(f'Alternatively, just use the {named_tuple.__name__} factory with "SELECT * FROM ..." ')
    return ValueError(f'Please provide all {named_tuple.__name__} values...')
    
    
def factory_named_tuple(c, row, named_tuple):
    """ Generic namedtuple factory with exception handler """
    try:
        return named_tuple(*row)
    except TypeError:
        raise tuple_factory_exception(c, named_tuple)
    
    
def factory_data_class(c, row, data_class):
    """ Generic data class factory with exception handler """
    try:
        return data_class(*row)
    except TypeError:
        raise data_class_factory_exception(c, data_class)


def factory_single_value(c: sqlite3.Cursor=None, row: tuple=None) -> any:
    """ Method used to return rows as its first element. Best in terms of runtime. """
    # print(c.description)
    return row[0]


class CursorIdx0(sqlite3.Cursor):
    """ Cursor subclass with a row factory that will yield the first element of a Container """
    factory = factory_single_value
    
    def __init__(self, *args):
        # print(args)
        super().__init__(*args)
        self.row_factory = self.factory


def factory_db_content_parser(c, row) -> SqliteSchema:
    """ Factory that returns a row from the sqlite schema table as a namedtuple """
    return SqliteSchema(*row)

# def factory_tuple(c: sqlite3.Cursor, row: tuple) -> tuple:
#     """ Method used to return rows a tuple of values. Better in terms of runtime compared to factory_dict. """
#     return row
#
#
# def factory_dict(c: sqlite3.Cursor, row: tuple) -> dict:
#     """ Method used to return rows as a dict, while also casting them to the appropriate type. """


def timeseries_row_factory(c: sqlite3.Cursor, row: tuple) -> TimeseriesDatapoint:
    """ sqlite db row factory for generating immutable TimeseriesDatapoints """
    return TimeseriesDatapoint(*row[:2])


class Factory(Enum):
    """
    Enumeration with various row factories
    
    """
    IDX0 = factory_single_value
    TUPLE = factory_tuple
    DICT = factory_dict
    ITEM = factory_item_tuple
    TRANSACTION =  factory_transaction_tuple
    AVG5M = factory_avg5m
    REALTIMEDATAPOINT = factory_realtime
    WIKIDATAPOINT = factory_wiki
    NPYAVG5MTUPLE = factory_avg5m
    NPYREALTIMETUPLE = factory_realtime
    NPYWIKITUPLE = factory_wiki
    SQLITESCHEMA = factory_db_content_parser


factories_by_type = {
    0: factory_single_value,    # 0 as in index_0
    tuple: factory_tuple,
    dict: factory_dict,
    Item: factory_item_tuple,
    Transaction: factory_transaction_tuple,
    Avg5mDatapoint: factory_avg5m,
    RealtimeDatapoint: factory_realtime,
    WikiDatapoint: factory_wiki,
    NpyAvg5mTuple: factory_avg5m,
    NpyRealtimeTuple: factory_realtime,
    NpyWikiTuple: factory_wiki,
    SqliteSchema: factory_db_content_parser
}

cast_to = {t: types.get(t).py for t in list(types.keys())}
def cast_row_elements(c: sqlite3.Cursor, row: tuple) -> tuple:
    """ Cast the elements of row using the types described in global_variables.variables.types """
    # for col in c.description:
    #     col = col[0]
    #     print(col)
    #     print('\t', cast_to[col])
    
    return tuple((cast_to[col[0]](el) for col, el in zip(c.description, row)))


def cast_row_elements2(c: sqlite3.Cursor, row: tuple) -> tuple:
    """ Cast the elements of row using the types described in global_variables.variables.types """
    return tuple(((el if isinstance(el, cast_to.get(col[0])) else cast_to[col[0]](el)) for col, el in zip(c.description, row)))


def get_row_factory(return_type, cast_row: bool = False, default_factory: Callable = None) -> Callable:
    """
    Get a row factory that returns rows typed as `return_type`, if it is implemented.
    
    Parameters
    ----------
    return_type
        The typing of the instance that is the row_factory to return.
    cast_row : bool, optional, True by default
        If True, cast all values according to the values paired to each column name in global_variables.variables.types
    default_factory : Callable, optional, None by default
        The factory to return if the requested factory could not be found. If set to None, it will produce a TypeError
        at some point.

    Returns
    -------
    Callable
        A method that can be used as a row_factory
    
    Raises
    ------
    TypeError
        If no `default_factory` is configured and the passed `return_type` has not been documented, a TypeError will be
        raised
    
    RuntimeWarning
        If `default` factory was passed, while not being a Callable, a RuntimeWarning will be raised.
    """
    try:
        if cast_row:
            return lambda c, row: factories_by_type.get(return_type)(c, cast_row_elements(c, row))
        else:
            return factories_by_type.get(return_type)
    except TypeError:
        if default_factory is None:
            raise TypeError(f"The requested factory for type {return_type} could not be found in the factory dict.")
        elif not isinstance(default_factory, Callable):
            raise RuntimeWarning(f"Returned factory that was set to {default_factory}, which is not a Callable...")
        
        return lambda c, row: default_factory(c, cast_row_elements(c, row)) if cast_row else default_factory

