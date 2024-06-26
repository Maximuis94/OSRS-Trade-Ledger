"""
In this module, variables used throughout the project are defined. Each variable has specific types associated with it.
Data type mappings between various data structures are defined here for each variable. Note that this applies to 
elementary values like integers, doubles, strings and bools that are stored in the database.

Any value type stored within the database is to be defined here. If a new variable is to be added, simply add it to the
types dict with the appropriate DataTypes tuple.

Methods
-------
get_dtype(variable: any) -> DataType:
    get_dtype can be used to obtain the DataType tuple mapped to the variable. `variable` can be a column_name, a python
    type, a sqlite dtype or a pandas dtype. Sqlite/pandas dtypes should be passed as a string.
    Note that requesting a python dtype with yield the most conservative, foolproof estimate (e.g. pandas dtype int64
    for int). For the most appropriate DataTypes tuple, a column_name should be provided.
"""
from collections import namedtuple

import numpy as np

from global_variables.data_classes import Avg5mDatapoint, RealtimeDatapoint, WikiDatapoint, Transaction, Item


#######################################################################################################################
# sqlite and dataframe constants
#######################################################################################################################


# Legacy tables; still corresponds with the actual data sources, although the labelling has shifted to the src attribute
TableTuple = namedtuple('TableTuple', ['item', 'transaction', 'avg5m', 'realtime', 'wiki'])
tables_timeseries = ['avg5m', 'realtime', 'wiki']

# Timeseries table tuple where each index corresponds to the src integer value
timeseries_srcs = ('wiki', 'avg5m_buy', 'avg5m_sell', 'realtime_buy', 'realtime_sell')
tables_local = ['item', 'transaction']
tables = tables_timeseries + tables_local


# Representation for the sqlite master schema
SqliteSchema = namedtuple('SqliteSchema', ['type', 'name', 'tbl_name', 'rootpage', 'sql'])


tables_local_old = ['itemdb', 'transactions']

# Each data type has fixed related datatypes when converting them to sqlite/dataframe/python variables
DataTypes = namedtuple('DataTypes', ['py', 'sql', 'df', 'default', 'np'])


# Special DataTypes (non-nullable values like primary keys or timestamps)
_dtype_pk_int = DataTypes(py=int, sql='INTEGER', df='UInt32', default=None, np=np.int32)
_dtype_pk_bool = DataTypes(py=bool, sql='INTEGER', df='bool', default=None, np=np.bool)
_dtype_pk_str = DataTypes(py=str, sql='TEXT', df='string', default=None, np=np.str)
_dtype_unix = DataTypes(py=int, sql='INTEGER', df='UInt32', default=None, np=np.uint32)


# DataTypes for regular values
_dtype_bool = DataTypes(py=bool, sql='INTEGER', df='bool', default=0, np=np.bool)
_dtype_ui16 = DataTypes(py=int, sql='INTEGER', df='UInt16', default=0, np=np.uint16)
_dtype_i16 = DataTypes(py=int, sql='INTEGER', df='int16', default=0, np=np.int16)
_dtype_ui32 = DataTypes(py=int, sql='INTEGER', df='UInt32', default=0, np=np.uint32)
_dtype_i32 = DataTypes(py=int, sql='INTEGER', df='int32', default=0, np=np.int32)
_dtype_ui64 = DataTypes(py=int, sql='INTEGER', df='UInt64', default=0, np=np.uint64)
_dtype_i64 = DataTypes(py=int, sql='INTEGER', df='int64', default=0, np=np.int64)
_dtype_f32 = DataTypes(py=float, sql='REAL', df='float32', default=0.0, np=np.float32)
_dtype_f64 = DataTypes(py=float, sql='REAL', df='float64', default=0.0, np=np.float64)
_dtype_str = DataTypes(py=str, sql='TEXT', df='string', default='', np=np.str)


# Dict with all columns used throughout the project and their respective DataTypes.
types = {
    'id': _dtype_pk_int,
    'item_id': _dtype_pk_int,
    'src': _dtype_pk_int,
    'transaction_id': _dtype_pk_int,
    'is_buy': _dtype_pk_bool,
    'item_name': _dtype_pk_str,
    'members': _dtype_bool,
    'alch_value': _dtype_ui32,
    'buy_limit': _dtype_ui32,
    'stackable': _dtype_bool,
    'release_date': _dtype_unix,
    'equipable': _dtype_bool,
    'weight': _dtype_f32,
    'update_ts': _dtype_unix,
    'augment_data': _dtype_ui16,
    'remap_to': _dtype_ui32,
    'remap_price': _dtype_f32,
    'remap_quantity': _dtype_f32,
    'target_buy': _dtype_ui32,
    'target_sell': _dtype_ui32,
    'item_group': _dtype_str,
    'timestamp': _dtype_unix,
    'price': _dtype_ui32,
    'volume': _dtype_ui32,
    'buy_price': _dtype_ui32,
    'buy_volume': _dtype_ui32,
    'sell_price': _dtype_ui32,
    'sell_volume': _dtype_ui32,
    'quantity': _dtype_ui32,
    'status': _dtype_ui32,
    'tag': _dtype_str,
    'current_tax': _dtype_ui32,
    'current_ge': _dtype_ui32,
    'current_buy': _dtype_ui32,
    'current_sell': _dtype_ui32,
    'current_avg': _dtype_ui32,
    'avg_volume_day': _dtype_ui32,
    'margin': _dtype_ui32,
    'n_wiki': _dtype_ui32,
    'n_avg5m_b': _dtype_ui32,
    'n_avg5m_s': _dtype_ui32,
    'n_rt_b': _dtype_ui32,
    'n_rt_s': _dtype_ui32,
    'average_buy': _dtype_ui32,
    'balance': _dtype_ui64,
    'profit': _dtype_ui64,
    'value': _dtype_ui64,
    'n_sold': _dtype_ui64,
    'n_sales': _dtype_ui32,
    'n_bought': _dtype_ui64,
    'n_purchases': _dtype_ui32,
    'tax': _dtype_ui64
    
    
}
columns = list(types.keys())


def get_dtype(variable) -> DataTypes:
    """
    Given `variable`, return the DataType tuple that is associated with it. Recommended u
    
    Parameters
    ----------
    variable : any
        A variable for which the DataTypes tuple is needed

    Returns
    -------
    DataTypes
        The DataTypes tuple corresponding to `variable`
    
    Raises
    ------
    NotImplementedError
        If a variable is queried, but not found, there is a decent chance it has not been implemented yet. If this Error
        is raised and the input provided is correct, the variable is not yet listed here.

    """
    if types.get(variable) is not None:
        return types.get(variable)
    elif dtypes_by_py.get(variable) is not None:
        return dtypes_by_py.get(variable)
    elif dtypes_by_sql.get(variable) is not None:
        return dtypes_by_sql.get(variable)
    elif dtypes_by_df.get(variable) is not None:
        return dtypes_by_df.get(variable)
    # raise NotImplementedError(f'Variable {variable} is not listed in any variables dict, while it should be.')
            


# The dicts dtypes_by dicts below are specific dtype mappings to DataTypes tuples
_locals = dict(locals())
_key = '_dtype_'
_dtype_keys = [k for k in tuple(_locals.keys()) if k[:len(_key)] == _key]
dtypes_by_df = {_locals.get(dt).df: _locals.get(dt) for dt in _dtype_keys}
# print(dtypes_by_df)

_dtype_keys = ['_dtype_bool', '_dtype_i64', '_dtype_f64', '_dtype_str']
dtypes_by_sql = {_locals.get(dt).sql: _locals.get(dt) for dt in _dtype_keys}
dtypes_by_py = {_locals.get(dt).py: _locals.get(dt) for dt in _dtype_keys}

dtypes_merged = list(dtypes_by_sql.keys()) + list(dtypes_by_df.keys()) + list(dtypes_by_py.keys())

# Below are lists that are specific to certain entities, which should reduce time needed to fetch data.
# transaction_columns = [
#     'transaction_id', 'item_id', 'timestamp', 'is_buy', 'quantity', 'price', 'status', 'tag', 'update_ts'
# ]
transaction_columns = tuple(Transaction.__annotations__.keys())
transaction_types = {c: types.get(c) for c in transaction_columns}

item_metadata = ('id', 'item_id', 'item_name', 'members', 'alch_value', 'buy_limit', 'stackable', 'release_date',
                 'equipable', 'weight', 'update_ts')
# item_columns = ['id', 'item_id', 'item_name', 'members', 'alch_value', 'buy_limit', 'stackable', 'release_date',
#                 'equipable', 'weight', 'update_ts', 'augment_data', 'remap_to', 'remap_price', 'remap_quantity',
#                 'target_buy', 'target_sell', 'item_group']
item_types = Item.__annotations__
item_columns = Item.__match_args__

avg5m_types = Avg5mDatapoint.__annotations__
avg5m_columns = Avg5mDatapoint.__match_args__

realtime_types = RealtimeDatapoint.__annotations__
realtime_columns = RealtimeDatapoint.__match_args__

wiki_types = WikiDatapoint.__annotations__
wiki_columns = WikiDatapoint.__match_args__

# Keys used for parsing rows from the raspberry pi
legacy_keys = ('item_id', 'timestamp', 'is_buy', 'buy_price', 'buy_volume', 'sell_price', 'sell_volume', 'price',
               'volume')

np_array_dtypes = {
    "item_id": np.uint16,
    "timestamp": np.uint32,
    "minute": np.uint16,
    "hour": np.uint16,
    "day": np.uint16,
    "month": np.uint16,
    "year": np.uint16,
    "day_of_week": np.uint16,
    "hour_id": np.uint32,
    "day_id": np.uint32,
    "week_id": np.uint32,
    "wiki_ts": np.uint32,
    "wiki_price": np.uint32,
    "wiki_volume": np.uint32,
    "wiki_value": np.uint64,
    "wiki_volume_5m": np.uint32,
    "buy_price": np.uint32,
    "buy_volume": np.uint32,
    "buy_value": np.uint64,
    "sell_price": np.uint32,
    "sell_volume": np.uint32,
    "sell_value": np.uint64,
    "avg5m_price": np.uint32,
    "avg5m_volume": np.uint32,
    "avg5m_value": np.uint64,
    "avg5m_margin": np.int32,
    "gap_bs": np.float32,
    "gap_wb": np.float32,
    "gap_ws": np.float32,
    "rt_avg": np.uint32,
    "rt_min": np.uint32,
    "rt_max": np.uint32,
    "n_rt": np.uint8,
    "realtime_margin": np.int32,
    "tax": np.uint32,
    "est_vol_per_char": np.uint32,
    "volume_coefficient": np.float32

}


#######################################################################################################################
# sqlite CHECK constraints per sqlite data type (applies to a subset)
#######################################################################################################################
def check_bool(column_name: str) -> str:
    """ Return the CHECK constraint for boolean values for `column_name` that can be added to the CREATE statement """
    return f'{column_name} IN (0, 1)'


def check_int(column_name: str) -> str:
    """ Return the CHECK constraint for integer values for `column_name` that can be added to the CREATE statement """
    return f'{column_name}>=0'


def check_real(column_name: str) -> str:
    """ Return the CHECK constraint for float values for `column_name` that can be added to the CREATE statement """
    return f'{column_name}>=0.0'


def check_src(column_name: str) -> str:
    """ Return the CHECK constraint for boolean values for `column_name` that can be added to the CREATE statement """
    return f'CHECK (src BETWEEN 0 AND 4)'
    

sqlite_check = {
    'src': check_src,
    int: check_int,
    bool: check_bool,
    float: check_real
}


def get_check(column_name: str) -> str:
    """
    Get the python dtype for `column_name` and return a formatted CHECK clause with `column_name` in it. Note that these
    constraints are designed to only apply to a few column names.
    
    Parameters
    ----------
    column_name : str
        Name of the column in the sqlite table

    Returns
    -------
    str
        CHECK clause that can be added to a sqlite CREATE statement
    
    Raises
    ------
    AttributeError
        If types.get(`column_name`) returns None (typo / unlisted), this will result in an AttributeError.

    """
    try:
        if column_name == 'src':
            return check_src(column_name)
        return f"CHECK ({sqlite_check.get(types.get(column_name).py)(column_name)})"
    except AttributeError as e:
        raise AttributeError(f'AttributeError while attempting to generate a CHECK clause for column_name={column_name}'
                             f', this is likely due to `column_name` {column_name} not being listed in the '
                             f'global_variables.variables.types dict. If it should be, it may be due to a typo.')


#######################################################################################################################
# sqlite row factories => Moved to sqlite.row_factories
#######################################################################################################################


ExceptionRow = namedtuple('ExceptionRow', ['msg_idx', 'exception_class', 'kwargs', 'message'])

exception_msg_csv_dtypes = {'msg_idx': 'int64', 'exception_class': 'string', 'kwargs': 'string', 'message': 'string'}
exception_msg_csv_dtypes = {
    k: exception_msg_csv_dtypes.get(k) if exception_msg_csv_dtypes.get(k) is not None else 'string'
    for k in ExceptionRow._fields}

if __name__ == '__main__':
    print(list(exception_msg_csv_dtypes.keys()))