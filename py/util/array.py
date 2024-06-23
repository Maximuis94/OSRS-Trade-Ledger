"""
This module contains various basic operations applicable to arrays and other list-like data structures

"""
from collections import namedtuple
from collections.abc import Iterable, Container
from typing import Callable

import numpy as np
import pandas as pd


def add_col(columns: list, ar: np.ndarray, column_name: str, values: np.ndarray, n_decimals: int = None):
    """
    Add a new column to the collection of arrays. The name can be used to find the index of the values within the
    array collection.

    Parameters
    ----------
    columns : list
        List with all the columns so fast
    ar : numpy.ndarray
        The array that is to have the values added to it
    column_name : str
        The name of the column that is to be added
    values : np.ndarray
        The values of the column that is to be added
    n_decimals : int, optional, None by default
        If passed, round the values in `values` to this number of decimals

    Returns
    -------
    np.ndarray, list
        The extended numpy array and the extended columns list

    """
    columns.append(column_name)
    if n_decimals is not None:
        return np.append(ar, np.round(values, n_decimals)[..., None], 1), columns
    
    else:
        return np.append(ar, values[..., None], 1), columns


def get_col(columns: list, ar: np.ndarray, column_name: str) -> np.ndarray:
    """ Return the values for column `column_name`, if present """
    return ar[:, columns.index(column_name)]


def merge_per_timeframe(columns: list, ar: np.ndarray, id_column: str, to_merge: list, merge_operation: Callable = np.average):
    Merged = namedtuple('Merged', to_merge)
    merged_values = {}
    id_column_values = get_col(columns=columns, ar=ar, column_name=id_column)
    values = [get_col(column_name=value_to_merge, columns=columns, ar=ar).astype(np.float) for value_to_merge in to_merge]
    # a_buy_price, a_sell_price, a_buy_volume, a_sell_volume = get_col('avg5m_buy_price'), get_col(
    #     'avg5m_sell_price'), get_col('avg5m_buy_volume'), get_col('avg5m_sell_volume')
    for merge_by_id in np.unique(id_column_values):
        # masks = [ for val in values]
        # print(merge_operation)
        # print(values)
        # print(values.d)
        merged_values[merge_by_id] = Merged(*tuple([merge_operation(
            val[np.nonzero((id_column_values == merge_by_id) & (val>0))]) for val in values]))
    
    for _to_merge in to_merge:
        ar, columns = add_col(column_name=f'{_to_merge}_by_{id_column}',
                              values=np.array([merged_values.get(d).__getattribute__(_to_merge) for d in id_column_values]), n_decimals=3, ar=ar, columns=columns)
    return columns, ar


def get_value_range(values: np.ndarray, indices: Iterable = (0, .25, .5, .75, 1), debug: bool = False,
                    as_list: bool = False):
    """
    Return specific values from indices of the sorted list. The `indices` object is defined as a series of floating
    point values corresponding to the value at index idx%; i.e. .25 -> values[24] after sorting list `values` of length
    100.

    Parameters
    ----------
    values : np.ndarray
        An iterable list of values
    indices : Iterable, optional, (0, .25, .5, 75, -1) by default
        A list of index values that should be returned, given the sorted array. Note that -1 means values[-1], which is
        equal to 1.0
    debug : bool, optional, False by default
        True to include the accessed index in the resulting dict keys, as well


    Returns
    -------
    dict
        Return a dict with floating point index value as key and the value in sorted values array located on that index
        as value

    """
    n, values = len(values), np.sort(values)
    if debug:
        return {(float(i), min(n - 1, int(i * n))): values[min(n - 1, int(i * n))] for i in indices}
    else:
        return [values[min(n - 1, int(i * n))] for i in indices] if as_list else {
            float(i): values[min(n - 1, int(i * n))] for i in indices}


def get_graph_interval(npy_array, t0: int, t1: int, y_values: Iterable = None):
    """
    Fetch a specified set of time-series `y_values` in interval t0-t1. The data is drawn from locally saved npy arrays,
    which means the preprocessed npy_array data has to exist and that it was prepared for the interval that is given.
    If the latter is not the case, an empty data structure will simply be returned. If you are not sure which y_values
    to request, setting it to None and inspecting the result may help.

    Parameters
    ----------
    npy_array
        data_preprocessing.NpyArray object. In order to load it, there should be an existing NpyArray file for that item
    t0 : int
        Minimum unix timestamp for the requested interval
    t1: int
        Maximum unix timestamp for the requested interval
    y_values : Iterable, optional, None by default
        An iterable object with attribute names that can be extracted from `npy_array`. If set to None, return all
        available y_values

    Returns
    -------
    dict
        A dict with numpy arrays is returned, with y_value names as key and the y_values for said interval as entry.

    See Also
    --------
    data_preprocessing.add_temporal_ids, data_preprocessing.add_wiki_data and data_preprocessing.augment_scraped_data:
    These methods contain the implementations of computing the arrays. Most of the y_values also have a line describing
    it
    """
    x, y = npy_array.timestamp, npy_array.columns if y_values is None else y_values
    
    return {col: npy_array.__dict__.get(col)[np.nonzero((t1 > x) & (x >= t0))] for col in ['timestamp'] + y}


def unique_values(_set: Iterable, return_type: Callable = list, sort_ascending: bool = None):
    """ Reduce `_set` to unique values and return as `return_type`. Sort if `sort_ascending` is passed  """
    if sort_ascending is not None:
        _set = list(set(_set))
        _set.sort(reverse=not sort_ascending)
    else:
        _set = set(_set)
    return return_type(_set)


df_filter_num_operators = ['<', '>', '<=', '>=', '==']
df_filter_str_operators = []


def df_filter_num(df: pd.DataFrame, column: str, operator: str, value: int or float) -> pd.DataFrame:
    """ Apply the input parameters to the given df to filter it """
    if operator == '<':
        return df.loc[df[column] < value]
    if operator == '>':
        return df.loc[df[column] > value]
    if operator == '<=':
        return df.loc[df[column] <= value]
    if operator == '>=':
        return df.loc[df[column] >= value]
    if operator == '==':
        return df.loc[df[column] == value]
    print(f'Unable to filter numerical df values with input operator {operator}')
    return df


# def get_wiki_timestamps(item_id: int = 2, min_ts: int = None, max_ts: int = None, ts_threshold: int = None):
#     """ Fetch a list of existing wiki timestamps from the sqlite database and return it
#
#     The returned list of timestamps can be used to determine bind wiki timestamps to other timestamps with different
#     values. Each wiki timestamp is the nearest wiki timestamp to the given timestamp that is smaller or equal to that
#     timestamp.
#
#     Parameters
#     ----------
#     item_id : int, optional, 2 by default
#         item_id for which to fetch the timestamps
#     min_ts : int (optional)
#         Lower bound for timestamps
#     max_ts : int (optional)
#         Upper bound for timestamps
#     ts_threshold : int
#         threshold timestamp; the closest value smaller than this threshold will be included in the output as well
#
#     Returns
#     -------
#     numpy.ndarray
#         All wiki timestamps within the specified interval as an np array
#     """
#     w, vd = 'WHERE item_id=:item_id', {'item_id': item_id}
#     if min_ts is not None and max_ts is None:
#         w += ' AND timestamp >= :min_ts'
#         vd.update({'min_ts': min_ts})
#     elif min_ts is None and max_ts is not None:
#         w += ' AND timestamp < :max_ts'
#         vd.update({'max_ts': max_ts})
#     elif min_ts is not None and max_ts is not None:
#         w += ' AND timestamp BETWEEN :min_ts AND :max_ts'
#         vd.update({'max_ts': max_ts, 'min_ts': min_ts})
#     output = db_wiki.read_db(where_clause=w, values_dict=vd)['timestamp'].to_numpy()
#     return output if ts_threshold is None else \
#         output[len([n for n in [w - ts_threshold for w in output] if n < 0]) - 1:]


# def filter_items_by_value(value_threshold: int, item_list: list = item_ids,
#                           reference_timestamp: int = time.time() - 86400 * 15):
#     """
#     Fetch a list of item_ids that is filtered by the averaged daily market value based on the minimum value that was
#     passed. The timeframe used to sample the averaged value is defined by reference timestamps.
#
#     Parameters
#     ----------
#     value_threshold : int
#         Daily market value threshold the item ids are filtered on
#     item_list : list, optional, global_values.item_ids by default
#         The item id list that is to be filtered
#     reference_timestamp : int, optional, time.time()-86400*15 by default
#         Reference timestamp the averaged market value is based on. Value is computed based on all entries of which the
#         timestamp is greater than this value.
#
#     Returns
#     -------
#     result : dict
#         A dict with item_id as keys and the computed daily market value as value. Only consists of entries with a value
#         of `threshold_value` or greater.
#     """
#     df = get_daily_market_values(reference_timestamp=reference_timestamp)
#
#     result = {}
#     for item_id, v in [(i, np.average(df.loc[df['item_id'] == i]['daily_value'].to_numpy())) for i in item_list]:
#         # print(id_name[item_id], v)
#         if v >= value_threshold:
#             result[item_id] = int(v)
#     return result


# def get_frequently_traded_items(min_trades: int = 1) -> list:
#     """ Get a list of items that have been traded at least `min_trades` times """
#     th = db_transactions.read_db()
#     return list({i: len(th.loc[th['item_id'] == i]) for i in np.unique(th['item_id'].to_numpy())
#                  if len(th.loc[th['item_id'] == i]) > min_trades}.keys())
