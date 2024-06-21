"""
This module contains a series of evaluations used to determine which items to include/exclude
in npy array updates.

The get_npy_update_list() method can be used to apply all evaluations, or to return a list of items using the
augment_item value from the sqlite db. augment_item is typically updated after applying all evaluations, to prevent
executing the same evaluations on roughly the same data each time.

update_item_augment_flags() can be called to update the database values, based on the given list of item_ids.
Additionally, the evaluation method can be completely bypassed by adding an item_id to the hard-coded include/exclude
lists.
    # TODO Implement item_augment override as separate values in sqlite db
        e.g. item_augment > 1 -> True and do not modify
"""
import sqlite3
import time
from collections.abc import Iterable

import numpy as np

import global_variables.configurations as gc
import global_variables.osrs as go
import global_variables.path as gp
import util.kw_parser as kw_


def np_ar_include_if_min_transactions(threshold: int = 150, **kwargs) -> bool:
    """ Include an item in array updates if it has `n` or more transactions """
    return len(kw_.parse(kw='con', return_handler=lambda x: sqlite3.connect(x), return_type=sqlite3.Connection,
                         **kwargs).execute('SELECT price FROM transactions WHERE item_id=:item_id',
                                           kwargs).fetchall()) > threshold


# Always include items that were traded less than this amount of days ago
def np_ar_include_traded_threshold_days(max_days: int = 90, **kwargs) -> bool:
    """ Include an item in array updates if it was traded less than `max_days` days ago """
    try:
        
        return (time.time() -
                kw_.parse(kw='con', return_handler=lambda x: sqlite3.connect(x), return_type=sqlite3.Connection,
                          **kwargs).execute('SELECT MAX(timestamp) FROM transactions WHERE item_id=:item_id '
                                            'ORDER BY timestamp DESC', kwargs).fetchone()[0]) \
               // 86400 < max_days
    except AttributeError as e:
        print(f'Failed to fetch ma to appropriate alternative')
        pass
    except TypeError:
        ...


def np_ar_include_daily_value(daily_value_threshold: int = 5 * pow(10, 7), n_entries: int = 14, **kwargs) -> bool:
    """
    Return whether the average price*volume value of the last `n_entries` entries of item=`item_id` in the wiki
    table exceeds `daily_value_threshold`

    Parameters
    ----------
    item_id : int
        The item id of the item that should have its average daily market value computed (added as **kwargs)
    daily_value_threshold : int
        The threshold value for the average daily market value (wiki price*volume) for the last `n_entries` should
        exceed
    n_entries : int
        Amount of entries taken into account when computing the average daily market value
    kwargs :

    Returns
    -------
    bool
        True if the last `n_entries` logged in the wiki table for item `item_id` have an average daily market value
        (=price*volume) of at least `daily_value_threshold`, False if not.

    Notes
    -----
    Tested; appears to work for proper db configs

    """
    return np.average([p * v for (p, v) in
                       kw_.parse(kw='con', return_handler=lambda x: sqlite3.connect(x), return_type=sqlite3.Connection,
                                 **kwargs).execute('SELECT price, volume FROM wiki WHERE item_id=:item_id '
                                                   'ORDER BY timestamp DESC', kwargs).fetchmany(n_entries)]) \
           >= daily_value_threshold


def np_ar_include_daily_volume(daily_volume_threshold: int, n_entries: int = 7, **kwargs) -> bool:
    """ Return True if the wiki volume of `item_id` averaged over `n_entries` exceeds `daily_volume_threshold` """
    return np.average([v for v in
                       kw_.parse(kw='con', return_handler=lambda x: sqlite3.connect(x), return_type=sqlite3.Connection,
                                 **kwargs).execute('SELECT volume FROM wiki WHERE item_id=:item_id '
                                                   'ORDER BY timestamp DESC', kwargs).fetchmany(n_entries)]) \
        >= daily_volume_threshold


def np_ar_include_alchables(n_entries: int = 3, threshold: int = 300, **kwargs) -> bool:
    """
    Return True if the given item is an alchable, i.e. it can be bought and alched for profit. Evaluates to True if
    the high alch value minus the price of a nature rune minus the guide price exceeds `threshold`
    The approach of this method is to identify items to sell, not to alch, Hence the tax.

    Parameters
    ----------
    item_id : int
        The item id of the item that should have its average daily market value computed (added as **kwargs)
    n_entries : int, optional, 3 by default
        Amount of wiki datapoints used to determine average price
    threshold : int, optional, 300 by default
        Threshold value used as evaluation criterion as described above

    Returns
    -------
    bool
        True if given `item_id` can be alched for profit

    Notes
    -----
    Tested; appears to work for proper db configs

    """
    return kwargs.get('alch_value') * .99 - np.average([p[0] for p in
                                                        kw_.parse(kw='con', return_handler=lambda x: sqlite3.connect(x),
                                                                  return_type=sqlite3.Connection,
                                                                  **kwargs).execute(
                                                            'SELECT price FROM wiki WHERE item_id=:item_id '
                                                            'ORDER BY timestamp DESC', kwargs).fetchmany(n_entries)]) \
           < threshold


def should_update(item_id: int, **kwargs) -> bool:
    """ Evaluate whether to include item `item_id` in array updates, based on a series of checks """
    
    result = False
    
    # Last transaction timestamp exceeds configurated threshold;
    if np_ar_include_traded_threshold_days(item_id=item_id, con=gp.f_db_local) or \
            np_ar_include_if_min_transactions(item_id=item_id, con=gp.f_db_local):
        result = True
    
    # Only return items with a specific daily value, unless item is traded frequently
    elif not np_ar_include_daily_value(item_id=item_id, con=gp.f_db_timeseries, daily_value_threshold=5 * pow(10, 7)):
        return False
    
    i = go.itemdb.get(item_id)
    ha = i.get('alch_value') - go.nature_rune_price
    if np_ar_include_alchables(item_id=item_id, alch_value=ha, con=gp.f_db_timeseries, buy_limit=i.get('buy_limit')):
        result = True
    # elif result and ha > 0 and i.get('buy_limit') < 150:
    #     print(i.get('item_name'), ha, nature_rune_price, int(alch_v))
    
    return result and i.get('buy_limit') >= 8 or i.get('buy_limit') == 0


def add_3dosed_remaps(npy_items, min_volume: int = 2000, min_value: int = 25*pow(10, 6)):
    """ Add certain remapped items of which the remap_to id is in the full item list """
    for i in go.item_ids:
        e = go.itemdb.get(i)
        if e.get('item_name')[-2] != '3':
            continue
        rmp_to = e.get('remap_to')
        if rmp_to is not None and isinstance(rmp_to, int) and i not in npy_items and rmp_to in npy_items and \
                np_ar_include_daily_value(item_id=i, con=gp.f_db_timeseries, daily_value_threshold=min_value) and \
                np_ar_include_daily_volume(item_id=i, con=gp.f_db_timeseries, daily_volume_threshold=min_volume):
            npy_items.append(i)
    return npy_items


def get_npy_update_list(use_augment_tag: bool, update_db: bool = False):
    """ Check if Collection `a` and `b` are mutually exclusive. If not, raise a ValueError. """
    # These lists are manually defined to include/exclude. These lists will override all other configurations;
    include = [i if i.isdigit() else go.name_id.get(i) for i in gc.np_ar_cfg_include_items]
    exclude = [i if i.isdigit() else go.name_id.get(i) for i in gc.np_ar_cfg_exclude_items]
    
    if len(frozenset(include).intersection(exclude)) > 0:
        i_e = frozenset(include).intersection(exclude)
        print("One or more item(s) should be forcibly included, but also excluded;")
        for n, i in zip(range(1, len(i_e) + 1), i_e):
            print(f'\t[{n}] {go.id_name[i]} (id={i})')
        raise ValueError("List of items to include and exclude should be mutually exclusive...")
    
    if use_augment_tag:
        include += [i for i in list(go.itemdb.keys()) if go.itemdb.get(i).get('augment_data')%2==1 and i not in exclude or go.itemdb.get(i).get('augment_data')%2==1 and go.itemdb.get(i).get('augment_data') > 1]
    else:
        include += [i for i in list(go.itemdb.keys()) if i not in exclude and should_update(item_id=i)]
    
    include = add_3dosed_remaps(npy_items=include)
    
    if update_db and not use_augment_tag:
        update_item_augment_flags(npy_array_items=include)
    
    return include
    


def update_item_augment_flags(npy_array_items: Iterable):
    """ Update the augment_item flags in the sqlite database, based on `npy_array_items`, but only if value is 0/1 """
    # TODO
    ...


if __name__ == '__main__':
    l = get_npy_update_list(True)
    print(l)
    print(len(l))
