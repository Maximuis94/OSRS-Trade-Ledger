"""
This module contains various methods for adding columns to a npy array

"""
from typing import Tuple

import numpy as np

from import_parent_folder import recursive_import
import util.array as u_ar
del recursive_import


def avg_price_summed_volume(ar: np.ndarray, cols: list, n_elements: int, prefix: str) -> Tuple[np.ndarray, list]:
    """
    Extend `ar` with 4 columns; averaged buy price, summed buy volume, averaged sell price, summed sell volume.
    `n_elements` dictates the scope of elements, e.g. 12 means the averaged buy price is computed per 12 subsequent
    elements.
    
    Parameters
    ----------
    ar : np.ndarray
        Numpy array with all rows. Rows should be able to be identified via `cols`
    cols : list
        List of column names
    n_elements : int
        Amount of elements to compute the average/sum over
    prefix: str
        Suffix to add to column names, e.g. 'hour' for hourly averages/sums

    Returns
    -------
    

    """
    sell_price, buy_price, sell_volume, buy_volume, price, volume = [], [], [], [], [], []
    relative_sell, relative_buy = [], []
    for idx in range(0, len(ar), n_elements):
        _ar = ar[idx:idx + n_elements]
        _p, _v = [], []
        for j, col in enumerate(['sell_', 'buy_']):
            summed_volume = u_ar.get_col(cols, _ar, f'{col}volume')
            avg_price = u_ar.get_col(cols, _ar, f'{col}price')[np.nonzero(summed_volume > 0)]
            _p += list(avg_price)
            _v += list(summed_volume)
            try:
                avg = [int(np.average(avg_price))] * n_elements
            except ValueError:
                avg = [0] * n_elements
            if j == 0:
                sell_price += avg
                sell_volume += [int(np.sum(summed_volume))] * n_elements
            else:
                buy_price += avg
                buy_volume += [int(np.sum(summed_volume))] * n_elements
                # print(len(buy_price))
                
                _p = np.array(_p)
                try:
                    _avg = [int(np.average(_p[np.nonzero(_p > 0)]))] * n_elements
                except ValueError:
                    _avg = [0] * n_elements
                price += _avg
                volume += [np.sum(_v)] * n_elements
    buy_price, sell_price = np.array(buy_price), np.array(sell_price)
    # print(buy_price.shape, sell_price.shape, n_elements, ar.shape)
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_buy_price_avg', buy_price)
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_buy_price_relative', u_ar.get_col(cols, ar, 'buy_price') - buy_price)
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_buy_volume_summed', np.array(buy_volume))
    
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_sell_price_avg', sell_price)
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_sell_price_relative', u_ar.get_col(cols, ar, 'sell_price') - sell_price)
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_sell_volume_summed', np.array(sell_volume))
    
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_avg5m_price_avg', np.array(price))
    ar, cols = u_ar.add_col(cols, ar, f'{prefix}_avg5m_volume_summed', np.array(volume))
    return ar, cols
    
    
    