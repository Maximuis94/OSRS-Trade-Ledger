"""
This module contains various methods for analyzing data for a specific item

TODO methods have not yet been redesigned for new db structure
"""
import datetime
import math
import os
import sqlite3
from collections import namedtuple
from collections.abc import Iterable, Container, Sized
from typing import List, Tuple, Dict, NamedTuple

import numpy as np
import pandas as pd
from multipledispatch import dispatch

from venv_auto_loader.active_venv import *
from global_variables.importer import *
from item.db_entity import Item
from data_processing import util as ud
__t0__ = time.perf_counter()


# El = namedtuple('El', ['t0', 't1', 'avg', 'relative'])

class El(NamedTuple):
    item_id: int
    attribute_name: str
    t0: int
    t1: int
    avg: int or float
    relative: int or float
    round_avg: bool = True
    
    def __repr__(self):
        if self.round_avg:
            avg = int(round(self.avg, 0))
        else:
            avg = self.avg if math.modf(self.avg)[0] != 0 else int(self.avg)
        return f"{ut.utc_unix_dt(self.t0)}-{ut.utc_unix_dt(self.t1-1).time()}:\t" \
               f"{avg}\t{self.relative if math.modf(self.relative)[0] != 0 else int(self.relative)}"
    
    def __repr_dict__(self, previous_row) -> dict:
        """ Return this object as a dict, designed to insert into csv files """
        if self.round_avg:
            avg = int(round(self.avg, 0))
        else:
            avg = self.avg if math.modf(self.avg)[0] != 0 else int(self.avg)
        try:
            if previous_row is not None:
                print('\t\t\t', previous_row.avg if math.modf(previous_row.avg)[0] != 0 else int(previous_row.avg))
                previous_average = f"{100*(round(1 / (previous_row.avg / avg), 4)-1):.1f}%"
            else:
                previous_average = ''
        except Exception as e:
            print(f'Exception {e}')
            previous_average = 'NA'
            
        dt0, dt1 = ut.utc_unix_dt(self.t0), ut.utc_unix_dt(self.t1-1)
        # return {'item_name': go.id_name[self.item_id], 'attribute_name': self.attribute_name, 'dow': dt0.strftime("%A"),
        #         'date': dt0.date(), 't0': f"{dt0.time()}", 't1': f"{dt1.time()}",
        #         'average': avg, 'relative': self.relative if math.modf(self.relative)[0] != 0 else int(self.relative),
        #         'relative_previous': previous_average}
        return {'dow': dt0.strftime("%A"), 'date': dt0.date(), 't0': f"{dt0.time()}", 't1': f"{dt1.time()}",
                'average': avg, 'relative': self.relative if math.modf(self.relative)[0] != 0 else int(self.relative),
                'relative_previous': previous_average}

# @dispatch(list or tuple, str, dict)
# def compare_npy_data(datetimes: List[datetime.datetime] or Tuple[datetime.datetime],
#                      attribute_name: str, **kwargs):
#     compare_npy_data(datetimes, [attribute_name], **kwargs)


# @dispatch(list or tuple, list or tuple, dict)
def compare_npy_data(datetimes: List[datetime.datetime] or Tuple[datetime.datetime],
                     attribute_names: List[str] or Tuple[str], **kwargs) -> Dict[Tuple[int, str], List[List[El]]]:
    """
    Given a set of datetimes, run a comparative analysis in which the first datetime is used as benchmark, while the
    other values expressed relative
    
    Parameters
    ----------
    datetimes : List[datetime.datetime or int] or Tuple[datetime.datetime or int]
        datetimes/timestamps to run the analysis on. The first value will be used as reference, unless `dt0` is passed.
    attribute_names : List[str] or Tuple[str]
        List or tuple filled with attribute names that are to be compared.
        
    Other Parameters
    ----------------
    item_ids : List[int], optional, global_variables.osrs.npy_items by default
        List of item_ids that should be included in the analysis
    timespan : int, optional, 86400 by default
        Temporal distance between two datapoints; t0 is set to the timestamp related to an element in `datetimes`, while
        t1 is set to t0+86400 (not inclusive)
    dt0 : datetime.datetime, optional, datetimes[0] by default
        The datetime to use as reference timestamp. If undefined, it will be set to the first value of `datetimes`.
    round_timestamp : bool, optional, True by default
        If True, timestamps in `datetimes` will be floored using `timespan`
    n_decimals : int, optional, 2 by default
        Amount of decimals floating points should be rounded down to
    

    Returns
    -------
    results_tupled : Dict[Tuple[int, str], List[List[El]]]
        Dict with results, of which the keys consist of an item_id, attribute_name and timestamp

    """
    if len(datetimes) < 2:
        raise ValueError("Unable to run a comparative analysis on less than two datapoints...")
    
    timespan = 86400 if kwargs.get('timespan') is None else kwargs['timespan']
    item_ids = go.npy_items if kwargs.get('item_ids') is None else kwargs['item_ids']
    round_timestamp = timespan if kwargs.get('round_timestamp') is None else int(kwargs['round_timestamp'])*timespan
    n_decimals = 2 if kwargs.get('n_decimals') is None else kwargs['n_decimals']
    
    try:
        dt0 = kwargs['dt0']
    except KeyError:
        dt0 = datetimes[0]
        datetimes = datetimes[1:]
    
    if round_timestamp > 0:
        t0 = int(ut.utc_dt_unix(dt0)/round_timestamp)*round_timestamp
        dt0 = ut.utc_unix_dt(t0)
        timestamps = [int(ut.utc_dt_unix(next_dt)/round_timestamp)*round_timestamp for next_dt in datetimes]
    else:
        t0 = ut.utc_dt_unix(dt0)
        timestamps = [ut.utc_dt_unix(next_dt) for next_dt in datetimes]
        
    results = {}
    results_tupled = {}
    
    # El.avg captures the average value across all datapoints for a combination of attribute/item within a time interval
    # El.relative is El.avg relative to the average value of the reference datapoints.
    
    def _round(n):
        """ Round `n` down to the `n_decimals` specified in kwargs """
        return round(n, n_decimals) if math.modf(n)[0] != 0 else int(n)
    for i in item_ids:
        item_results, references = {}, []
        ref_dps = ud.get_datapoints(i, t0, t0+timespan)
        for a in attribute_names:
            ref_values = El(i, a, t0, t0+timespan, ud.avg_dp_value(ref_dps, a, True, 3), 1.0)
            references.append(ref_values)
        ts_results = [[references[a_idx]] for a_idx in range(len(attribute_names))]
        for t_idx, _t0 in enumerate(timestamps):
            t1 = _t0 + timespan
            
            dps: List[ud.Dp] = ud.get_datapoints(i, _t0, t1)
            
            for a_idx, a in enumerate(attribute_names):
                avg = ud.avg_dp_value(dps, a, True, 3)
                # el =
                ts_results[a_idx].append(El(i, a, _t0, t1, _round(avg), _round(avg/references[a_idx].avg)))
            item_results[_t0] = ts_results
        for a_idx, a in enumerate(attribute_names):
            results_tupled[(i, a)] = ts_results[a_idx]
        results[i] = item_results
    print(timestamps)
    return results_tupled


def check_target_prices() -> (float, float):
    """
    Check how often the price of Item `i` has exceeded the configured target prices for this item in unix timestamp
    interval `t0`, `t1`. Override a configured target price if it is passed.
    # TODO refine method -> check neighbouring datapoints for missing price data?
    # TODO move to data_analysis?
    
    
    

    Returns
    -------
    fraction_exceeded : (float, float)
        Fraction of timestamps in which eithe
    
    Raises
    ------
    ValueError
        If both the target_buy and target_sell price are None, a ValueError is raised.
    
    Notes
    -----
    This method uses avg5m data to check whether target prices were exceeded or not. However, if no trades have been
    registered in a 5-minute interval, the respective price and volume are set to 0. This does not necessarily mean the
    prices have not been exceeded, it simply means there is no data to confirm that. The `min_n` parameter was added to
    alter this behaviour; it ensures that the % target prices exceeded is a fraction of at least this many datapoints.
    Suppose there are 10000 datapoints in the within the given interval and 100 of them have a price of > 1000, while
    the rest of the prices is equal to 0. For a `target_sell` of 900 and a `min_n` of 0, this would mean `target_sell`
    was exceeded in 100% of the non-zero price datapoints.
    However, if `min_n`=1.0, `target_sell` was exceeded 100/10000 = 1% of the datapoints.
    
    
    
    
    
    
    """
    # TODO Implement using SQLite
    raise NotImplementedError


def estimate_buy_price():
    """
    Estimate buy price by comparing various 4h intervals of the item of the last `n_days` days. The lowest buy prices
    for each 4h interval will be listed


    Parameters
    ----------
    item_id : int
        id of the item for which the buy price is to be estimated
    n_days : int, optional, 2 by default
        The amount of days to take into account when estimating the buy price

    Returns
    -------
        The recommended buy price for the item, given `n_days` worth of data

    """
    # TODO
    raise NotImplementedError


def estimate_daily_profit():
    """
    Estimate daily profit for an item, given its buy and sell prices. Using indices derived with the gap interval, the
    difference between the buy and sell price is multiplied by 20% of the wiki volume OR 6 times the buy_limit,
    whichever is lowest
    """
    # TODO
    raise NotImplementedError


def long_term_trade_analysis():
    """
    Analyze data for the given item and assess it's viablitity for long-term trading.
    Assessment should be based on;
    - Current price
    - Price development during past X days/weeks/months
    - All-time low+high price
    - Price affected by update -> best ignore?
    - Daily volume
    """
    
    # TODO
    raise NotImplementedError


def item_dump_analysis():
    """
    Find item dumps in the given array and return them as a list.
    How is an item dump defined?
    - High gap between avg5m and wiki prices -> local extremes
    - Volume spike
    - Sorted values: difference between average top20% and bot20%
    Idea: With a chronologically sorted array, assign a rank to each value, with 0 being the lowest value in the array
    and n_unique_values being the highest value. Express the transition between temporally adjacent datapoints as an
    increase/decrease of value ranks. As for the ranking -> convert the values to a set of unique values and sort it.
    The index of the sorted list of unique values represents the rank of said value.
    Evaluating item dumps: how?
    Estimate profit using trade volumes, buy limit, buy price and sell price
    Estimate reliability by checking how frequently the item is dumped. Recurring item dumps should be favoured.

    Encoding item dumps: How should item dumps be encoded?
    - temporal values (hour/day_id/day-of-week/week_id)
    - price change
    - price gap
    - trade volume
    - buy limit

    Method is designed to analyze a timespan of multiple days/weeks
    """
    # a, columns, buy_limit = data.ar.copy(), data.column_list, data.buy_limit
    # df = pd.DataFrame(data=a, columns=columns)
    # # print(a)
    # df = df.loc[df['buy_price'] > 0]
    # wiki_price = int(np.average(df['wiki_price'].to_numpy()))
    # wiki_volume = int(np.average(df['wiki_volume'].to_numpy()))
    # roi_threshold = max(1000000, min([wiki_volume / 6, buy_limit]) * wiki_price * .05)
    #
    # # Rows of interest
    # roi = df.loc[df['flip_buy'] >= roi_threshold]
    # # roi = df.loc[df['flip_buy'] >= 1000000]
    # ratio_roi = len(roi) / len(df)
    # roi_flip_buy = roi['flip_buy'].to_numpy(dtype=np.uint64)
    # # print(ratio_roi, len(roi_flip_buy))
    # if len(roi_flip_buy) >= 10 and ratio_roi > .10:
    #     roi_flip_buy.sort()
    #     roi_flip_buy = int(
    #         np.average(roi_flip_buy[:int(len(roi_flip_buy) * .9)]) * (min(buy_limit, wiki_volume) / buy_limit))
    # else:
    #     return
    # return {'item_id': data.item_id, 'item_name': data.item_name, 'wiki_price': wiki_price, 'wiki_volume': wiki_volume,
    #         'buy_limit': buy_limit, 'ratio_roi': ratio_roi, 'roi_threshold': roi_threshold, 'avg_rfb': roi_flip_buy}
    
    # TODO
    raise NotImplementedError


def flip_analysis():
    """
    Analyze the given data for its potential for flipping. The timespan of the analysis can be altered using n_days,
    although it should be noted that this number should be kept as low as possible to ensure the results are
    interpretable
    """
    # a, columns, buy_limit = data.ar.copy(), data.column_list, data.buy_limit
    # df = pd.DataFrame(data=a, columns=columns)
    #
    # if isinstance(day_ids, list):
    #     df = pd.concat([df.loc[df['day_id'] == dat] for dat in day_ids])
    #     min_ts, max_ts = df['timestamp'].min(), df['timestamp'].max()
    # elif min_ts is not None and max_ts is not None:
    #     df = df.loc[(df['timestamp'] >= min_ts) & (df['timestamp'] < max_ts)]
    # else:
    #     raise ValueError("Input error in data_analysis.flip_analysis(); a time interval should be specified by passing "
    #                      "day_ids or by using a minimum and maximum timestamp")
    #
    # # print(ts_util.ts_to_dt(df['timestamp'].min(), utc_time=True),
    # #       ts_util.ts_to_dt(df['timestamp'].max(), utc_time=True))
    # i1, i2 = .3, .7
    #
    # wiki_price = int(np.average(df['wiki_price'].to_numpy()))
    # wiki_volume = int(np.average(df['wiki_volume'].to_numpy()))
    #
    # if wiki_price > 10000000 or wiki_volume * wiki_price < 500000:
    #     return
    #
    # sell_prices, buy_prices = df['sell_price'].to_numpy(), df['buy_price'].to_numpy()
    # sell_prices, buy_prices = np.sort(sell_prices[sell_prices != 0]), np.sort(buy_prices[buy_prices != 0])
    # # weighted_price_gap = sp_bp_gap * min(wiki_volume * .2, buy_limit * 6)
    # try:
    #     avg_bot20_buy = int(get_sorted_interval_averages(a=buy_prices, intervals=[(0.05, .3)]).get((0.05, .3)))
    #     avg_top20_sell = int(get_sorted_interval_averages(a=sell_prices, intervals=[(.7, .95)]).get((.7, .95)))
    # except ValueError:
    #     return
    # sp_bp_gap = sell_prices[int(np.ceil(len(sell_prices) * i2))] - buy_prices[int(np.ceil(len(buy_prices) * i1))]
    #
    # # Volume coefficient; (very) rough estimate of how many items one can expect to buy throughout the day
    # vc, vc_4h = min(int(wiki_volume * .2), buy_limit * 6), min(int(wiki_volume * .1), buy_limit)
    # return {'item_id': data.item_id, 'item_name': data.item_name, 'wiki_price': wiki_price, 'wiki_volume': wiki_volume,
    #         'buy_limit': buy_limit, 'min_ts': min_ts, 'max_ts': max_ts,
    #         'flip_est': int(sp_bp_gap * vc_4h), 'avg_bot20': avg_bot20_buy,
    #         'avg_top20': avg_top20_sell,
    #         'delta_sell_buy': vc * int(avg_top20_sell - avg_bot20_buy - int(wiki_price * .01))}
    
    # TODO
    raise NotImplementedError


def get_sorted_interval_averages(a: np.ndarray, intervals: list, remove_zeros: bool = True):
    """
    Get the averages per interval within the passed np array. The list of intervals should consist of tuples of size 2
    where the first element indicates the first index and the second element indicates the second index. The tuple
    elements should be defined as percentages, e.g. interval (0, .2) translates as all indices between index 0 and the
    index equal to 20% of the elements.
    Intervals should be passed as a list of tuples. Note that the array will be sorted before computing averages.
    :param a: The array that should be processed
    :param intervals: Tuples indicating which averages should be computed
    :param remove_zeros: True if zeros should be removed from the array before computing the averages
    :return: A dict with an average of all elements within the corresponding interval for the given array
    """
    if remove_zeros:
        a = a[a != 0]
    a.sort()
    n = len(a)
    return {i: np.average(np.sort(a[int(np.ceil(n * i[0])):int(np.ceil(n * i[1]))])) for i in intervals}


# def target_prices_exceeded(i: NpyArray, t_min: int = int(time.time() - 86400), t_max: int = int(time.time())):
#     """
#     Determine the fraction of price datapoints that exceed the target buy and target sell price of the given item.
#     If no target_prices are set, it will return 0,  1 as the default target prices are 0
#     :param i: Loaded numpy archive for a specific item
#     :param t_min: Lower bound timestamp
#     :param t_max: Upper bound timestamp
#     :return: tuple of % buy/sell prices smaller/greater or equal to target buy/sell prices
#     """
#     df = pd.DataFrame([{'timestamp': ts, 'buy_price': bp, 'sell_price': sp}
#                        for ts, bp, sp in zip(i.timestamp, i.buy_price, i.sell_price)])
#     df = df.loc[(df['timestamp'] <= t_max) & (df['timestamp'] >= t_min)]
#     df_b, df_s = df.loc[df['buy_price'] > 0], df.loc[df['sell_price'] > 0]
#     b_exceeded = len(df_b.loc[df_b['buy_price'] <= i.target_buy]) / len(df_b)
#     s_exceeded = len(df_s.loc[df_s['sell_price'] >= i.target_sell]) / len(df_s)
#     # print(i.target_buy, b_exceeded)
#     # print(i.target_sell, s_exceeded)
#     return b_exceeded, s_exceeded


def check_target_prices(i: Item, t0: int or float, t1: int or float, target_buy: int = None, target_sell: int = None,
                        min_n: float = 1.0) -> (float, float):
    """
    Check how often the price of Item `i` has exceeded the configured target prices for this item in unix timestamp
    interval `t0`, `t1`. Override a configured target price if it is passed.
    # TODO refine method -> check neighbouring datapoints for missing price data?
    # TODO move to data_analysis?

    Parameters
    ----------
    i : Item
        The Item for which the target prices / timeseries data is to be evaluated
    t0: int or float
        The lower bound of the timestamp interval the prices are to be checked for
    t1: int or float
        The upper bound of the timestamp interval the prices are to be checked for
    target_buy: int, optional, None by default
        If passed, override the database target buy price with this value
    target_sell: int, optional, None by default
        If passed, override the database target sell price with this value
    min_n: float, optional, 1.0 by default
        The minimum value for n, which is the amount of non-zero prices found within the interval. See Notes for a more
        detailed explanation.


    Returns
    -------
    fraction_exceeded : (float, float)
        Fraction of timestamps in which eithe

    Raises
    ------
    ValueError
        If both the target_buy and target_sell price are None, a ValueError is raised.

    Notes
    -----
    This method uses avg5m data to check whether target prices were exceeded or not. However, if no trades have been
    registered in a 5-minute interval, the respective price and volume are set to 0. This does not necessarily mean the
    prices have not been exceeded, it simply means there is no data to confirm that. The `min_n` parameter was added to
    alter this behaviour; it ensures that the % target prices exceeded is a fraction of at least this many datapoints.
    Suppose there are 10000 datapoints in the within the given interval and 100 of them have a price of > 1000, while
    the rest of the prices is equal to 0. For a `target_sell` of 900 and a `min_n` of 0, this would mean `target_sell`
    was exceeded in 100% of the non-zero price datapoints.
    However, if `min_n`=1.0, `target_sell` was exceeded 100/10000 = 1% of the datapoints.






    """
    # if target_buy is None:
    #     target_buy = i.target_buy
    # if target_sell is None:
    #     target_sell = i.target_sell
    # has_target_buy, has_target_sell = target_buy > 0, target_sell > 0
    # if not has_target_buy and not has_target_sell:
    #     raise ValueError('Unable to assess whether target prices have been exceeded if they are not defined...')
    #
    # sell, buy = [], []
    # n = 0
    # data = avg5m.load_as_np(item_id=i.item_id, t0=target_prices_eval_t0, t1=eval_t1)
    # data = avg5m.load_as_np(item_id=i.item_id, t0=t0, t1=t1)
    # avg5m = timeseries.Avg5m()
    # timestamps, prices = np.append(data.timestamp, data.timestamp), np.append(data.buy_price, data.sell_price)
    # buy_prices, sell_prices = {}, {}
    #
    # n = 0
    # n = len(data.timestamp)
    # for ts, buy, sell in zip(data.timestamp, data.buy_price, data.sell_price):
    #     bp, sp = min(buy, sell), max(buy, sell)
    #     if bp == 0:
    #         bp = sp
    #     s = None
    #     if bp < target_buy:
    #         buy_prices[ts] = bp
    #         s = f'buy: {bp} ({target_buy})  '
    #     if sp > target_sell:
    #         sell_prices[ts] = sp
    #         s = f'sell: {sp} ({target_sell})  ' if s is None else s + f'sell: {sp} ({target_sell})  '
    #     if s is not None:
    #         print(f'[{ut.loc_unix_dt(ts)}]  {s}')
    # sell, buy = len(sell_prices.keys()), len(buy_prices.keys())
    # print(len(list(buy_prices.keys())), len(list(sell_prices.keys())))
    #
    # for ts in data.timestamp:
    #     bp, sp = buy_prices.get(ts), sell_prices.get(ts)
    #     if bp is not None or sp is not None:
    #         s = f'{ut.loc_unix_dt(ts)}: '
    #         if bp is not None:
    #             s += f'buy: {bp} ({target_buy})  '
    #         if sp is not None:
    #             s += f'sell: {sp} ({target_sell})  '
    #         print(s[:-2])
    # print(
    #     f'n_buy: {buy}/{n} / n_sell: {sell}/{n}')  # | n_missing: {n_missing}/{n_total} ({n_missing/n_total*100:.1f}%)')
    # return round(buy / n, 4), round(sell / n, 4)
    
    # TODO
    raise NotImplementedError


def activity_analysis():
    """
    Analyze the realtime entries for the given item per hour for a 24-hour timeframe. Resulting data provides an
    indication for the trading activity throughout the day. Note that this analysis involves realtime entries, which are
    found to have gaps in data if the data was not scraped during a given minute, which can occur for a variety of
    reasons. Analysis results values are elaborated in the Notes section.
    
    Parameters
    ----------
    item : model_item.NpyArray
        The loaded NpyArray object for the item that is to be analyzed
    timestamp : int
        Timestamp for the day that is to be analyzed.
    full_day : bool, optional, True by default
        Indicates whether the timestamp should be rounded down to a 12am UTC time or to the nearest hour.

    Returns
    -------
    results : list of dicts
        A list with a dict containing results for each hourly interval
    
    Notes
    -----
    The activity is analyzed in various ways in 1-hour intervals across the 24h timeframe. The resulting values have
    the following meaning;
    %_volume_traded: Summed averaged 5-minute trading volume for this hour relative to total volume for the day. Note
    that this value can be somewhat misleading as this reflects the logged volume instead of the actual volume. In some
    cases this value can be heavily inflated.
    %_rt_[buy/sell]: Amount of entries logged within this hour relative to the amount of entries logged for the full 24h
    timeframe. Keep in mind that a logged entry means the realtime price was updated at least once during a minute, this
    provides no indication as to how often it was updated during that minute.
    [buy/sell]_activity: A score between 0-1 that reflects how many entries have been logged. The score ranges from not
    updated during the hour (0) to updated at least once every minute throughout the hour (1)
    In order to truly grasp the meaning of the underlying values, you should check the resulting values of items that
    are known to be traded (in)frequently
    
    
    """
    # timestamp = timestamp - timestamp % 86400
    # timestamp = timestamp - timestamp % (86400 if full_day else 3600)
    # t_end = timestamp + 86400
    
    # Reduce arrays to data for this particular day
    # ts_mask = np.nonzero((item.timestamp >= timestamp) & (t_end > item.timestamp))
    
    # timestamps = item.timestamp[ts_mask]
    
    # Avg5m volume arrays for this interval
    # buy, sell, total = item.buy_volume[ts_mask], item.sell_volume[ts_mask], item.avg5m_volume[ts_mask]
    # sum_b, sum_s, sum_t = np.sum(buy), np.sum(sell), np.sum(total)
    
    # As wiki volume, use the first wiki volume logged for this day
    # print(item.wiki_volume)
    # max_s, max_b = 0, 0
    
    # results = []
    # dt_0 = ts_to_dt(timestamp=timestamp)
    
    # Realtime entries use different timestamps and should therefore be loaded separately
    # df_s = load_realtime_entries(item_id=item.item_id, t0=timestamp, t1=t_end)
    # df_b, df_s = df_s.loc[df_s['is_sale'] == 0], df_s.loc[df_s['is_sale'] == 1]
    # n_rt_s, n_rt_b = len(df_s), len(df_b)
    # for h in range(24):
    #     t0 = timestamp + h * 3600
    #     t1 = t0 + 3600
    #     ts_mask = np.nonzero((timestamps >= t0) & (t1 > timestamps))
    #     b, s, t = buy[ts_mask], sell[ts_mask], total[ts_mask]
    #     temp_dfs = df_s.loc[(df_s['timestamp'] >= t0) & (t1 > df_s['timestamp']) & (df_s['is_sale'] == 1)]
    #     temp_dfb = df_b.loc[(df_b['timestamp'] >= t0) & (t1 > df_b['timestamp']) & (df_b['is_sale'] == 0)]
    #     if len(temp_dfs) > max_s:
    #         max_s = len(temp_dfs)
    #     if len(temp_dfb) > max_b:
    #         max_b = len(temp_dfb)
    #
    #     # hour refers to the interval start
    #     r = {'hour': ts_to_dt(t0, utc_time=True).hour}
    #     # r['b'] = len(b)
    #     # r['nz_buy'] = len(b[np.nonzero(b > 0)])
    #     # r['nz_sell'] = len(s[np.nonzero(s > 0)])
    #     # r['nz_total'] = len(t[np.nonzero(t > 0)])
    #
    #     # This indicates the % of volume logged within the hour relative to the total logged volume for this day
    #     r['%_volume_traded'] = np.round(np.sum(t) / sum_t, decimals=4) if sum_t > 0 else 0
    #
    #     # Amount of entries logged within the hour relative to the amount of entries logged within this day
    #     r['%_rt_sell'] = np.round(len(temp_dfs) / n_rt_s, decimals=4) if n_rt_s > 0 else 0
    #     r['%_rt_buy'] = np.round(len(temp_dfb) / n_rt_b, decimals=4) if n_rt_b > 0 else 0
    #
    #     # A value of 1 indicates the realtime buy and sell prices were updated at least once every minute for the hour
    #     r['buy_activity'] = np.round(len(temp_dfb)/60, 4)
    #     r['sell_activity'] = np.round(len(temp_dfs)/60, 4)
    #     results.append(r)
    #     print(ts_to_dt(t0, utc_time=True), r)
    # return results
    
    # TODO
    raise NotImplementedError


def get_saturated_items():
    """
    Fetch a list of items of which the buy offers are estimated to be saturated, indicating a recent dump or an
    unusually large amount of items being sold within the past 4 hours
    
    Parameters
    ----------
    item_ids :
    threshold_price :

    Returns
    -------

    """
    # output, ct = [], int(time.time())
    # con = sqlite3.connect(p.f_db_timeseries)
    # cursor = con.cursor()
    # for item_id in item_ids:
    #     i, cur = NpyArray(item_id), {'item_id': item_id, 'name': go.id_name[item_id]}
    #     cur['buy_limit'] = i.buy_limit
    #     cur['volume'] = int(np.average(i.wiki_volume[-7:]))
    #     if cur.get('volume') < i.buy_limit * 10:
    #         print(f'\tSkipped {go.id_name[item_id]}')
    #         continue
    #     try:
    #
    #         rt_prices = load_realtime_entries(item_id=item_id, t0=ct-86400, t1=ct, c=cursor)
    #         for h_tag, timespan in zip(['4h', '24h'], [14400, 86400]):
    #             t0 = ct-timespan
    #             b = i.buy_price[np.nonzero((i.timestamp >= t0) & (i.buy_price > 0))]
    #             s = i.sell_price[np.nonzero((i.timestamp >= t0) & (i.sell_price > 0))]
    #             b_s = np.sort(np.append(b, s))
    #             rt_b = rt_prices.loc[~(rt_prices['is_sale']) & (rt_prices['timestamp'] > t0)].to_numpy()
    #             rt_s = rt_prices.loc[(rt_prices['is_sale']) & (rt_prices['timestamp'] > t0)].to_numpy()
    #             cur.update({
    #                 f'p_avg_{h_tag}': int(np.average(b_s)),
    #                 f's_avg_{h_tag}': int(np.average(s)),
    #                 f'b_avg_{h_tag}': int(np.average(b)),
    #                 f'b_std_{h_tag}': np.std(b),
    #                 f'p_min_{h_tag}': int(b_s[0]),
    #                 f'p_max_{h_tag}': int(b_s[-1]),
    #                 f'delta_p_{h_tag}': int(b_s[-1]-b_s[0]-int(b_s[-1]*.01)) * i.buy_limit,
    #                 f'n_rt_{h_tag}': len(rt_s) + len(rt_b),
    #                 f'rt_b_std_{h_tag}': np.std(rt_b),
    #                 f'rt_s_std_{h_tag}': np.std(rt_s)
    #             })
    #     except ValueError:
    #         print(f'ValueError for item {go.id_name[item_id]}')
    #     finally:
    #         if len(output) % 25 == 0:
    #             print(f'Processed {len(output)}/{len(item_ids)} items')
    #         output.append(cur)
    # pd.DataFrame(output).to_csv(p.dir_output+'saturated_items.csv',index=False)
    
    # TODO
    raise NotImplementedError
    

if __name__ == '__main__':
    dt0 = datetime.datetime(2024, 6, 1)
    test = compare_npy_data(
        datetimes=[dt0+datetime.timedelta(days=n) for n in range(61)],
        # datetimes=[
        #     datetime.datetime(2024, 7, 18),
        #     datetime.datetime(2024, 7, 25),
        #     datetime.datetime(2024, 8, 1),
        #     datetime.datetime(2024, 8, 8),
        #     datetime.datetime(2024, 8, 15),
        #     datetime.datetime(2024, 8, 22),
        #     datetime.datetime(2024, 8, 29),
        #     datetime.datetime(2024, 9, 5)],
        attribute_names=['buy_price', 'sell_price', 'wiki_price', 'wiki_volume'],
        item_ids=[2, 62, 4697, 5952, 10033]
    )
    wd = gp.dir_compare_npy_analysis
    if not os.path.exists(wd):
        os.mkdir(wd)
    df_rows = []
    item_id = None
    _wd = None
    attribute_name = None
    for k, v in test.items():
        if item_id is not None and item_id != k[0]:
            if attribute_name is not None:
                pd.DataFrame(df_rows).to_csv(_wd + f"{attribute_name}.csv", index=False)
            # pd.DataFrame(df_rows).to_csv(_wd+f"{attribute_name}.csv", index=False)
            # df_rows = []
            
            # if not os.path.exists(_wd):
                # os.mkdir(_wd)
        item_id = k[0]
        _wd = wd + f"{go.id_name[item_id]}/"
        if not os.path.exists(_wd):
            os.mkdir(_wd)
        print(go.id_name[k[0]], k[1])
        previous = None
        # df_rows = []
        # attribute_name = None
        for el in v:
            if not isinstance(el, El):
                raise RuntimeError
            if attribute_name is not None and attribute_name != el.attribute_name:
                pd.DataFrame(df_rows).to_csv(_wd + f"{attribute_name}.csv", index=False)
                print(_wd + f"{attribute_name}.csv")
                df_rows = []
            df_rows.append(el.__repr_dict__(previous_row=previous))
            attribute_name = el.attribute_name
            previous = el
            # print(el.__repr_dict__())
            print(f'\t{el}')
        # print(df_rows)
        print('\n')
    
    # print(f)
    pd.DataFrame(df_rows).to_csv(_wd+f"{attribute_name}.csv", index=False)
    
    