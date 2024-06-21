"""
This module contains various graph-related utility methods


"""
import math
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

import util.unix_time as ut
import util.str_formats as fmt
import global_variables.configurations as gc
import global_variables.values as gv


def compute_graph_statistics(x_axis, y_axis, value_name):
    """ Compute basic statistics of a graph """
    assert len(x_axis) == len(y_axis)
    y_sorted = np.sort(y_axis)
    n = len(y_axis)
    non_zero = y_axis[np.nonzero(y_axis != 0)]
    print(f'Graph statistics for {value_name} ({(min(x_axis))} - {ut.loc_unix_dt(max(x_axis))})')
    avg = np.average(y_axis)
    output = {
        # using non-zero values: lowest, idx=25%, idx=50%, idx=75%, highest
        'y_distribution': (min(non_zero), y_sorted[int(.25*n)], y_sorted[int(.5*n)], y_sorted[int(.75*n)], max(y_axis)),
        'y_average': avg,
        'std': np.std(y_axis),
        'n_below_avg': len(non_zero[np.nonzero(non_zero < avg)])/len(non_zero),
        'n_above_avg': len(non_zero[np.nonzero(non_zero > avg)])/len(non_zero),
        'n_missing': n-len(non_zero),
        'n_total': n,
    }
    
    
    print(output)


def xaxis_dow_format(ts: int, tick_id=None, n_chars: int = 3):
    """ Convert the amount of seconds on the x-axis to a day-of-week (e.g. 0-86399=monday) """
    try:
        # return dow[utc_ts_to_dt(timestamp=ts + 604800).weekday()][:3]
        return gv.dow[ts % 604800 // 86400][:n_chars]
    except IndexError:
        return ''


def xaxis_hod_format(hod: int, tick_id=None):
    """ Format the hour of day for the x-axis """
    try:
        return f'{hod // 3600:0>2}h'
    except IndexError:
        return ''


def xaxis_dmyh_format(timestamp: int, arg=None):
    """ Format the timestamps in dd-mm-yy Hh format for the x-axis """
    # print(arg)
    return fmt.unix_(unix_ts=timestamp, fmt_str='%d-%m-%Y %Hh')


def major_format_price_non_abbreviated(price, tick_id=None):
    """ Return the price as a non-abbreviated string without decimals, with a ',' every 3 digits """
    temp = f'{price:.0f}'
    n_chars, output = len(temp), ''
    delimiters = [n_chars - (1 + i) * 3 for i in range(n_chars // 3)] if n_chars > 3 else []
    
    for i in range(n_chars):
        # i += 1
        try:
            c = temp[i]
            if i in delimiters:
                output += f',{c}'
            else:
                output += c
        except IndexError:
            break
    return output[1:] if output[0] == ',' else output


def major_format_price(p, tick_id=None):
    """ Format the price for ticks on the y-axis. """
    return fmt.number(int(p), max_length=8, max_decimals=1)


def major_format_price_taxed(price: int, tick_id=None):
    """ Return the formatted price, minus the 1%/5m tax """
    return fmt.number(int(price - math.floor(price * .01)), max_length=8, max_decimals=1)


def major_format_percentage(p, tick_id=None):
    """ Format the price for ticks on the y-axis. """
    return f'{p * 100:.1f}%'


def configure_vertical_plots(vplot_frequencies, y_value_range, x_value_range):
    """ Prepare vertical plotted lines that indicate a fixed time interval on the graph. """
    if isinstance(vplot_frequencies, int):
        vplot_frequencies = [vplot_frequencies]
    vplot_frequencies.sort()
    patches = {}
    vpf_a, vpf = .7, vplot_frequencies[0]
    rgba = [.8, .8, .8, vpf_a]
    label = f'{vpf // 3600}h intervals' if vpf < 86400 else f'{vpf // 86400}-day intervals'
    patches[tuple(rgba)] = mpatches.Patch(color=rgba, label=label)
    
    # Axis spans for horizontal and vertical plots within the graph
    y_span = [min(y_value_range) * .95, max(y_value_range * 1.05)]
    plots = []
    for vpf in vplot_frequencies:
        cur_x = min(x_value_range)
        cur_x = int(cur_x - cur_x % vpf)
        max_x = max(x_value_range)
        while cur_x < max_x:
            plots.append({
                'x': [cur_x for _ in y_span],
                'y': y_span,
                'c': (.8, .8, .8, vpf_a),
                'w': .8 * (1 + vplot_frequencies.index(vpf))
            })
            cur_x += vpf
        vpf_a += .15
    return plots, patches


def get_vplot_timespans(delta_t: int):
    """ Generate up to two fixed intervals to plot vertical lines for """
    return [vp_value for vp_value in gc.timeseries_intervals if delta_t // 7.5 < vp_value * 2.5 < delta_t]
