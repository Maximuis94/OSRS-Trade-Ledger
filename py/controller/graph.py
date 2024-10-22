"""
This module contains the model of a graph.

TODO Classes/Methods need to be re-designed
"""
from global_variables.importer import *
import collections
import datetime
import time
from collections.abc import Collection, Iterable
from copy import copy, deepcopy

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import patches as mpatches
from import_parent_folder import recursive_import

import str_formats
import ts_util
from model_item import NpyArray
from ge_util import remap_item
from global_values import dow, int32_max, realtime_prices, name_id, delta_t_utc
from graph_util import xaxis_dow_format, major_format_percentage, xaxis_hod_format, configure_vertical_plots, \
    major_format_price, major_format_price_non_abbreviated, major_format_price_taxed, xaxis_dmyh_format
# import str_formats
from str_formats import format_ts, format_n
from ts_util import ts_to_dt, utc_ts_to_dt
del recursive_import


class Graph:
    def __init__(self, x, y, color: tuple = (0, 0, 0, .3), patch_label: str = None, line_width: float = None,
                 line_marker: str = None, line_style: str = None,
                 value_range_patch_label: bool = False):
        self.x_values = x
        self.y_values = y
        self.color = color
        if isinstance(patch_label, str) and value_range_patch_label:
            patch_label += f" {format_n(min(self.y_values))} - {format_n(max(self.y_values))}"
        self.patch = mpatches.Patch(color=self.color, label=patch_label)
        self.line_width = line_width
        self.line_marker = line_marker
        self.line_style = line_style
    
    def plot_graph(self, axs: plt.Axes, patches: dict = None):
        """ Plot the available graph data to `axs`, and add a legendpatch to the legend, if available. """
        axs.plot(self.x_values, self.y_values, color=self.color, linewidth=self.line_width, linestyle=self.line_style)
        patches[self.color] = self.patch
        return axs, patches


class Axis:
    def __init__(self, label: str = '', label_font: str = None, font_size: float = 10.0, minor_format: callable = None,
                 major_format: callable = None, ticks: Collection = None):
        self.label = label,
        self.font_dict = {
            'fontsize': font_size,
            'fontstyle': label_font
        }
        self.minor_format = minor_format if minor_format is not None else major_format
        self.major_format = major_format if major_format is not None else minor_format
        self.ticks = ticks
    
    def set_y_config(self, axs: plt.Axes, ylim: tuple = None) -> plt.Axes:
        if self.ticks is not None:
            axs.set_ticks(self.ticks)
        # if isinstance(ylim, tuple):
        #     axs.set_
        axs.yaxis.set_major_formatter(formatter=self.major_format)
        axs.yaxis.set_minor_formatter(formatter=self.minor_format)
        # axs.yaxis.set_label_text(label=self.label, fontdict=self.font_dict)
        # axs.yaxis.set_ticks(self.ticks)
        
        # if ylim is not None:
        #     axs.set_ylim(ymin=min(ylim), ymax=max(ylim))
        
        return axs


class PricesGraph:
    def __init__(self, item: NpyArray, t0, t1, y_values, axs_gen=None, output_file: str = None):
        """
        Graph designed specifically for the item prices interface. Displays plots of specified `y-values` within the
        specified unix timestamp interval `t0` and `t1`, using data from the given NpyArray `item`.

        Parameters
        ----------
        item : NpyArray
            NpyArray object with data that is to be inserted into the graph. Can be changed when calling plot_graph()
        t0 : int
            Starting timestamp of the graph
        t1 : int
            Ending timestamp of the graph
        y_values : list, optional, None by default
            A list of y-values that are to be plotted. Can be changed when calling plot_graph()

        Methods
        -------
        plot_graph(p, t0: int, t1: int, item: NpyArray = None, y_values: list = None, vplot_frequencies=None)
            Plot the graph using the args and object attributes as graph configs into matplotlib.Axis `p` and returns it
        """
        self.item = item
        self.t0, self.t1 = t0, t1
        self.title = ''
        
        self.patches = None
        
        self.y_values = [y_values] if y_values is not None and not isinstance(y_values, list) else y_values
        
        # buy_price and sell_price are temporary hard-coded placeholders to get a working version
        self.plots, self.buy_price, self.sell_price = {}, np.array(0), np.array(0)
        self.x_axis_hplot, self.y_axis_vplot = [0, 0], [0, 0]
        self.axs_gen = axs_gen
        self.output_file = output_file
    
    def generate_graph(self, p, t0: int, t1: int, item: NpyArray = None, vplot_frequencies=None,
                       t_formatter: str = '%d-%m-%y %H:%M', graph_generator: callable = None):
        """ Generate+insert graph plots and other configs of this graph into matplotlib Axis `p` and return it """
        if isinstance(item, NpyArray):
            self.item = item
        if graph_generator is not None:
            self.axs_gen = graph_generator
        return self.axs_gen(axs=p, item=self.item, t0=t0, t1=t1, vplot_frequencies=vplot_frequencies)


class TimeSeriesPlot:
    def __init__(self, item: NpyArray, y_config: Axis = None, ylim_multipliers: Collection = (1, 1),
                 vline_graph: Graph = None):
        """
        Class that serves as a blueprint for plotting timeseries graphs and streamlining plotting data. The x-axis is
        pre-defined as a list of unix timestamps that

        todo: integrate this class in existing graph generation pipelines
        """
        # self.item_name = item if isinstance(item, str) else id_name[item)
        # self.item_id = item if isinstance(item, int) else name_id.get(item)
        # plt.Figure(label='1')
        # _, self.axs = plt.subplots(1, 1)
        self.graphs, self.patches = [], {}
        self.vline, self.spikes = vline_graph, []
        print(item.wiki_volume)
        
        # x-Axis is always timestamps
        self.x_min, self.x_max = time.time(), 0
        self.x_label = 'Date'
        self.x_ticks = [0, .5, 1]
        self.x_lim = [time.time(), 0]
        self.x_format = utc_ts_to_dt
        
        self.y_min, self.y_max = int32_max, 0
        self.ylim_multipliers = ylim_multipliers
        if isinstance(y_config, Axis):
            self.y_axis = y_config
        
        self.title = f'{item.item_name} ({item.item_id})\n' \
                     f'__DT0__ - __DT1__\n' \
                     f'Price range: __P0__ - __P1__ (Daily volume: {format_n(np.average(item.wiki_volume[-7:]))})\n'
    
    def add_graphs(self, graphs):
        """ Add one or more graph(s) to the graphs list """
        if isinstance(graphs, Graph):
            graphs = [graphs]
        for g in graphs:
            if isinstance(g, Graph):
                self.graphs.append(g)
            else:
                raise TypeError(f'Unexpected value type {type(g)}; should be Graph()')
    
    def plot_figure(self, vlines: Iterable = None):
        self.fig, self.axs = plt.subplots(1, 1)
        patches = []
        title = ''
        for g in self.graphs:
            if isinstance(g, Graph):
                # print(g.__dict__)
                self.axs, patches = g.plot_graph(axs=self.axs, patches=self.patches)
                self.x_min, self.x_max = min(self.x_min, min(g.x_values)), max(self.x_max, max(g.x_values))
                self.y_min, self.y_max = min(self.y_min, min(g.y_values)), max(self.y_max, max(g.y_values))
        p0, p1 = int(self.y_min), int(self.y_max)
        self.y_min, self.y_max = int(self.y_min * self.ylim_multipliers[0]), int(self.y_max * self.ylim_multipliers[1])
        
        if isinstance(self.vline, Graph) and isinstance(vlines, Iterable):
            v_graph = deepcopy(self.vline)
            v_graph.y_values = self.y_min, self.y_max
            for next_x in vlines:
                g = copy(v_graph)
                g.x_values = [next_x, next_x]
                self.axs, self.patches = g.plot_graph(axs=self.axs, patches=self.patches)
                # print(g.__dict__)
        
        # patches = [i for _, i in list(self.patches.items())]
        # title = f'Item {o}'
        self.axs.legend(handles=[i for _, i in list(self.patches.items())])
        # title += f'{}'
        self.axs.set_xlim(self.x_min, self.x_max)
        self.axs.set_xticks([self.x_min, (self.x_max + self.x_min) // 2, self.x_max])
        self.axs.set_xlabel(xlabel=self.x_label)
        self.axs.xaxis.set_major_formatter(xaxis_dmyh_format)
        # self.axs.xaxis.set_minor_formatter(time_series_x_format)
        self.axs.tick_params(right=True, labelright=True)
        
        if isinstance(self.y_axis, Axis):
            # self.axs.yaxis = self.y_axis.set_y_config(deepcopy(self.axs), ylim=(self.y_min, self.y_max))
            if self.y_axis.minor_format is not None:
                self.axs.yaxis.set_minor_formatter(self.y_axis.minor_format)
            if self.y_axis.major_format is not None:
                self.axs.yaxis.set_major_formatter(self.y_axis.major_format)
            if self.y_axis.ticks is not None:
                self.axs.yaxis.set_ticks(self.y_axis.ticks)
            if self.y_axis.minor_format is not None:
                self.axs.yaxis.set_minor_formatter(self.y_axis.minor_format)
            self.axs.set_ylim(self.y_min, self.y_max)
        dt_f = '%d-%m %H:00'
        self.title = self.title.replace('__DT0__', format_ts(self.x_min, dt_f)).replace('__DT1__', format_ts(self.x_max,
                                                                                                             dt_f) + f' (~{(self.x_max - self.x_min) // 86400:.0f} days)')
        title = self.title.replace('__P0__', format_n(p0)).replace('__P1__', format_n(p1))
        self.axs.set_title(title)
        self.fig.set_figheight(7.5)
        self.fig.subplots_adjust(hspace=0.25, wspace=0.5)
        self.fig.set_figwidth(8)
        
        # self.axs.set_ylim(self.x_min, self.x_max)
        # self.axs.set_yticks([self.x_min, (self.x_max+self.x_min)//2, self.x_max])
        # self.axs.set_ylabel(ylabel=self.y_label)
        plt.show()
        return self.fig, self.axs


def price_graph(axs: plt.Axes, item: NpyArray, t0: int, t1: int, vplot_frequencies=None, t_formatter: str = None):
    """
    Plot the buy and sell price against the time for the given `item` within the given timespan `t0`, `t1`

    Parameters
    ----------
    axs: matplotlib.Axes
        The Axes object that will be plotted on
    item : NpyArray
        Loaded NpyArray for the item of interest
    t0 : int
        timestamp lower bound
    t1 : int
        timestamp upper bound

    Returns
    -------
    axs : matplotlib.Axes
        The given Axes object with additional data

    """
    plots, patches, t0, t1 = {}, {}, t0, t1
    y_values = ['buy_price', 'sell_price']
    
    if isinstance(item, NpyArray):
        item = item
    
    # Get relevant data
    price_plots = []
    for y_val in y_values:
        y = item.__dict__.get(y_val)
        np_values_mask = np.nonzero((item.timestamp >= t0) & (t1 > item.timestamp) & (y > 0))
        price_plots.append((item.timestamp[np_values_mask], y[np_values_mask]))
        plots[y_val] = item.timestamp[np_values_mask], y[np_values_mask]
    buy_price, sell_price = price_plots[0], price_plots[1]
    del price_plots
    all_prices = np.sort(np.append(buy_price[1], sell_price[1]))
    bp_24h = buy_price[1][len(buy_price[0][np.nonzero(buy_price[0] < t1 - 86400)]):]
    sp_24h = sell_price[1][len(sell_price[0][np.nonzero(sell_price[0] < t1 - 86400)]):]
    idx_range_p24h = len(np.append(bp_24h, sp_24h)) // 20
    p24h = list(np.sort(np.append(bp_24h, sp_24h)))[idx_range_p24h:-idx_range_p24h]
    
    # Plot buy and sell prices
    rgba, label = (.7, .35, .35), 'Buy prices'
    axs.plot(buy_price[0], buy_price[1], color=rgba)
    patches[rgba] = mpatches.Patch(color=rgba, label=label)
    
    rgba, label = (.35, .7, .35), 'Sell prices'
    axs.plot(sell_price[0], sell_price[1], color=rgba)
    patches[rgba] = mpatches.Patch(color=rgba, label=label)
    # axs.plot(x_axis_hplot, [min(all_prices), min(all_prices)], get_rgb(item.item_id%255))
    
    # Plot horizontal line indicating min and max prices
    x_values = []
    for cur_x in [plots.get(yv)[0] for yv in y_values]:
        x_values += list(cur_x)
    x_axis_hplot = [min(x_values), max(x_values)]
    
    # Add vertical lines indicating fixed timespans (e.g. vertical line every 3 hours)
    if vplot_frequencies is not None:
        vplots, vplot_patches = configure_vertical_plots(vplot_frequencies, all_prices, x_axis_hplot)
        # for vp in configure_vplots(vplot_frequencies=vplot_frequencies, y_values_merged=all_prices):
        patches.update(vplot_patches)
        for vp in vplots:
            axs.plot(vp.get('x'), vp.get('y'), color=vp.get('c'), linewidth=vp.get('w'))
    
    # Add min, max prices as horizontal lines
    min_price, max_price = min(all_prices), max(all_prices)
    
    rgba, label = (.3, .3, .7, 1.), f'Min/Max prices ({min_price}, {max_price})'
    axs.plot(x_axis_hplot, [min_price for _ in x_axis_hplot], color=rgba, linewidth=.9)
    axs.plot(x_axis_hplot, [max_price for _ in x_axis_hplot], color=rgba, linewidth=.9)
    # axs.plot(sell_price[0], sell_price[1], color=rgba)
    patches[rgba] = mpatches.Patch(color=rgba, label=label)
    
    # Configure additional settings
    cur_prices = realtime_prices.get(item.item_id)
    cs, cb = format_n(max(cur_prices), max_decimals=3, max_length=10), format_n(min(cur_prices), 3, 10)
    ts_format = '%d-%m %Hh'
    title = f"{item.item_name}  {format_ts(t0, ts_format)} - {format_ts(t1, ts_format)} " \
            f"buy limit: {format_n(item.buy_limit, 0)}\n" \
            f"Price ranges   [last 24h: {format_n(min(p24h), 2, 10)} - {format_n(max(p24h), 2, 10)}]   " \
            f"[Overall: {format_n(min_price, 2, 10)} - {format_n(max_price, 2, 10)}]\n" \
            f"Current prices [{cb} / {cs}]   " \
            f"Daily volume (last 7d): {format_n(int(np.average([item.wiki_volume[-1 + idx * -288] for idx in range(7)])))}"
    axs.set_title(title, fontdict={'fontsize': 9})
    if t_formatter is None:
        fmt_t = utc_ts_to_dt
    else:
        def fmt_t(t: int, fmt_str: str = t_formatter):
            """ Convert the given Unix timestamp according to the formatted string specified """
            return str_formats.format_ts(timestamp=t, str_format=fmt_str)
    axs.xaxis.set_minor_formatter(fmt_t)
    axs.xaxis.set_major_formatter(fmt_t)
    
    # Set 3 xticks; one left, one middle and one right
    x_ticks = [t0, (t0 + t1) / 2, t1]
    axs.set_xticks(x_ticks, labels=[str(ts_to_dt(timestamp=t, utc_time=False)) for t in x_ticks])
    axs.set_xlim(t0, t1)
    
    # Remove outliers so the scale doesn't get messed up
    ylim_prices = all_prices[int(len(all_prices) * .01):-int(len(all_prices) * .01)]
    axs.set_ylim(int(min(ylim_prices) - (max(ylim_prices) - min(ylim_prices)) * .1),
                 int(max(ylim_prices) + (max(ylim_prices) - min(ylim_prices)) * .1))
    
    # Add the legend if any patches have been defined
    if len(patches) > 0:
        axs.legend(handles=[p for _, p in patches.items()], fontsize='small')
    
    axs.xaxis.set_minor_formatter(ts_to_dt)
    axs.xaxis.set_major_formatter(utc_ts_to_dt)
    p2 = axs.twinx()
    p2.set_ylim(axs.get_ylim())
    p2.set_ylabel("Price (gp)")
    # p2.set_yticks(p.get_yticks())
    axs.set_ylabel(f'Price - tax')
    axs.set_xlabel(
        f'UTC | {-1 * delta_t_utc} hour{"s" if abs(delta_t_utc) != 1 else ""} relative to local time')
    # print(p.get_yticks())
    
    axs.yaxis.set_major_formatter(major_format_price_taxed)
    p2.yaxis.set_major_formatter(major_format_price_non_abbreviated)
    p2.yaxis.set_minor_formatter(major_format_price_non_abbreviated)
    
    return axs


def price_graph_by_dow(axs: plt.Axes, item: NpyArray, n_weeks: int = 4, colors: collections.abc.Mapping = None,
                       ts_cutoff: int = 14400, **kwargs) -> plt.Axes:
    """
    Plot the price for the given `item_id` as a series of plots per week. Each separate week is plotted as a different
    line, providing a comparison of how the price develops throughout different weeks.

    Parameters
    ----------
    axs: matplotlib.Axes
        The Axes object that will be plotted on
    item : NpyArray
        Loaded NpyArray for the item of interest
    n_weeks : int, optional, 4 by default
        The amount of weeks to plot, aside from the current week.

    Returns
    -------

    """
    t1 = int(time.time())
    t1 = t1 - t1 % ts_cutoff
    t0 = ts_to_dt(t1 - t1 % 604800 - 604800 * n_weeks) - datetime.timedelta(
        days=datetime.datetime.utcfromtimestamp(time.time()).weekday() + 1)
    ct = int(time.time())
    if colors is None:
        # colors = [((20+i*50)%255/255, (100+i*20)%255/255, (80+i*10)%255/255, 1.) for i in range(n_weeks+1)]
        # colors = ['r', 'g', 'c', 'm', 'y', 'b']
        colors = [(.8, 0, 0), (.1, .6, 0), (0, .7, .7), (.8, .1, .8), (.8, .8, .1), (.1, .1, .8)]
    t0_, x_axes, y_axes, patches, weeks_ago = t1 - t1 % 604800 - n_weeks * 604800, [], [], [], n_weeks + 1
    # print(dow[utc_ts_to_dt(int(time.time()-time.time()%608400)).weekday()])
    print(ts_to_dt(t0_), ts_to_dt(t1), time.time(), t0)
    y_merged, avg_prices = [], []
    # while t0_ < t1:
    # while weeks_ago >= 0:
    t0_ = ts_util.dt_to_ts(t0) - ts_util.delta_t
    if t0_ % 60 > 50:
        t0_ += (60 - t0_ % 60)
    # Note that these shifts are to ensure the x-axs starts at monday
    # This is due to the unix time % 604800 == 0 is set to a thursday
    t0_ -= 3 * 86400
    while t0_ < ct:
        t0_ = t0_ - t0_ % 604800 + 4 * 86400
        # print(ts_to_dt(t0_, utc_time=True))
        t1_ = t0_ + 604800
        # if weeks_ago == 0:
        #     avg_prices = list(item.buy_price[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) &
        #                                                 (item.buy_price > 0))]) + \
        #                  list(item.sell_price[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) &
        #                                                 (item.sell_price > 0))])
        #     break
        w_idx = (ct - t0_) // 604800
        c = colors[weeks_ago] if weeks_ago > 0 else colors[0]
        c = tuple(list(c) + [.7])
        patches.append(mpatches.Patch(color=c,
                                      label=f'Current week' if weeks_ago == 0 else
                                      f'{weeks_ago} week{"s" if weeks_ago > 1 else ""} ago'))
        for y in (item.buy_price, item.sell_price):
            x = item.timestamp[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) & (y > 0))] - t0_
            y = y[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) & (y > 0))]
            y_avg = int(np.average(y))
            avg_prices.append(y_avg)
            y = y / y_avg - 1
            axs.plot(x % 604800, y, color=c, linewidth=1.3 - weeks_ago * .15)
            y_merged += list(y)
        t0_ += 604800
        weeks_ago -= 1
    axs.legend(handles=patches)
    cp = realtime_prices.get(item.item_id)
    axs.set_title(f"{item.item_name} ({item.item_id}) Price/day of week\n"
                  f"Current prices: {format_n(min(cp), 10, 2)} {format_n(max(cp), 10, 2)} "
                  f"Timespan: {(utc_ts_to_dt(t1) - t0).days // 7} weeks")
    
    axs.set_xlim(0, 604800)
    axs.xaxis.set_major_formatter(xaxis_dow_format)
    axs.xaxis.set_ticks([day_id * 86400 for day_id in range(7)])
    
    y_merged.sort()
    y0, y1 = y_merged[int(len(y_merged) * .1)], y_merged[int(len(y_merged) * .9)]
    y_merged.sort()
    n_cutoff = int(len(y_merged) * .05)
    y1 = .025
    while max(abs(y_merged[n_cutoff]), abs(y_merged[-n_cutoff])) > y1 * .8:
        y1 += .025
    y0 = -1 * y1
    # y0, y1 = y_merged[n_cutoff]*(.8 if y0 > 0 else 1.2), y_merged[-n_cutoff]*(1.2 if y1 > 0 else .8)
    # axs.set_ylim(y0-(y1-y0)*.3, y1+(y1-y0)*.3)
    # axs.set_ylim(y0, y1)
    axs.set_ylim(y0, y1)
    
    # Rough estimation for potential profit; Average price (current week) - tax * (% difference between price at
    # rank 10% and rank 90%) * buy_limit OR 10% of avg daily volume, whichever is lower.
    n_cutoff *= 2
    volatility = (np.average(avg_prices) - int(np.average(avg_prices) * .01)) * (
                y_merged[-n_cutoff] - y_merged[n_cutoff]) * \
                 min(item.buy_limit, int(np.average(item.wiki_volume[-7:]) / 10))
    # print('Estimated profit', format_n(volatility, max_decimals=2))
    
    cur_time_vline = int(t1 - t0_) % 604800
    # axs.plot([cur_time_vline, cur_time_vline], [y0, y1], color='r', linewidth=1.0)
    axs.vlines(x=cur_time_vline, ymin=y0, ymax=y1, colors='r', label='Current timestamp', linestyles='dashed')
    axs.vlines(x=[day_id * 86400 for day_id in range(7)], ymin=y0, ymax=y1, colors=(0., 0., 0., 1.),
               label='Current timestamp', linewidths=1)
    
    axs.yaxis.set_major_formatter(major_format_percentage)
    patches.append(mpatches.Patch(color='r', label='Current timestamp'))
    
    return axs


def price_graph_by_hod(axs: plt.Axes, item: NpyArray, n_days: int = 7, colors: collections.abc.Mapping = None,
                       ts_cutoff: int = 14400, **kwargs) -> plt.Axes:
    """
    Plot the price for the given `item_id` as an averaged plot per hour of day.

    Parameters
    ----------
    axs : matplotlib.Axes
        The Axes object that will be plotted on
    item : NpyArray
        Loaded NpyArray for the item of interest
    n_days : int, optional, 7 by default
        The timespan of the plotted graphs in days.
    colors : collections.abc.Mapping, optional, None by default
        A sequence of colors to apply for the plots

    Returns
    -------
    axs : matplotlib.Axes
        The axs object that was passed, with the additional plots


    Notes
    -----
    The graph is divided in windows of 24 hours for which the average price is computed, each price within that 24 hour
    window is expressed as a price relative to that averaged price.
    Further design considerations include plotting multiple graphs per hour of day (much like the dow graph)
    Also;
    - Passing days of week plotted as an arg; this could allow the plotted days of week to be tweaked from the gui
    - Altering the coloring schemes; using specific color per day of week / group of days (e.g. weekend)

    """
    t1 = int(time.time())
    t1 = t1 - t1 % ts_cutoff
    # t0 = t1 - t1 % 604800 - 604800 * n_weeks
    legend_plots = [1, (n_days - 1) // 2, n_days]
    
    # todo: group lines by day of week, e.g. red lines denoting data from tuesdays; this would allow for plotted data
    #   to span multiple weeks while limiting the amount of colors used and providing some overview
    if colors is None:
        colors = [((20 + i * 30) % 255 / 255, (100 + 30 * i // 3) % 255 / 255, (80 + 30 * i // 4) % 255 / 255, 1.) for i
                  in range(n_days + 1)]
        # colors = ['b', 'g', 'c', 'm', 'y', 'k']
    t0_, x_axes, y_axes, patches, days_ago = t1 - t1 % 86400 - n_days * 86400, [], [], [], n_days
    # print(dow[utc_ts_to_dt(int(time.time()-time.time()%608400)).weekday()])
    
    y_merged, avg_prices = [], []
    # while t0_ < t1:
    while days_ago >= 0:
        t1_ = t0_ + 86400
        if days_ago == 0:
            # avg_prices = list(item.buy_price[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) &
            #                                             (item.buy_price > 0))]) + \
            #              list(item.sell_price[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) &
            #                                             (item.sell_price > 0))])
            break
        c = colors[days_ago] if days_ago > 0 else 'r'
        
        # Prevent the legend from flooding the entire plot
        if days_ago in legend_plots:
            patches.append(mpatches.Patch(color=c,
                                          label=f'Today' if days_ago == 0 else
                                          f'{days_ago} day{"s" if days_ago > 1 else ""} ago'))
        
        for y in (item.buy_price, item.sell_price):
            x = item.timestamp[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) & (y > 0))]
            y = y[np.nonzero((item.timestamp >= t0_) & (item.timestamp < t1_) & (y > 0))]
            y_avg = int(np.average(y))
            y = y / y_avg - 1
            axs.plot(x % 86400, y, color=c, linewidth=1.3 - days_ago * .10)
            y_merged += list(y)
        t0_ += 86400
        days_ago -= 1
    axs.legend(handles=patches)
    cp = realtime_prices.get(item.item_id)
    
    axs.set_xlim(0, 86400)
    axs.xaxis.set_major_formatter(xaxis_hod_format)
    axs.xaxis.set_ticks([h * 4 * 3600 for h in range(7)])
    
    y_merged.sort()
    y0, y1 = y_merged[int(len(y_merged) * .1)], y_merged[int(len(y_merged) * .9)]
    y_merged.sort()
    n_cutoff = int(len(y_merged) * .05)
    y1 = .025
    while max(abs(y_merged[n_cutoff]), abs(y_merged[-n_cutoff])) > y1 * .8:
        y1 += .025
    y0 = -1 * y1
    # y0, y1 = y_merged[n_cutoff]*(.8 if y0 > 0 else 1.2), y_merged[-n_cutoff]*(1.2 if y1 > 0 else .8)
    # axs.set_ylim(y0-(y1-y0)*.3, y1+(y1-y0)*.3)
    # axs.set_ylim(y0, y1)
    axs.set_ylim(y0, y1)
    
    # Rough estimation for potential profit; Average price (current week) - tax * (% difference between price at
    # rank 10% and rank 90%) * buy_limit OR 10% of avg daily volume, whichever is lower.
    n_cutoff *= 2
    # avg_prices = avg_prices[~np.isnan(avg_prices)]
    max_ts = max(item.timestamp)
    
    # avg_prices should reflect average prices for over the past 24 hours
    avg_prices = list(item.buy_price[np.nonzero((item.timestamp >= max_ts - 86400) &
                                                (item.timestamp < max_ts) &
                                                (item.buy_price > 0))]) + \
                 list(item.sell_price[np.nonzero((item.timestamp >= max_ts - 86400) &
                                                 (item.timestamp < max_ts) &
                                                 (item.sell_price > 0))])
    # print('avg prices', avg_prices)
    # print('n_cutoff', n_cutoff)
    # print('wiki_volume', item.wiki_volume[-7:])
    volatility = (np.average(avg_prices) - int(np.average(avg_prices) * .01)) * (
                y_merged[-n_cutoff] - y_merged[n_cutoff]) * \
                 min(item.buy_limit, int(np.average(item.wiki_volume[-7:]) / 10))
    # print('Estimated profit', format_n(volatility, max_decimals=2))
    axs.set_title(f"{item.item_name} ({item.item_id}) Price/hour of day (UTC)\n"
                  f"Current prices: {format_n(min(cp), 10, 3)} {format_n(max(cp), 10, 3)}\n"
                  f"Estimated profit: {format_n(volatility, max_decimals=2)}",
                  fontdict={'fontsize': 8.5})
    
    cur_time_vline = int(t1) % 86400
    # axs.plot([cur_time_vline, cur_time_vline], [y0, y1], color='r', linewidth=1.0)
    axs.vlines(x=cur_time_vline, ymin=y0, ymax=y1, colors='r', label='Current timestamp', linestyles='dashed')
    
    axs.yaxis.set_major_formatter(major_format_percentage)
    patches.append(mpatches.Patch(color='r', label='Current timestamp'))
    
    return axs


def price_per_dose(potion_name: str, axs: plt.Axes, t0: int = None, t1: int = None):
    if t1 is None:
        t1 = int(np.max(NpyArray(name_id.get(potion_name + f'(4)')).sell_price))
    if t0 is None:
        t0 = t1 - 86400 * 2
    ts_format = '%d-%m %Hh'
    bc, sc = [el + .1 * d for el in (.3, .15, .15) for d in range(4)], (.3, .6, .3)
    patches, cbs, all_y = [], [], []
    
    for n in (1, 2, 3, 4):
        next_id = name_id.get(potion_name + f' ({n})')
        if next_id is None:
            continue
            # raise ValueError(f"Unable to extract item ids from potion_name {potion_name + f' ({n})'}")
        npa = NpyArray(next_id)
        cur_prices = realtime_prices.get(next_id)
        x, y, c = npa.timestamp, npa.buy_price / n, bc[n]
        np_values_mask = np.nonzero((x >= t0) & (t1 > x) & (y > 0))
        axs.plot(x[np_values_mask], y[np_values_mask], color=bc[n])
        all_y += list(y[np_values_mask])
        patches.append(mpatches.Patch(color=c, label=f'{n}-dose buy (cur={min(cur_prices)})'))
        cbs.append(min(cur_prices))
        
        if n == 4:
            y = npa.sell_price / n
            np_values_mask = np.nonzero((x >= t0) & (t1 > x) & (y > 0))
            axs.plot(x[np_values_mask], y[np_values_mask], color=sc)
            all_y += list(y[np_values_mask])
            patches.append(mpatches.Patch(color=sc, label=f'{n}-dose sell (cur={max(cur_prices)})'))
    axs.legend(handles=patches, fontsize='small')
    title = f"{potion_name}  {format_ts(t0, ts_format)} - {format_ts(t1, ts_format)}\n" \
            f"Buy price per dose, for each dosage & Sell price for 4-dosed"
    axs.set_title(label=title)
    all_y.sort()
    ylim_prices = all_y[int(len(all_y) * .01):-int(len(all_y) * .01)]
    axs.set_ylim(int(min(ylim_prices) - (max(ylim_prices) - min(ylim_prices)) * .1),
                 int(max(ylim_prices) + (max(ylim_prices) - min(ylim_prices)) * .1))
    
    return axs


def plot_prices(axs: plt.Axes, np_ar: NpyArray, t0: int, t1: int):
    """
    Plot avg5m buy_prices, avg5m sell_prices and wiki_prices using data from `np_ar` in `axs`. Only plot prices for the
    timestamps between `t0` and `t1`.

    Parameters
    ----------
    axs : matplotlib.pyplot.Axes
        Area to plot the price graphs in
    np_ar : NpyArray
        Predefined NpyArray object with all the necessary data
    t0 : int
        lower bound unix timestamp
    t1 : int
        upper bound unix timestamp

    Returns
    -------
    The input `axs` with the price graph plots.

    """
    axs.set_title(label=f'{np_ar.item_name} prices')
    
    plots = []
    y_min, y_max = 2000000000, 0
    
    for y_val in ['buy_price', 'sell_price', 'wiki_price']:
        y = np_ar.__dict__.get(y_val)
        np_values_mask = np.nonzero((np_ar.timestamp >= t0) & (t1 > np_ar.timestamp) & (y > 0))
        y_values = y[np_values_mask]
        plots.append((np_ar.timestamp[np_values_mask], y_values))
        y_min, y_max = min(y_min, min(y_values)), max(y_max, max(y_values))
    
    # x-axis
    axs.set_xlim(t0, t1)
    x_ticks = [t0, t1]
    axs.xaxis.set_major_formatter(utc_ts_to_dt)
    axs.xaxis.set_minor_formatter(utc_ts_to_dt)
    axs.set_xticks(x_ticks)
    # axs.set_xticklabels([format_ts(ts, '%d-%m') for ts in x_ticks])
    print(ts_to_dt(t0), ts_to_dt(t1))
    
    # y-axis
    axs.set_ylim(y_min * .95, y_max * 1.05)
    
    # Legend
    blue_patch = mpatches.Patch(color='blue', label='Buy prices')
    green_patch = mpatches.Patch(color='green', label='Sell prices')
    red_patch = mpatches.Patch(color='red', label='Guide prices')
    
    colors = ['b', 'g', 'r', 'c', 'm', 'y']
    
    for p in plots:
        print(len(p[0]), len(p[1]))
        axs.plot(p[0], p[1], colors[plots.index(p)])
    axs.legend(handles=[blue_patch, green_patch, red_patch])
    return axs


def plot_volumes():
    pass


def plot_prices_weekly(axs: plt.Axes, np_ar: NpyArray):
    """
    Plot avg5m buy_prices, avg5m sell_prices and wiki_prices using data from `np_ar` in `axs`. Only plot prices for the
    timestamps between `t0` and `t1`.

    Parameters
    ----------
    axs : matplotlib.pyplot.Axes
        Area to plot the price graphs in
    np_ar : NpyArray
        Predefined NpyArray object with all the necessary data
    t0 : int
        lower bound unix timestamp
    t1 : int
        upper bound unix timestamp

    Returns
    -------
    The input `axs` with the price graph plots.

    """
    axs.set_title(label=f'{np_ar.item_name} prices')
    
    plots, t0, t1, week = [], min(np_ar.timestamp), max(np_ar.timestamp), 86400 * 7
    print(ts_to_dt(t0))
    print(dow[(t0 % (86400 * 7) * 86400 + 3) % 7])
    print(ts_to_dt(t1))
    print(dow[(t1 % (86400 * 7) * 86400 + 3) % 7])
    y_min, y_max = 2000000000, 0
    
    ts_temp = np.array([ts % week for ts in np_ar.timestamp])
    print(ts_temp[np.nonzero(ts_temp == 0)])
    print()
    buy_prices = np_ar.buy_price
    
    # print(ts_temp.index(0))
    print(ts_temp[575 + 86400 * 7 // 300])
    idx_0 = 0
    cur_idx = 0
    per_week = []
    mask = np.nonzero(buy_prices > 0)
    ts_div = np.array([ts % week for ts in np_ar.timestamp])
    min_y = 2000000000
    for week_id in np.nonzero(ts_div == 0):
        for idx_1 in week_id:
            per_week.append(([ts % week for ts in np_ar.timestamp[idx_0:idx_1]], list(np_ar.buy_price[idx_0:idx_1])))
            timestamps, prices = ts_div[idx_0:idx_1], np_ar.buy_price[idx_0:idx_1]
            np_values_mask = np.nonzero(prices != 0)
            print(len(timestamps), len(prices), len(np_values_mask))
            per_week.append((timestamps[np_values_mask], prices[np_values_mask]))
            min_y = min(min_y, min(prices[np_values_mask]))
            
            # print(week_id[cur_idx], list(np.nonzero(ts_temp == 0)))
            # # idx_1 = int(idx_1)
            # x_axis = np_ar.timestamp[idx_0:idx_1]
            # print(min(x_axis), max(x_axis), len(x_axis))
            # for y_val in ['buy_price', 'sell_price', 'wiki_price']:
            #     y = np_ar.__dict__.get(y_val)
            #     np_values_mask = np.nonzero((np_ar.timestamp >= t0) & (t1 > np_ar.timestamp) & (y > 0))
            #     y_values = y[np_values_mask]
            #     plots.append((np_ar.timestamp[np_values_mask], y_values))
            #     y_min, y_max = min(y_min, min(y_values)), max(y_max, max(y_values))
            idx_0 = idx_1
    # exit(0)
    # x-axis
    # axs.set_xlim(t0, t1)
    # x_ticks = [t0, t1]
    # axs.xaxis.set_major_formatter(utc_ts_to_dt)
    # axs.xaxis.set_minor_formatter(utc_ts_to_dt)
    # axs.set_xticks(x_ticks)
    # axs.set_xticklabels([format_ts(ts, '%d-%m') for ts in x_ticks])
    # print(ts_to_dt(t0), ts_to_dt(t1))
    
    # y-axis
    # axs.set_ylim(y_min*.95, y_max*1.05)
    
    # Legend
    blue_patch = mpatches.Patch(color='blue', label='Buy prices')
    green_patch = mpatches.Patch(color='green', label='Sell prices')
    red_patch = mpatches.Patch(color='red', label='Guide prices')
    
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'b', 'g', 'r', 'c', 'm', 'y', 'b', 'g', 'r', 'c', 'm', 'y']
    w_id = 1
    for p in per_week:
        # print(len(p[0]), len(p[1]))
        x, y = p
        idx = 0
        for n in y:
            if n == 0:
                y[idx] = (y[idx - 1] + y[idx + 1]) // 2
            idx += 1
        axs.plot(x, y, colors[w_id])
        w_id += 1
    axs.legend(handles=[blue_patch, green_patch, red_patch])
    # print(min_y)
    # exit(-1)
    return axs


if __name__ == "__main__":
    fig, a = plt.subplots(1)
    a = price_graph_by_dow(axs=a, item=NpyArray(2))
    plt.show()
