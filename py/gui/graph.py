"""
Deprecated module for displaying graphs within the GUI. Following a restructuring, the GUI component is now defined
in gui_objects, whereas graph implementations can be found in the graphs module.

For now, this module serves as reference as it did contain an old, working graph gui implementation

"""

import matplotlib

from model_item import NpyArray
from global_values import npyar_items
from path import save_data
from str_formats import format_ts
from ts_util import dt_to_ts, has_volume_ts

import copy
import tkinter as tk
from tkinter import ttk
from ge_util import *
from global_values import item_ids
from tkinter.messagebox import showerror
from matplotlib import pyplot as plt
import random

matplotlib.use('TkAgg')
# from matplotlib.backends._backend_tk import NavigationToolbar2
# from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg #, NavigationToolbar2Tk
try:
    # NavigationToolbar2TkAgg was deprecated in matplotlib 2.2 and is no
    # longer present in v3.0 - so a version test would also be possible.
    from matplotlib.backends._backend_tkagg import FigureCanvasTk, NavigationToolbar2Tk
except ImportError:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk


# This GUI is on a different window than the GE DB GUI and it runs on a separate thread. The implementation is very old
# and is stored here for reference. For a proper integrated implementation, see gui_objects.GuiGraph
class GraphGUIOld(ttk.Frame):
    def __init__(self, master, item_name='Cannonball', y_value='price',
                 start_date=date.datetime.today() - date.timedelta(days=730),
                 end_date=date.datetime.today(), src='wiki', **kwargs):
        super().__init__(**kwargs)
        # root = tkinter.Tk()
        # root.wm_title("Embedding in Tk")
        self.window = master
        self.window_width, self.window_height = 775, 800
        self.window.geometry(f'{self.window_width}x{self.window_height}')

        res = load_data('resources/values.dat')
        self.item_list = item_ids
        self.tracked_items = npyar_items
        if self.tracked_items is None:
            self.tracked_items = []
            res['tracked_item_graphs'] = []
            print("Could not find a tracked quantities list in resources/values.dat. Creating a new list!")
            save_data(res, 'resources/values.dat')
        elif not isinstance(self.tracked_items, list):
            print("Something was loaded as tracked_items_list for the graph GUI, but is doesn't appear to be a list...")

        del res
        random.seed()
        self.str_toplabel_variable = tk.StringVar()
        self.str_plot_header = tk.StringVar()
        self.str_graph_itemname = tk.StringVar()
        self.str_graph_itemname.set('Cannonball')
        self.graph_set = None
        self.plot_volume = True

        self.bool_use_timespan = tk.BooleanVar()
        self.bool_use_timespan.set(True)
        self.use_timespan = True
        self.timespan = -730
        self.wiki_volume_smoothing_window_size = 5
        self.str_end_date = tk.StringVar()
        self.str_end_date.set(ymd_to_dmy(str(date.date.today())))
        self.str_start_date = tk.StringVar()
        self.str_start_date.set(ymd_to_dmy(str(date.date.today() + date.timedelta(days=self.timespan))))
        self.str_graph_t1 = tk.StringVar()
        self.str_graph_t1.set(ymd_to_dmy(str(date.date.today() + date.timedelta(days=self.timespan))))
        self.str_graph_t2 = tk.StringVar()
        self.str_graph_t2.set(ymd_to_dmy(str(date.datetime.today())))
        self.str_source_radiobutton = tk.StringVar()
        self.str_source_radiobutton.set('wiki')
        self.str_graph_timespan = tk.StringVar()
        self.str_graph_timespan.set('-730')
        self.str_entry_horizontal_plots = tk.StringVar()
        self.str_entry_vertical_plots = tk.StringVar()
        self.str_entry_horizontal_plots.set('2')
        self.str_entry_vertical_plots.set('2')
        self.str_entry_next_col = tk.StringVar()
        self.str_entry_next_row = tk.StringVar()
        self.str_entry_next_col.set('0')
        self.str_entry_next_row.set('0')
        self.str_entry_statistics_t1 = tk.StringVar()
        self.str_entry_statistics_t2 = tk.StringVar()
        self.bool_track_item = tk.BooleanVar()
        self.bool_plot_volume = tk.BooleanVar()
        self.plot_queue = []
        self.next_col, self.next_row = 0, 0

        self.graph_item = ''
        self.planned_trades = []
        self.next_plot = None
        self.y_value = ''
        self.src = 'wiki'

        # self.window.title("OSRS Price Graphs")
        self.str_window_header = tk.StringVar()
        self.plot_header = ''
        self.btn = tk.Label(self.window, textvariable=self.str_plot_header)
        self.btn.grid(row=0, column=0, padx=20, pady=5, sticky='N')

        if start_date >= end_date or not y_value == 'price' and (
                not has_volume_ts(start_date) or not has_volume_ts(end_date)):
            showerror("Invalid start/end date",
                      "start date should be smaller than end date and trading volumes are only available from September 25th, 2018")
            self.destroy()

        ###
        self.fig, self.axs = None, None
        self.canvas = None
        self.toolbarFrame = tk.Frame(master=self.window)
        self.toolbar = None

        self.graph_interface_frame = ttk.Frame(self.window)
        self.t1 = dt_to_ts(date.datetime.today() + date.timedelta(days=self.timespan))
        self.t2 = dt_to_ts(date.datetime.today())
        self.plot_idx = 0

        self.str_label_graph_interface = tk.StringVar()
        self.str_label_time_frame = tk.StringVar()
        self.str_label_radiobutton_src = tk.StringVar()

        self.label_graph_interface_top = tk.Label(self.graph_interface_frame, text='Item name')
        self.label_graph_interface_timeframe = tk.Label(textvariable=self.str_label_time_frame)

        self.label_graph_item = tk.Label(self.graph_interface_frame, text="Item name:")
        self.entry_graph_item = tk.Entry(self.graph_interface_frame, textvariable=self.str_graph_itemname)
        self.label_graph_t1 = tk.Label(self.graph_interface_frame, text="Start date:")
        self.entry_t1 = tk.Entry(self.graph_interface_frame, textvariable=self.str_graph_t1)
        self.label_graph_t2 = tk.Label(self.graph_interface_frame, text="End date:")
        self.entry_t2 = tk.Entry(self.graph_interface_frame, textvariable=self.str_graph_t2)
        self.label_graph_timespan = tk.Label(self.graph_interface_frame, text="Timespan (days):")
        self.entry_timespan = tk.Entry(self.graph_interface_frame, textvariable=self.str_graph_timespan)
        self.button_timespan = tk.Button(self.graph_interface_frame, text='Add/subtract days',
                                              command=self.add_timespan)
        self.label_radiobutton_src = tk.Label(self.window, textvariable=self.str_label_radiobutton_src)
        self.radiobutton_wiki = tk.Radiobutton(self.graph_interface_frame, text='wiki',
                                               variable=self.str_source_radiobutton, value='wiki')
        self.radiobutton_osb180 = tk.Radiobutton(self.graph_interface_frame, text='avg5m',
                                                 variable=self.str_source_radiobutton, value='avg5m')
        self.radiobutton_osb1440 = tk.Radiobutton(self.graph_interface_frame, text='realtime',
                                                  variable=self.str_source_radiobutton, value='realtime')
        # self.radiobutton_osb4320 = tk.Radiobutton(self.graph_interface_frame, text='osb 3 months',
        #                                           variable=self.str_source_radiobutton, value='osb4320')

        self.button_load_graph = tk.Button(self.graph_interface_frame, text='Load item graph',
                                           command=self.submit_wiki_plot)
        self.checkbutton_track_item = tk.Checkbutton(self.graph_interface_frame, text='Track item (currently unused)',
                                                command=self.toggle_timespan)
        self.button_random_graph = tk.Button(self.graph_interface_frame, text='Random graph', command=self.random_graph)

        # self.label_entry_plot_next_col = tk.Label(self.graph_interface_frame, text='col # of next plot submission')
        # self.label_entry_plot_next_row = tk.Label(self.graph_interface_frame, text='row # of next plot submission')
        # self.entry_plot_next_col = tk.Entry(self.graph_interface_frame, textvariable=self.str_entry_next_col)
        # self.entry_plot_next_row = tk.Entry(self.graph_interface_frame, textvariable=self.str_entry_vertical_plots)
        # self.label_entry_plot_max_cols = tk.Label(self.graph_interface_frame,
        #                                           text='Nax # of plots plotted horizontally')
        # self.label_entry_plot_max_rows = tk.Label(self.graph_interface_frame, text='Max # of plots plotted vertically')
        # self.entry_plot_max_col = tk.Entry(self.graph_interface_frame, textvariable=self.str_entry_horizontal_plots)
        # self.label_entry_plot_max_rows = tk.Label(self.graph_interface_frame, text='Max # of plots plotted vertically')
        # self.entry_plot_max_row = tk.Entry(self.graph_interface_frame, textvariable=self.str_entry_vertical_plots)

        # self.label_entry_target_price = tk.Label(self.graph_interface_frame, text="Target_price")

        self.planned_trades_frame = ttk.Frame(master=self.window)

        # Planned trades
        self.str_label_planned_trades = tk.StringVar()
        self.str_entry_item = tk.StringVar()
        self.str_entry_quantity = tk.StringVar()
        self.str_entry_ge_price = tk.StringVar()
        self.str_entry_target_price = tk.StringVar()

        self.label_entry_item = tk.Label(self.planned_trades_frame, text='Item name')
        self.label_entry_quantity = tk.Label(self.planned_trades_frame, text='Quantity')
        self.label_entry_ge_price = tk.Label(self.planned_trades_frame, text='Buy price')
        self.label_entry_target_price = tk.Label(self.planned_trades_frame, text="Target_price")

        self.entry_item = tk.Entry(self.planned_trades_frame, textvariable=self.str_entry_item)
        self.entry_quantity = tk.Entry(self.planned_trades_frame, textvariable=self.str_entry_quantity)
        self.entry_ge_price = tk.Entry(self.planned_trades_frame, textvariable=self.str_entry_ge_price)
        self.entry_target_price = tk.Entry(self.planned_trades_frame, textvariable=self.str_entry_target_price)

        self.button_submit_trade = tk.Button(self.planned_trades_frame, text='Submit to queue',
                                             command=self.submit_entry)
        self.button_clear_queue = tk.Button(self.planned_trades_frame, text='Clear queue', command=self.clear_queue)
        self.button_save_queue = tk.Button(self.planned_trades_frame, text='Add queue to ledger',
                                           command=self.submit_queue)
        # self.submit_wiki_plot(plot_price=True)
        self.setup_plot_interface()

    def setup_plot_interface(self):
        self.toggle_timespan()
        self.label_graph_interface_top.grid(row=0, rowspan=1, column=0, columnspan=1, sticky='NW')
        # self.label_graph_interface_timeframe.grid(row=1, rowspan=1, column=0, columnspan=1, sticky='NW')

        self.label_graph_item.grid(row=2, rowspan=1, column=0, columnspan=1, sticky='NW')
        self.entry_graph_item.grid(row=2, rowspan=1, column=1, columnspan=2, sticky='NW')
        self.checkbutton_track_item.grid(row=2, rowspan=1, column=3, columnspan=2, sticky='NW')
        
        self.label_graph_timespan.grid(row=3, rowspan=1, column=0, columnspan=1, sticky='E')
        self.entry_timespan.grid(row=3, rowspan=1, column=1, columnspan=1, sticky='W')
        self.button_timespan.grid(row=3, rowspan=1, column=2, columnspan=3, sticky='W')
        
        self.label_graph_t1.grid(row=4, rowspan=1, column=0, columnspan=1, sticky='E')
        self.entry_t1.grid(row=4, rowspan=1, column=1, columnspan=2, sticky='W')
        self.label_graph_t2.grid(row=5, rowspan=1, column=0, columnspan=1, sticky='E')
        self.entry_t2.grid(row=5, rowspan=1, column=1, columnspan=2, sticky='W')
        
        self.label_radiobutton_src.grid(row=5, rowspan=1, column=0, columnspan=2, sticky='S')
        self.radiobutton_wiki.grid(row=6, rowspan=1, column=0, columnspan=1, sticky='E')
        self.radiobutton_osb180.grid(row=6, rowspan=1, column=1, columnspan=1, sticky='W')
        self.radiobutton_osb1440.grid(row=7, rowspan=1, column=0, columnspan=1, sticky='E')
        # self.radiobutton_osb4320.grid(row=7, rowspan=1, column=1, columnspan=1, sticky='W')

        self.button_load_graph.grid(row=8, rowspan=1, column=1, columnspan=2, sticky='W', pady=8)
        self.button_random_graph.grid(row=8, rowspan=1, column=3, columnspan=2, sticky='W', pady=8)

        self.graph_interface_frame.grid(row=3, column=0, pady=5)

    def toggle_timespan(self, e=None):
        ts = parse_integer(self.str_graph_timespan.get())
        self.use_timespan = self.bool_use_timespan.get()
        msg = 'Start date: {t1}, End date: {t2}\n'.format(t1=format_ts(self.t1), t2=format_ts(self.t2))
        if self.use_timespan:
            if ts > 0:
                msg += 'Timespan is set to a positive shift of {ts} days \nEnd date = {t1} + {ts} days'.format(ts=ts, t1=format_ts(self.t1))
            elif ts < 0:
                msg += 'Timespan is set to a negative shift of {ts} days \nStart date = {t2} - {ts} days' \
                    .format(ts=-1 * ts, t2=format_ts(self.t2))
            else:
                msg += 'Timespan is used as interval length in days if checked \nIt should not be 0...'
        else:
            msg += ' Timespan is currently disabled \nUsing manual input from start/end entries as interval'
        self.str_label_time_frame.set(msg)

    def add_timespan(self, e=None):
        ts = parse_integer(self.str_graph_timespan.get())
        print(ts)
        if ts > 0:
            dt = ts_to_dt(self.t1)
            self.t2 = dt_to_ts(dt + date.timedelta(days=ts))
            self.str_graph_t2.set(ymd_to_dmy(str(dt + date.timedelta(days=ts))))
        elif ts < 0:
            dt = ts_to_dt(self.t2)
            self.t1 = dt_to_ts(dt - date.timedelta(days=ts))
            self.str_graph_t1.set(ymd_to_dmy(str(dt + date.timedelta(days=ts))))


    def submit_entry(self, e):
        item_name = self.str_entry_item.get()
        if item_name not in self.item_list:
            showerror(title="Input error",
                      message="Item not found in full item DB! Check spelling / plurals. Caps don't matter, though.")
            return

        quantity = parse_integer(self.str_entry_quantity.get())
        ge_price = parse_integer(self.str_entry_ge_price.get())
        target_price = parse_integer(self.str_entry_target_price.get())

        if quantity <= 0 or ge_price <= 0 or target_price <= 0:
            showerror(title="Input error",
                      message="Quantity, ge price and target price should all be >0")
            return

        today = date.datetime.today()
        e = ledger_entry(item_name=item_name, account_name='Dogadon', status=Status.planned, is_buy_offer=True,
                         quantity=quantity, ge_price=ge_price,
                         target_price=target_price, date_logged=today, date_completed=today)
        self.planned_trades.append(e)

    def random_graph(self):
        srcs = ('wiki', 'osb180', 'osb1440', 'osb4320')
        self.str_source_radiobutton.set(srcs[random.randint(0, 3)])
        self.str_graph_itemname.set(self.item_list[random.randint(0, len(self.item_list) - 1)])

    def clear_queue(self, e):
        self.planned_trades = []

    def submit_queue(self, e):
        ledger = load_data('Data/Ledger/Ledger.dat')
        planned_trades = copy.deepcopy(self.planned_trades)
        for e in planned_trades:
            ledger.new_entry(e)
        save_data(ledger, 'Data/Ledger/Ledger.dat')
        self.planned_trades = []

    def update_variables(self):
        self.graph_item = self.str_graph_itemname.get()
        self.use_timespan = self.bool_use_timespan.get()
        print('strg1= ', self.str_graph_t1.get())
        self.t1 = parse_date_entry(self.str_graph_t1.get(), dmy_input=True)
        self.t2 = parse_date_entry(self.str_graph_t2.get(), dmy_input=True)
        self.src = self.str_source_radiobutton.get()
        # if self.use_timespan:
        #     if self.timespan > 0:
        #         self.t2 = self.t1 + date.timedelta(days=self.timespan)
        #         if self.t2 > date.datetime.today():
        #             self.t2 = date.datetime.today()
        #     elif self.timespan < 0:
        #         self.t1 = self.t2 + date.timedelta(days=self.timespan)
        self.t1 = dt_to_ts(self.t1)
        self.t2 = dt_to_ts(self.t2)
        self.str_label_radiobutton_src.set("Current src: {src}".format(src=self.src))
        self.max_plot_cols, self.max_plot_rows = int(self.str_entry_horizontal_plots.get()), int(
            self.str_entry_vertical_plots.get())
        self.next_col = int(self.str_entry_next_col.get())
        self.next_row = int(self.str_entry_next_row.get())
        ts = ', timespan (active)' if self.use_timespan else ', timespan (inactive)'
        ts += '={ts}'.format(ts=self.timespan)
        print(
            "The following variables are set: Item={i}, source={src}, start_time={t1}, end_time={t2}{timespan}, col={col}, row={row}" \
                .format(i=self.graph_item, src=self.src, t1=format_ts(self.t1), t2=format_ts(self.t2),
                        timespan=ts, col=self.next_col, row=self.next_row))

    def setup_planned_trade_frame(self):

        return None

    # Plot the price or the volume of a wiki plot, based on plot_price (e.g. False means plotting volume)
    def submit_wiki_plot(self, e=None, plot_price=True):
        self.update_variables()
        src = 'OSRS Wiki ' if 'wiki' in self.src else 'RSBuddy Exchange '
        src += '({ts})'.format(ts='1 Week' if '180' in self.src else '1 Month' if '1440' in self.src else '3 Months' if '4320' in self.src else 'All-time')
        header = ' graph of {i} | {src} | '.format(i=self.graph_item, src=src)
        if plot_price:
            self.plot_queue.append(
                {'item_name': self.graph_item, 'source': self.src, 'y_value': 'price', 'col': self.next_col,
                 'row': self.next_row, 'start_date': ts_to_dt(self.t1), 'end_date': ts_to_dt(self.t2),
                 'header': '| Price' + header})
            self.update_plotgrid_idx()
        else:
            self.plot_queue.append(
                {'item_name': self.graph_item, 'source': self.src, 'y_value': 'volume', 'col': self.next_col,
                 'row': self.next_row, 'start_date': ts_to_dt(self.t1), 'end_date': ts_to_dt(self.t2),
                 'header': '| Trading volume' + header})
            self.update_plotgrid_idx()
        self.plot_queue = [self.plot_queue[-1]]
        item = NpyArray(2)
        t_now = int(time.time())
        x = item.timestamp[np.nonzero(item.timestamp >= t_now - 86400 - t_now % 86400)]
        y1 = item.buy_price[np.nonzero(item.timestamp >= t_now - 86400 - t_now % 86400)]
        self.plot_data(x, y1, 'None')

    # Set all variables to the values of the plot at index plot_idx in the plot_queue
    def load_plot(self):
        
        plot = self.plot_queue[self.plot_idx]
        self.str_graph_itemname.set(plot.get('item_name'))
        self.str_graph_t1.set(ymd_to_dmy(str(plot.get('start_date'))))
        self.str_graph_t2.set(ymd_to_dmy(str(plot.get('end_date'))))
        self.str_graph_timespan.set(str((plot.get('end_date') - plot.get('start_date')).days))
        self.str_label_graph_interface.set('Graph plot interface')
        self.bool_use_timespan.set(True)
        self.plot_header = plot.get('header')
        self.next_col, self.next_row = plot.get('col'), plot.get('row')
        self.y_value = plot.get('y_value')
        ts = ', timespan (active)' if self.use_timespan else ', timespan (inactive)'
        ts += '={ts}'.format(ts=self.timespan)
        print(
            "The following variables are set: Item={i}, source={src}, y-axis={yval} start_time={t1}, end_time={t2}{timespan}, col={col}, row={row}" \
                .format(i=self.graph_item, src=self.src, yval=self.y_value, t1=format_ts(self.t1),
                        t2=format_ts(self.t2),
                        timespan=ts, col=self.next_col, row=self.next_row))
        return plot

    # Remove the plot with the set index from the queue
    def remove_plot(self, e):
        if 0 <= self.plot_idx < len(self.plot_queue):
            self.plot_queue.remove(self.plot_queue[self.plot_idx])
        else:
            showerror("Index error", "The currently selected plot_idx is invalid! Setting it to 0...")
            self.plot_idx = 0

    # Set the next column and row for plotting a plot in the subgrid
    def update_plotgrid_idx(self):
        self.next_col += 1
        if self.next_col == self.max_plot_cols:
            self.next_col = 0
            self.next_row += 1
            if self.next_row == self.max_plot_rows:
                self.next_row = 0

    # Plot all the plots that have been loaded in the plot queue.
    # Although there is some code for it, the plots will be stacked vertically on top of each other for now
    def plot_data(self, x, y, title):
        self.plot_idx = 0
        # self.load_plot()
        self.plot_queue = [{'item_name': 'Cannonball', 'source': self.src, 'y_value': 'volume', 'col': self.next_col,
                            'row': self.next_row, 'start_date': ts_to_dt(min(x)), 'end_date': ts_to_dt(max(x)),
                            'header': '| Trading volume'}]
        self.fig, self.axs = plt.subplots(len(self.plot_queue))
        # self.fig = plt.figure(self.plot_idx, )
        print(type(self.fig))
        fig_height = len(self.plot_queue) * 3.75
        self.fig.set_size_inches(w=7.3, h=int(fig_height))
        # self.fig.set_constrained_layout_pads(w_pad=0.1, h_pad=0.1)
        self.fig, self.axs = plt.subplots(1)
        
        while self.plot_idx < 1:
            if len(self.plot_queue) == 1:
                next_plot = self.axs
            else:
                next_plot = self.axs[self.plot_idx]

            vals = self.load_plot()
            item, src, y_value, title = vals.get('item_name'), vals.get('source'), vals.get('y_value'), vals.get(
                'header')
            item_id = name_id.get(item)
            t1, t2 = vals.get('start_date'), vals.get('end_date')
            del vals

            if src == 'wiki' or 'realtime':
                valid, idx_e, total = 0, 0, 0
                # x and y are the full axes that will be plotted, each y-value should be valid
                volume = y_value == 'volume' and src != 'realtime'
                if volume:
                    n_days = self.wiki_volume_smoothing_window_size
                    # The interval is extended a little to add extra days for averaging the data (smoothing the graph)
                    t1 -= date.timedelta(days=n_days)
                else:
                    n_days = 1

                x_ticks, y_ticks, y_temp, y_idx, y2, x2 = [], [], [], 2 if volume else 1, [], []
                # print('db', self.dbs.read_db(item_id=item_id, con=self.con, src=src, attributes='*'))
                # wiki = src == 'wiki'
                # realtime = src == 'realtime'
                # for d in self.dbs.read_db(item_id=item_id, con=self.con, src=src).get(self.dbs.src_ids.get(src)).to_dict('records'):
                #     total += 1
                #     ts, p, v = d.get('timestamp'), d.get('price'), d.get('volume')
                #     valid += 1
                #     if p > 0 and self.t2 > ts > self.t1:
                #
                #         if wiki:
                #             x.append(ts_to_dt(ts))
                #             y.append(p)
                #         if realtime:
                #             if d.get('is_sale'):
                #                 x.append(ts_to_dt(ts))
                #                 y.append(p)
                #             else:
                #                 x2.append(ts_to_dt(ts))
                #                 y2.append(p)
                                
                    # if volume:
                    #     i = valid - n_days if valid >= n_days else 0
                    #     val = np.average(y[i:valid])
                    #     y_temp.append(val)
                # if volume:
                #     y = y_temp
                # ytemp = [n for n in y]
                # ytemp.sort()
                # ytemp2 = [n for n in y2]
                # ytemp2.sort()
                # if realtime:
                #     ytemp = ytemp[int(len(ytemp)*.05):int(len(ytemp)*.95)]
                #     ytemp2 = ytemp2[int(len(ytemp2)*.05):int(len(ytemp2)*.95)]
                # min_x, min_y, max_x, max_y = min(x + x2), min(ytemp + ytemp2), max(x + x2), max(ytemp + ytemp2)
                # if wiki:
                #     ylim = [max(0, int(min_y-.05*min_y)), int(max_y + max_y * 0.05)]
                # else:
                #     ylim = [max(0, min_y-min_y*.1), int(max_y + max_y * 0.1)]
                # title_t = '{d1}/{m1}/{y1} - {d2}/{m2}/{y2}'.format(d1=min_x.day, m1 = min_x.month, y1=min_x.year, d2=max_x.day, m2=max_x.month, y2=max_x.year)
                # title += title_t    # 'Start:{t1} - End:{t2}'.format(t1=ymd_to_dmy(str(min_x))[:-2], t2=ymd_to_dmy(str(max_x))[:-2])
                self.axs.set_title(title, fontdict={'fontsize': 8.5}, pad=15.0)
                self.axs.set_xlabel("Date")
                self.axs.set_ylabel('Amount traded (n)' if volume else 'Price (gp)')

                self.axs.set_xlim(min(x), max(x)), next_plot.set_ylim(min(y), max((y)))
                next_plot.ticklabel_format(axis='y', style='sci', useMathText=True)
                # print(x)
                # print(y)
                self.axs.plot(x, y, 'r', linewidth=.4)
                # if realtime:
                #     next_plot.plot(x2, y2, 'b', linewidth=.4)
                self.plot_idx+=1
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.window)
        self.canvas.get_tk_widget().grid(row=1, column=0, columnspan=10, padx=5, pady=5, sticky='N')

        # navigation toolbar
        self.toolbarFrame = tk.Frame(master=self.window)
        self.toolbarFrame.grid(row=2, column=0, columnspan=10, sticky='N')
        self.toolbar = NavigationToolbar2Tk(self.canvas, self.toolbarFrame)

        self.canvas.draw()
        print("Done plotting!")
        return None

    def get_window(self):
        return self.window


"""
Graph UI
- Define a set of graphs that can be generated with some input (e.g. price graph given item_id, timespan, price value)
- Given a subplot, scroll through items and generate graphs for each item


"""




# Verify whether the datapoint can be processed.
# Datapoint is a timestamp entry from the wiki or osb database
# If the required data is found to be invalid or absent, None will be returned
# (x, y) will be returned if the data is valid.
# A subset of the dataset can be gathered by providing a start and/or end time
def valid_wiki_datapoint(timestamp, price, volume, t1=date.datetime.today() - date.timedelta(days=5000), t2=date.datetime.today()):
    if t1 < date.datetime(2015, 2, 26):
        t1 = date.datetime(2015, 2, 26)
    t1 = dt_to_ts(t1)
    t2 = dt_to_ts(t2)
    if volume and not has_volume_ts(timestamp):
        return False
    if price > 0 and t1 <= timestamp <= t2:
        return True
    return False
