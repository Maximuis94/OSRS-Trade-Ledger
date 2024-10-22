import datetime
import math
import sqlite3
import time
from collections.abc import Iterable, Collection

import ge_util
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import path
import ts_util
from ge_util import get_item_info, get_graph_interval, load_realtime_entries
from global_values import id_name, min_avg5m_ts, int32_max, name_id
from graphs import TimeSeriesPlot, Graph, Axis
from gui_formats import get_rgb
from model_item import NpyArray, Item
from path import load_data, f_np_archive_columns
from str_formats import format_n, format_dt, format_ts
from ts_util import dt_to_ts

from file.file import File

league_timestamps = [
    # Trailblazer 2 15-11-2023 - 10-1-2024
    ('toa', 'Trailblazer 2', dt_to_ts(datetime.datetime(2022, 8, 1)), dt_to_ts(datetime.datetime(2024, 12, 24)))
]


def npya_to_graph_dow(npy: NpyArray, columns: list = load_data(f_np_archive_columns), load=True):
	# for i in range(len(columns)):
	# 	print(columns[i], npy.ar[:, i], '\n')
	# print(npy)
	item_name = npy.item_name
	timestamps = npy.timestamp[:100]
	print(ts_util.ts_to_dt(np.min(timestamps)), ts_util.ts_to_dt(np.max(timestamps)))
	volume = False
	# print(tses)
	
	fig, axs = plt.subplots(1)
	axs.set_title(item_name, fontdict={'fontsize': 8.5}, pad=15.0)
	axs.set_xlabel("Date")
	axs.set_ylabel('Amount traded (n)' if volume else 'Price (gp)')
	
	# axs.set_xlim(min_x, max_x), next_plot.set_ylim(ylim[0], ylim[1])
	# next_plot.ticklabel_format(axis='y', style='sci', useMathText=True)
	g = npy.extract_columns(columns=['day_of_week', 'buy_price'])[1]
	n = len(g[0])
	ts, vs = g[0][n // 4:], g[1][n // 4:]  # g[len(g[0])//4*3:], g[1][len(g[0])//4*3:]
	x, y = [], []
	for idx in range(len(vs)):
		if vs[idx] > 0:
			x.append(ts[idx])
			y.append(vs[idx])
	min_y, max_y = np.sort(y)[int(.05 * len(y))], np.sort(y)[int(.95 * len(y))]
	print(min_y, max_y)
	x, y = [], []
	for idx in range(len(vs)):
		if max_y > vs[idx] > min_y:
			x.append(ts[idx])
			y.append(vs[idx])
	
	y_avg = [np.average([y[i] for i in range(len(x)) if x[i] == dow]) for dow in range(7)]
	print(y_avg)
	
	plt.figure(1)
	plt.scatter(x, y)
	plt.scatter([dow for dow in range(7)], y_avg, c='r')
	plt.show()
	exit(1)
	# if not isinstance(y, np.ndarray):
	# 	exit(1233)
	g = [[] for _ in range(7)]
	
	for idx in range(len(y)):
		t, v = x[idx], y[idx]
		if v > 0:
			print(x[idx], y[idx])
			g[x[idx]].append(y[idx])
	
	for vals in g:
		print(g.index(vals), int(np.average(vals)))
	
	# for h in range(len(hs)):
	# 	print(h, hs[h])
	
	# plt.xticks(ticks=x, labels=[str(i) for i in x])
	g = {x[idx]: y[idx] for idx in list(range(len(x))) if y[idx] > 0}
	
	plt.figure(1)
	plt.scatter(list(g.keys()), [g.get(idx) for idx in list(g.keys())])
	plt.show()
	print(len(x), len(y))
	print(x[:47])
	print(y[:47])
	exit(1)


xlim_min, xlim_max, ylim_min, ylim_max = None, None, int32_max, 0

"""
TODO: Extend price plots towards multiple items;
	For each item: similar color schemes for lines/extremes
		> 5 plots per item; y_min (low horizontal line), y_max (high horizontal line), buy_prices (bottom of line), sell_prices (top of line), wiki_prices (centre of line)
		> average price plot?
		> Given a color, return a set of somewhat related colors
"""
def get_colors(base_rgb: tuple = (.5, .5, .5), multipliers: tuple = (1, 1, 1), v: int = 1, edge_color_multipliers: tuple = (.5, 1.5)):
	y0 = tuple([max(0,min(1, v*c*m*.7*edge_color_multipliers[0]**2)) for c, m in zip(base_rgb, multipliers)])
	y1 = tuple([max(0,min(1, v*c*m*1.3*edge_color_multipliers[1])) for c, m in zip(base_rgb, multipliers)])
	p0 = tuple([max(0,min(1, v*c*m*.85*edge_color_multipliers[0])) for c, m in zip(base_rgb, multipliers)])
	p1 = tuple([max(0,min(1, v*c*m*1.15*edge_color_multipliers[1]**2)) for c, m in zip(base_rgb, multipliers)])
	w = tuple([max(0,min(1, v*c)) for c, m in zip(base_rgb, multipliers)])
	return y0, p0, w, p1, y1

plot_color_schemes = [[
		(.1, .1, .1),
		(.1, .175, .1),
		(.1, .25, .1),
		(.1, .325, .1),
		(.1, .4, .1)
	], [
	(.5, .35, .2),
	(.5, .25, .2),
	(.5, .1, .2),
	(.5, .25, .2),
	(.5, .35, .2)
]]


color_dicts = {
    'red': (
        (0.0, 0.0, 0.0),
        (0.5, 0.0, 0.1),
        (1.0, 1.0, 1.0),
    ),
    'green': (
        (0.0, 0.0, 0.0),
        (1.0, 0.0, 0.0),
    ),
    'blue': (
        (0.0, 0.0, 1.0),
        (0.5, 0.1, 0.0),
        (1.0, 0.0, 0.0),
    )
}


# TODO: Implement using TimeSeriesPlot object
# TODO: Rewrite to show sum of resource graphs and product graph, e.g. 20*zulrah scales + antidote against anti-venom
#   Create new method for plotting produced items?
def plot_items(item_ids: list, t0: int, t1: int, v_step: int = None, plot_y_min_max: bool = True, horizontal_plots: Iterable = (),
               rgbs: Iterable = None, y_multiplier: list = None, merge_ids: Collection = None):
	"""
	Plot multiple item price graphs in the same figure. For each item_id in `item_ids`;
	- Add its name, daily volume and price range as plotted to the title
	- plot the buy/sell prices, wiki price and a horizontal line for the min and max y-values.
	- Assign a unique/custom color to each item
	- (if True) Plot vertical lines, indicating fixed time steps of which the size depends on the total coverage.
	
	Parameters
	----------
	item_ids : Iterable
		One or more item_ids that should be plotted into the figure
	t0 : int
		Lower bound unix timestamp
	t1 : int
		Upper bound unix timestamp
	out_file : str
		Output file to which the graph should be exported as an image
	v_step : int, optional, None by default
		Step size in days for each vertical line that is plotted
	horizontal_plots : list
		A list of y-values and corresponding color schemes that should be plotted horizontally
	plot_y_min_max : bool, optional, True by default
		If True, plot a horizontal line for the lowest and highest price values for each item, colored similarly
	rgbs : Iterable
		A collection of rgb-values that is to be used for plots of each item_id. There should be at least as many
		rgb-tuples as item_ids.
		

	Returns
	-------

	"""
	if rgbs is None:
		rgbs =  [[.1, .1, .7], [.3, .1, .3], [.5, .1, .5], [.2, .4, .2]]
		print(len(rgbs), 'ffdgdf')
	fig, axs = plt.subplots(1)
	con = sqlite3.connect(path.f_db_scraped_src)
	global ylim_min, ylim_max, xlim_min, xlim_max
	ts_format = "%d-%m-%y %H:00"
	if (t1 - t0) // 86400 > 30:
		ts_format = ts_format.split(' ')[0]
	plot_title = f'Timespan: {format_ts(t0, ts_format)} - {format_ts(t1, ts_format)} (~{round((t1-t0)/86400)} days)\n'
	y_mins, y_maxs, patches = [], [], []
	# Plot a vertical line every v_step days
	if isinstance(v_step, int):
		v_plots = []
		v_plot_range = range(t0, t1, v_step*86400)
		for x, y in zip([(t-t%86400, t-t%86400) for t in v_plot_range], [(0, int32_max) for _ in v_plot_range]):
			v_plots.append((x, y))
		# v_plots = [[(t-t%86400, t-t%86400), (0, int32_max)] for t in range(t0, t1, v_step*86400)]
		patches.append(mpatches.Patch(color='grey', label=f'{v_step}-daily vertical plots'))
	else:
		v_plots = []
	plots = {}
	for item_id, rgb, m in zip(item_ids, rgbs,
	                           [1 for _ in item_ids] if y_multiplier is None else
	                           list(y_multiplier) + [1 for _ in range(len(item_ids)-len(y_multiplier))]):
		name = id_name[item_id]
		# rgb_outline, rgb = tuple(rgb + [.5]), tuple(rgb + [.9])
		rgb = list(get_rgb(item_id % 2525))
		print(name, rgb)
		rgb_outline, rgb = tuple(rgb + [.5]), tuple(rgb + [.9])
		# y_low, p_low, p_mid, p_high, y_high = get_colors(c, (.8, .1, .8))
		# y_low, p_low, p_mid, p_high, y_high = plot_color_schemes[item_ids.index(item_id)]
		# print(y_low, p_low, p_mid, p_high, y_high)
		sql_exe, values = 'SELECT COLUMNS FROM avg5m WHERE item_id=:id AND timestamp > :t0', {'id': item_id, 't0': t0}
		wiki_exe = 'SELECT timestamp, price, volume FROM wiki WHERE item_id=:id AND timestamp > :t0'
		price_data = pd.read_sql(sql=sql_exe.replace('COLUMNS', 'timestamp, buy_price, sell_price'), params=values,
		                         con=con)
		wiki_data = pd.read_sql(sql=wiki_exe, params=values, con=con)
		buy_price_data = price_data.loc[price_data['buy_price'] > 0]
		sell_price_data = price_data.loc[price_data['sell_price'] > 0]
		if y_multiplier is not None:
			try:
				if m != 1:
					print(f'applying m={m} for item {id_name[item_id]}')
					# print(buy_price_data['buy_price'].to_list())
					buy_price_data['buy_price'] = buy_price_data['buy_price'].apply(lambda y_value: m*y_value)
					sell_price_data['sell_price'] = sell_price_data['sell_price'].apply(lambda y_value: m*y_value)
					# print(buy_price_data['buy_price'].to_list())
			except IndexError:
				pass
		else:
			pass
		
		n_cutoff = int(max(len(sell_price_data), len(buy_price_data)) * .05)
		y_sorted = buy_price_data['buy_price'].to_list() + sell_price_data['buy_price'].to_list()[n_cutoff:-n_cutoff]
		y_sorted.sort()
		y_sorted = [y for y in y_sorted if y > 0][n_cutoff:-n_cutoff]
		y_min = min(y_sorted)
		y_max = max(y_sorted)
		y_mins.append(y_min)
		y_maxs.append(y_max)
		print(id_name[item_id], y_max)
		print(y_mins)
		
		horizontal_plots = [()]
		# x2, y2 = None, None
		del buy_price_data['sell_price'], sell_price_data['buy_price']
		
		# item_name = npy.item_name
		# timestamps = npy.extract_columns(['timestamp'])[1][:100]
		# print(ts_util.ts_to_dt(np.min(timestamps)), ts_util.ts_to_dt(np.max(timestamps)))
		volume = False
		# print(tses)
		avg_volume = int(np.average(wiki_data['volume'].to_numpy()))
		
		plot_title += f'{name}   Volume: {avg_volume}   Price range: {format_n(y_min, 1)} - {format_n(y_max, 1)}\n'
		x0, x1 = buy_price_data['timestamp'].to_list(), sell_price_data['timestamp'].to_list()
		y0, y1 = buy_price_data['buy_price'].to_list(), sell_price_data['sell_price'].to_list()
		x2, y2 = wiki_data['timestamp'].to_list(), wiki_data['price'].to_list()
		x3, y3 = [min(x1), max(x1)], [y_min, y_min]
		x4, y4 = [min(x1), max(x1)], [y_max, y_max]
		axs.xaxis.set_major_formatter(ts_util.utc_ts_to_dt)
		axs.xaxis.set_minor_formatter(ts_util.utc_ts_to_dt)
		axs.yaxis.set_major_formatter(format_n)
		axs.yaxis.set_minor_formatter(format_n)
		delta_y = (y_max-y_min)*.15
		if merge_ids is None or isinstance(merge_ids, Collection) and item_id not in merge_ids:
			plt.plot(buy_price_data['timestamp'].to_list(), buy_price_data['buy_price'].to_list(), color=rgb_outline, linewidth=1.7)
			plt.plot(sell_price_data['timestamp'].to_list(), sell_price_data['sell_price'].to_list(), color=rgb_outline, linewidth=1.7)
		elif isinstance(merge_ids, Collection) and item_id in merge_ids:
			plots[item_id] = [(buy_price_data['timestamp'].to_list(), buy_price_data['buy_price'].to_list()),
			                  sell_price_data['timestamp'].to_list(), sell_price_data['sell_price'].to_list()]
			plt.plot(wiki_data['timestamp'].to_list(), wiki_data['price'].to_list(), color=rgb, linewidth=1.5)
		plt.plot(x1, [y_min for _ in x1], color=rgb, linewidth=.7)
		plt.plot(x1, [y_max for _ in x1], color=rgb, linewidth=.7)
		# for color, label in zip([p_low, p_mid, p_high],
		#                         ['Buy price', 'Guide price', 'Sell price']):
		# 	print(label, color)
		# 	patches.append(mpatches.Patch(color=color, label=label))
		s_rgb = ''
		for v in rgb[:3]:
			s_rgb += f'{v:.1f}, '
		# patches.append(mpatches.Patch(color=rgb, label=name+f" color=[{s_rgb[:-2]}]"))
		patches.append(mpatches.Patch(color=rgb, label=name))
	# axs[0].set_title(plot_title[:-1], fontdict={'fontsize': 8.5}, pad=15.0)
	axs.set_title(plot_title[:-1], fontdict={'fontsize': 8.5}, pad=15.0)
	for vp in v_plots:
		plt.plot(vp[0], vp[1], color='grey', linewidth=.3)
	# print(patches)
	# patches.append(mpatches.Patch(color='grey', label='7-day vertical plot'))
	axs.legend(handles=patches)
	
	# delta_y = max(y_maxs) - min(y_mins)
	# axs.set_ylim(int(min(y_mins) - delta_y), int(max(y_maxs) + delta_y))
	# axs.set_ylabel('Amount traded (n)' if volume else 'Price (gp)')
	t_mid = (t0+t1)//2
	axs.set_xlabel("Date")
	x_ticks = [t0+3600-t0%3600, t_mid-t_mid%3600+3600, t1-t1%3600]
	axs.set_xticks(x_ticks)
	axs.set_xlim(t0, t1)
	
	axs.yaxis.set_major_formatter(format_n)
	axs.yaxis.set_minor_formatter(format_n)
	y0, y1 = min(y_mins), max(y_maxs)
	y_ = (y1-y0)*.25
	# axs.set_ylim(y0*.95, y1*1.05)
	axs.set_ylim(max(y0*.95, y0-y_), min(y1*1.05, y1+y_))

	fig.set_figheight(10)
	fig.subplots_adjust(hspace=0.25, wspace=0.5)
	fig.set_figwidth(8)
	
	fname = 'plots/plot_items/'
	for i in item_ids:
		fname += f'{i:0>5}_'
	plt.savefig(fname[:-1]+'.png')
	plt.show()
		


def plot_price(item_id: int, t0: int = 0, t1: int = time.time(), output_dir: File = None, vertical_plots: list = None,
               fig=None, axs=None):
	# for i in range(len(columns)):
	# 	print(columns[i], npy.ar[:, i], '\n')
	# print(npy)
	con = sqlite3.connect(path.f_db_scraped_src)
	sql_exe, values = 'SELECT COLUMNS FROM avg5m WHERE item_id=:id AND timestamp > :t0', {'id': item_id, 't0': t0}
	wiki_exe = 'SELECT timestamp, price, volume FROM wiki WHERE item_id=:id AND timestamp > :t0'
	price_data = pd.read_sql(sql=sql_exe.replace('COLUMNS', 'timestamp, buy_price, sell_price'), params=values, con=con)
	print(f'Loaded {len(price_data)} avg5m rows for item {id_name[item_id]}')
	wiki_data = pd.read_sql(sql=wiki_exe, params=values, con=con)
	buy_price_data = price_data.loc[price_data['buy_price'] > 0]
	sell_price_data = price_data.loc[price_data['sell_price'] > 0]
	
	global ylim_min, ylim_max, xlim_max, xlim_min
	
	y_min = min(buy_price_data['buy_price'].min(), sell_price_data['sell_price'].min(), ylim_min)
	y_max = max(price_data['buy_price'].max(), price_data['sell_price'].max(), ylim_max)
	horizontal_plots = [()]
	# x2, y2 = None, None
	del buy_price_data['sell_price'], sell_price_data['buy_price']
	
	# item_name = npy.item_name
	# timestamps = npy.extract_columns(['timestamp'])[1][:100]
	# print(ts_util.ts_to_dt(np.min(timestamps)), ts_util.ts_to_dt(np.max(timestamps)))
	volume=False
	# print(tses)
	name = id_name[item_id]
	avg_volume = int(np.average(wiki_data['volume'].to_numpy()))
	
	item_data = get_item_info(item_id=item_id, columns='item_name, buy_limit, members', con=con).to_dict('records')[0]
	item_data['average daily volume'] = format_n(avg_volume, max_decimals=1).replace(" ", "")
	# plot_title = f'{item_data.get("item_name")}\n'
	plot_title = f'Item: {item_data.get("item_name")}     ' \
	             f'{format_dt(ts_util.ts_to_dt(t0, utc_time=True), "%d-%m-%y %H:%M")} - ' \
	             f'{format_dt(ts_util.ts_to_dt(t1-t1%3600, utc_time=True), "%d-%m-%y %H:%M")} ' \
	             f'(~{(ts_util.ts_to_dt(t1, True)-ts_util.ts_to_dt(t0, True)).days} days)\n'
	for k in list(item_data.keys())[1:]:
		plot_title += f'[{k}: {item_data.get(k)}]  '
	# plot_title += f'\nTimespan: {ts_util.ts_to_dt(t0).date()} - {ts_util.ts_to_dt(t1).date()}'
	
	if fig is None or axs is None:
		fig, axs = plt.subplots(1)
	# axs.set_title(f'{name}\navg daily volume: {format_n(avg_volume)}', fontdict={'fontsize': 8.5}, pad=15.0)
	axs.set_title(plot_title[:-1], fontdict={'fontsize': 8.5}, pad=15.0)
	axs.set_xlabel("Date")
	axs.set_xlim(t0, t1)
	t_mid = (t0+t1)//2
	x_ticks = [t0+3600-t0%3600, t_mid-t_mid%3600+3600, t1-t1%3600]
	axs.set_xticks(x_ticks)
	axs.set_xlim(t0, t1)
	x0, x1 = buy_price_data['timestamp'].to_list(), sell_price_data['timestamp'].to_list()
	y0, y1 = buy_price_data['buy_price'].to_list(), sell_price_data['sell_price'].to_list()
	x2, y2 = wiki_data['timestamp'].to_list(), wiki_data['price'].to_list()
	x3, y3 = [min(x1), max(x1)], [y_min, y_min]
	x4, y4 = [min(x1), max(x1)], [y_max, y_max]
	print(y_min)
	
	# if np.average(y0[:int(len(y0)*.1)]) > np.average(y0[int(len(y0)*.9):]):
	# 	print('')
	# 	return
	axs.xaxis.set_major_formatter(ts_util.utc_ts_to_dt)
	axs.xaxis.set_minor_formatter(ts_util.utc_ts_to_dt)
	axs.yaxis.set_major_formatter(format_n)
	axs.yaxis.set_minor_formatter(format_n)
	delta_y = (y_max-y_min)*.15
	axs.set_ylim(int(y_min-delta_y), int(y_max+delta_y))
	axs.set_ylabel('Amount traded (n)' if volume else 'Price (gp)')
	plt.plot(x0, y0)
	plt.plot(x1, y1)
	plt.plot(x2, y2)
	plt.plot(x3, y3)
	plt.plot(x4, y4)
	
	blue_patch = mpatches.Patch(color='blue', label='Buy prices')
	orange_patch = mpatches.Patch(color='orange', label='Sell prices')
	green_patch = mpatches.Patch(color='green', label='Guide prices')
	purple_patch = mpatches.Patch(color='purple', label=f'Max price ({format_n(y_max, 1)})')
	red_patch = mpatches.Patch(color='red', label=f'Min price ({format_n(y_min, 1)})')
	patches = [blue_patch, orange_patch, green_patch, purple_patch, red_patch]
	if isinstance(vertical_plots, list):
		# vp = {'color': 'red', 'label':
		for vp in vertical_plots:
			if isinstance(vp, dict):
				c, l, v = vp.get('color'), vp.get('label'), vp.get('x')
			else:
				c, l, v = vp
			if not t1 >= v >= t0:
				print(f'Did not include vertical plot at x={v} as it exceeded min/max x_axis...')
				break
			red_patch = mpatches.Patch(color=c, label=l)
			patches.append(red_patch)
			plt.plot([v, v], [int(min(min(y0), min(y1)) * .85), 2147483648])
	axs.legend(handles=patches)
	league_start = ts_util.dt_to_ts(datetime.datetime(2023, 11, 15, 12, 0, 0))
	if output_dir is not None:
		if output_dir.exists():
			# plt.savefig(output_dir+name)
			pass
		else:
			# plt.show()
			print(f"Unable to save plot at non-existent folder {output_dir}")
	else:
		pass
		# plt.show()
	return fig, axs
		
		
def plot_leagues(item_id: int, league_data: list, output_dir: File, item_data: dict, df_wiki: pd.DataFrame,
                 df_avg5m: pd.DataFrame):
	"""
	Create a figure composed of N subplots, with each subplot being a price plot of `item_id` for that item during a
	specific league (N represents the amount of leagues). For each league, two vertical lines are plotted as well,
	representing the start and the end of the league.
	
	Parameters
	----------
	item_id : int
		The item_id of the data that should be plotted
	league_data : list
		A list with data for each league that has taken place so far.
	output_dir : str
		Folder in which the plots will be saved
	item_data : dict
		A dict with item metadata of the relevant item
	df_avg5m : pandas.DataFrame
		A pandas DataFrame with avg5m data relevant for the operations that are to be carried out
	df_wiki : pandas.DataFrame
		A pandas DataFrame with wiki data relevant for the operations that are to be carried out

	Returns
	-------

	"""
	# for i in range(len(columns)):
	# 	print(columns[i], npy.ar[:, i], '\n')
	# print(npy)
	plt.close()
	print('plotting leagues...')
	plot_id, n_plots = 0, len(league_data)
	fig, a = plt.subplots(min(2, n_plots), math.ceil(n_plots / 2))
	t0s = [0] + [d[2] for d in league_data]
	t0s.sort()
	# print(con.cursor().execute('SELECT * FROM wiki WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1', {'item_id': 2, 't0': 1699398000, 't1': 1702425600}).fetchall())
	# c = con.cursor()
	name = id_name[item_id]
	release_date = item_data.get('release_date')
	del item_data['release_date']
	for tag, league_name, t0, t1 in league_data:
		if t0 < release_date:
			continue
		# axs = a[int(plot_id/2), plot_id%2]
		idx_y, idx_x = int(plot_id/2), plot_id % 2
		vertical_plots = [
			('red',	f'{league_name} start ({format_dt(ts_util.ts_to_dt(t0), "%d-%m-%y")})', t0),
			('purple',	f'{league_name} end ({format_dt(ts_util.ts_to_dt(t1), "%d-%m-%y")})', t1)
		]
		if t1 > time.time():
			t1 = int(time.time())
			t1 = t1 + 86400 - t1 % 86400
			max_ts = t1
		else:
			# Extend the timespan by 7 days preceding the league and 14 days trailing the league
			max_ts = t1+86400*14
			if max_ts > time.time():
				max_ts = int(time.time())
				max_ts = max_ts + 86400 - max_ts % 86400
		min_ts = t0-86400*7
		
		# print(tag, min_ts, t0, t1, max_ts)
		# avg5m_sql = 'SELECT timestamp, buy_price, sell_price FROM avg5m WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1'
		# wiki_sql = 'SELECT timestamp, price, volume FROM wiki WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1'
		# values = {'item_id': item_id, 't0': min_ts, 't1': max_ts}
		
		wiki_data = df_wiki.loc[(df_wiki['timestamp'] >= min_ts) & (df_wiki['timestamp'] < max_ts)]
		
		
		volume = False
		temp_volume = wiki_data['volume'].to_numpy()
		if len(temp_volume) > 0:
			avg_volume = int(np.average(temp_volume))
		else:
			avg_volume = -1
		
		item_data['average daily volume'] = format_n(avg_volume, max_decimals=1).replace(" ", "")
		# plot_title = f'{item_data.get("item_name")}\n'
		plot_title = f'Item: {item_data.get("item_name")}\nLeague #{t0s.index(t0)}: {league_name}  ' \
		             f'{format_dt(ts_util.ts_to_dt(t0, utc_time=True), "%d-%m-%y")} - ' \
		             f'{format_dt(ts_util.ts_to_dt(t1-t1%3600, utc_time=True), "%d-%m-%y")} ' \
		             f'(~{(ts_util.ts_to_dt(t1, True)-ts_util.ts_to_dt(t0, True)).days} days)\n'
		print('plot title')
		print(plot_title)
		for k in list(item_data.keys())[1:]:
			plot_title += f'[{k}: {item_data.get(k)}]  '
		a[idx_y, idx_x].set_title(plot_title[:-1], fontdict={'fontsize': 11}, pad=15.0)
		a[idx_y, idx_x].set_xlabel("Date")
		t_mid = (min_ts+max_ts)//2
		x_ticks = [min_ts+3600-min_ts % 3600, t_mid-t_mid % 3600+3600, max_ts-max_ts % 3600]
		a[idx_y, idx_x].set_xticks(x_ticks)
		a[idx_y, idx_x].set_xlim(min_ts, max_ts)
		
		patches = []
		if t0 > min_avg5m_ts:
			# price_data = pd.read_sql(sql=avg5m_sql, params=values, con=sqlite3.connect(path.f_db_scraped_src))
			price_data = df_avg5m.loc[(df_avg5m['timestamp'] >= min_ts) & (df_avg5m['timestamp'] < max_ts)]
			buy_price_data = price_data.loc[price_data['buy_price'] > 0]
			sell_price_data = price_data.loc[price_data['sell_price'] > 0]
			del buy_price_data['sell_price'], sell_price_data['buy_price']
			x0, x1 = buy_price_data['timestamp'].to_list(), sell_price_data['timestamp'].to_list()
			y0, y1 = buy_price_data['buy_price'].to_list(), sell_price_data['sell_price'].to_list()
			a[idx_y, idx_x].plot(x0, y0, 'blue')
			a[idx_y, idx_x].plot(x1, y1, 'orange')
			patches.append(mpatches.Patch(color='blue', label='Buy prices'))
			patches.append(mpatches.Patch(color='orange', label='Sell prices'))
		else:
			x0, x1, y0, y1 = [], [], [], []
		x2, y2 = wiki_data['timestamp'].to_list(), wiki_data['price'].to_list()
		a[idx_y, idx_x].plot(x2, y2, 'green')
		y_list = y0 + y1 + y2
		a[idx_y, idx_x].xaxis.set_major_formatter(ts_util.utc_ts_to_dt)
		a[idx_y, idx_x].xaxis.set_minor_formatter(ts_util.utc_ts_to_dt)
		a[idx_y, idx_x].yaxis.set_major_formatter(format_n)
		a[idx_y, idx_x].yaxis.set_minor_formatter(format_n)
		a[idx_y, idx_x].set_ylim(int(min(int32_max, min(y_list)*.85)), max(1.1*np.sort(y_list)[:int(0.99*(len(y_list)))]))
		a[idx_y, idx_x].set_ylabel('Amount traded (n)' if volume else 'Price (gp)')
		
		patches.append(mpatches.Patch(color='green', label='Guide prices'))
		
		if isinstance(vertical_plots, list):
			# vp = {'color': 'red', 'label':
			for vp in vertical_plots:
				if isinstance(vp, dict):
					c, l, v = vp.get('color'), vp.get('label'), vp.get('x')
				else:
					c, l, v = vp
				if not t1 >= v >= t0:
					# print(f'Did not include vertical plot at x={v} as it exceeded min/max x_axis...')
					break
				next_patch = mpatches.Patch(color=c, label=l)
				patches.append(next_patch)
				a[idx_y, idx_x].plot([v, v], [int(min(y0+y1+y2) * .85), int32_max], color=c)
		a[idx_y, idx_x].legend(handles=patches)
	
		plot_id += 1
	fig.set_figheight(15)
	fig.subplots_adjust(hspace=0.25, wspace=0.5)
	fig.set_figwidth(15)
	if output_dir is not None:
		if output_dir.exists():
			plt.savefig(f'{output_dir}{name}.png')
			print(f'saved plot as {output_dir}{name}.png')
			# plt.savefig(f_jpeg)
			# Image.open(f_png).save(f_jpg, 'JPEG')
			plt.close()
		else:
			raise FileNotFoundError(f"Unable to save plot at non-existent folder {output_dir}")
	else:
		plt.show()
	

def item_prices_graph(item_id: int, graph_id: int = -1, ):
	"""
	Create 4 graphs for the given item id using data from the numpy archives;
	Graph 1 (top-left): buy_price, sell_price, wiki_price for each time stamp for the past week
	Graph 2 (top-right): buy_volume, sell_volume per hour of day (averaged)
	Graph 3 (bot-left):  buy_price, sell_price, wiki_price for each time stamp for the entire timespan
	Graph 4 (bot-right): buy_volume, sell_volume per day-of-week
	
	The graphs that are produces should provide a comprehensive overview of the scraped data for the given item.
	:param item_id: Input item_id
	:return:
	"""
	
	
	return


def item_prices_entry_as_graph(item: NpyArray, t0: int, t1: int, step_size: int = 14400, index_threshold: float = .1):
	"""
	Convert an item prices listbox entry to a prices graph of that data. First, the data is prepared
	Ideas;
	vertical plot at 12 am each day, of which the coloring is based on the day of week
	Plot a datapoint for each 4 hour interval (guide price, runelite prices, min+max realtime prices (or runelite if
	realtime data is missing)
	Plot th
	
	Parameters
	----------
	item : NpyArray
		Loaded numpy array object
	t0 : int
		Lowest ts
	t1 : int
		highest ts
	step_size : int
		Interval step size; used for total interval size, but also for rounding interval start and end timestamp values
	index_threshold : float, optional, .1 by default
		Index to use within a sorted prices list for buy and sell prices. E.g. .1 translates to using the buy price
		ranked at 10% in a sorted list, and a sell price ranked at 90% in a sorted list.

	Returns
	-------
	
	TODO: At what point is there insufficient data available for creating a graph?
	
	TODO: Which realtime values should I plot per 4h interval
	value range: min, 10%, q1, median, q3, 90%, max
	n_rt: amount of elements
	
	
	
	"""
	tsp = TimeSeriesPlot(item=item, y_config=Axis(label='Price (gp)', minor_format=format_n, major_format=format_n),
	                     vline_graph=Graph([0], [0], (0, 0, 0, .3), patch_label='4-hour intervals', line_width=.6),
	                     ylim_multipliers=[.98, 1.02])
	vertical_plots = [
		# ('red', f'{league_name} start ({format_dt(ts_util.ts_to_dt(t0), "%d-%m-%y")})', t0),
		# ('purple', f'{league_name} end ({format_dt(ts_util.ts_to_dt(t1), "%d-%m-%y")})', t1)
	]
	# patches.append(mpatches.Patch(color='blue', label='Buy prices'))
	# patches.append(mpatches.Patch(color='orange', label='Sell prices'))
	
	con = sqlite3.connect(path.f_db_scraped_src)
	cursor = con.cursor()
	# fig, a = plt.subplots(min(2, n_plots), math.ceil(n_plots / 2))
	# avg5m = pd.read_sql(sql="SELECT * FROM avg5m WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1",
	#                     params={'item_id': item.item_id, 't0': t0}, con=con)
	
	dow_plots = []
	dow_colors = [None]*7
	intervals = []
	tses, mins, q1s, medians, q3s, maxes, guide_prices, buy_prices, sell_prices = [], [], [], [], [], [], [], [], []
	
	# In each iteration, data is computed for the interval
	for i_start in range(t0, t1+1, step_size):
		i_start = i_start - i_start % step_size
		i_end = i_start + step_size
		if i_end > time.time():
			break
		# print(f'Current interval: {ts_util.ts_to_dt(i_start)} - {ts_util.ts_to_dt(i_end)}')
		
		stats = {}
		
		# Generate a vertical line
		# if i_start % 86400 == 0:
		# 	dow = i_start // 86400 % 7
		# 	rgb = dow_rgb[i_start // 86400 % 7]
		# 	rgb = tuple([el / 255 for el in dow_rgb[i_start // 86400 % 7]])
		# 	print(dow, rgb)
		# 	dow_plots.append({
		# 		'rgb': rgb,
		# 		'x': (i_start, i_start),
		# 		'y': (0, int32_max)})
		#
		# 	# Patches for each dow
		# 	if dow_colors[dow] is None:
		# 		dow_colors[dow] = mpatches.Patch(color=rgb, label=dow_list[dow])
		#
		# 	# Add vertical marker to mark start of a day; use color tied to DoW
		# 	patches.append(mpatches.Patch(color='blue', label='Buy prices'))
		g = get_graph_interval(npy_array=item, t0=i_start, t1=i_end,
		                       y_values=['buy_price', 'sell_price', 'wiki_price', 'wiki_volume', 'flip_buy', 'flip_sell'])
		# util.get_value_range(values=g.get('buy_price'))
		# print(list(g.keys()))
		potential_profit = []
		
		ts, p, v, bp, sp = g.get('timestamp'), g.get('wiki_price'), g.get('wiki_volume'), g.get('buy_price'), g.get('sell_price')
		# print(p)
		# print(sp)
		# print(len(sp))
		# print(len(sp[np.nonzero(sp)]))
		# print(bp)
		
		# Realtime prices shows a distribution of actually logged prices
		rt_p = load_realtime_entries(item_id=item.item_id, t0=i_start, t1=i_end, c=cursor)
		rt_b, rt_s = rt_p.loc[rt_p['is_sale']==0], rt_p.loc[rt_p['is_sale']==1]
		# print(rt_p)
		try:
			rt_price_list = ge_util.get_value_range(values=rt_p['price'].to_numpy(), indices=(0, .1, .25, .35, .5, .65, .75, .9, 1.0), as_list=True)
		except IndexError:
			continue
		# print()
		# print(util.get_value_range(values=rt_b['price'].to_numpy(), indices=(0, .1, .25, .35, .5, .65, .75, .9, 1.0), as_list=True))
		# print(util.get_value_range(values=rt_s['price'].to_numpy(), indices=(0, .1, .25, .35, .5, .65, .75, .9, 1.0), as_list=True))
		bp_f, sp_f = np.nonzero(bp), np.nonzero(sp)
		# intervals.append({
		# 	'i_start': ts_util.ts_to_dt(i_start),
		# 	'i_end': ts_util.ts_to_dt(i_end),
		# 	'min': rt_price_list[0],
		# 	'q1': rt_price_list[2],
		# 	'median': rt_price_list[4],
		# 	'q3': rt_price_list[6],
		# 	'max': rt_price_list[8],
		# 	'buy_low': np.sort(bp[bp_f])[int(index_threshold*len(bp[bp_f]))],
		# 	'sell_high': np.sort(sp[sp_f])[int((1-index_threshold)*len(sp[sp_f]))]
		# })
		try:
			tses.append(i_start)
			mins.append(rt_price_list[0])
			q1s.append(rt_price_list[2])
			medians.append(rt_price_list[4])
			q3s.append(rt_price_list[6])
			maxes.append(rt_price_list[8])
			
			buy_prices.append(np.sort(bp[bp_f])[int(index_threshold*len(bp[bp_f]))])
			sell_prices.append(np.sort(sp[sp_f])[int((1-index_threshold)*len(sp[sp_f]))])
			guide_prices.append(int(np.average(p)))
		except IndexError:
			tses = tses[:len(guide_prices)]
			mins = mins[:len(guide_prices)]
			medians = medians[:len(guide_prices)]
			maxes = maxes[:len(guide_prices)]
			buy_prices = buy_prices[:len(guide_prices)]
			sell_prices = sell_prices[:len(guide_prices)]
		
		
		
		# timestamp = i_end-i_start
		# for k, i in list(intervals[-1].items())[2:]:
		# 	plt.plot(tses[-1], i)
		# print(intervals[-1])
		# print(util.get_value_range(values=bp))
		continue
		
	# Value Range Patch Label
	vrpl = False
	col = [.2, .3, .8]
	tsp.add_graphs(graphs=Graph(x=tses, y=mins, color=tuple(col+[.42]),line_width=1.0, value_range_patch_label=vrpl))
	# tsp.add_graphs(graphs=Graph(x=tses, y=q1s, color=tuple(col+[.82]), line_width=2.0, line_marker='+', value_range_patch_label=vrpl))
	tsp.add_graphs(graphs=Graph(x=tses, y=medians, color=(0, 0, 0, 1), patch_label=f'Median ({format_n(min(medians))} - {format_n(max(medians))})', line_width=2.0, value_range_patch_label=vrpl))
	# tsp.add_graphs(graphs=Graph(x=tses, y=q3s, color=tuple(col+[.82]), patch_label=f'Q1-Q3 ({format_n(min(q1s))} - {format_n(max(q3s))})', line_width=2.0, line_marker='+', value_range_patch_label=vrpl))
	tsp.add_graphs(graphs=Graph(x=tses, y=maxes, color=tuple(col+[.42]), patch_label=f'Min-Max ({format_n(min(mins))} - {format_n(max(maxes))})', line_width=1.0, value_range_patch_label=vrpl))
	tsp.add_graphs(graphs=Graph(x=tses, y=buy_prices, color=(.8, .25, .25), patch_label=f'Buy prices ({format_n(min(buy_prices))} - {format_n(max(buy_prices))})', line_width=2.0, line_marker='+', value_range_patch_label=vrpl))
	tsp.add_graphs(graphs=Graph(x=tses, y=guide_prices, color=(.5, .5, .2), patch_label=f'Guide prices ({format_n(min(guide_prices))} - {format_n(max(guide_prices))})', line_width=2.0, line_marker='o', value_range_patch_label=vrpl))
	tsp.add_graphs(graphs=Graph(x=tses, y=sell_prices, color=(.25, .8, .25), patch_label=f'Sell prices ({format_n(min(sell_prices))} - {format_n(max(sell_prices))})', line_width=1.0, value_range_patch_label=vrpl))
	# tsp.spikes =
	tsp.plot_figure(vlines=[t - t % step_size for t in range(t0, t1, step_size)])
	# for k, i in list(a.__dict__.items()):
	# 	print(k, i)
	exit(1)
	# plt.plot(tses, mins, 'red')
	# patches.append(mpatches.Patch(color='red', label='Price (min)'))
	# plt.plot(tses, q1s)
	# plt.plot(tses, medians, 'black')
	# patches.append(mpatches.Patch(color='black', label='Price (median)'))
	# plt.plot(tses, q3s)
	# plt.plot(tses, maxes, 'green')
	# patches.append(mpatches.Patch(color='green', label='Price (max)'))
	a.legend(handles=patches)
	# plt.plot(tses, buy_prices)
	# plt.plot(tses, sell_prices)
		# print(g)
		# for k in list(g.keys()):
			# print(k)
			# print(f'{k}={item.print_column_description(k, False)}')
	plt.show()
	exit(123)
	# 	# TODO: Alternative data if realtime is not available
	# 	try:
	# 		g_rt = load_realtime_entries(item_id=item.item_id, t0=i_start, t1=i_end, c=cursor)
	# 		if len(g_rt) >= 20:
	# 			rt_5p = int(len(g_rt) / 20)
	# 		else:
	# 			continue
	# 		print(list(g.get('buy_price')))
	# 		print(list(g.get('sell_price')))
	# 		rt_b, rt_s = g_rt.loc[g_rt['is_sale'] == 0], g_rt.loc[g_rt['is_sale'] == 1]
	# 		# rt_b = [(e.get('timestamp'), e.get('price')) for e in rt_b.to_dict('records')]
	# 		# rt_s = [(e.get('timestamp'), e.get('price')) for e in rt_s.to_dict('records')]
	# 		if len(rt_b) == 0:
	# 			print('No realtime buy data available for this interval')
	# 		rt_s = []
	# 		if len(rt_s) == 0:
	# 			print('No realtime sell data available for this interval')
	# 		rt_m = np.sort(g_rt['price'].to_list())#+list(g.get('buy_price'))+list(g.get('sell_price')))
	# 		if len(rt_m) > 0:
	# 			rt_min, rt_median, rt_max = rt_m[0], rt_m[rt_5p*10], rt_m[-1]
	# 			rt_10, rt_q1, rt_q3,  rt_90 = rt_m[rt_5p*2], rt_m[rt_5p*5], rt_m[rt_5p*-5], rt_m[rt_5p*-2]
	# 			print(rt_min, rt_10, rt_q1, rt_median, rt_q3, rt_90, rt_max)
	# 	except ValueError:
	# 		pass
	# 	# print(all_prices[np.nonzero(all_prices > 0)])
	# 	# item.timestamp[np.nonzero(item.timestamp >= t_max - 86400 - t_max % 86400)]
	# 	print(len(rt_b), len(rt_s))
	# 	exit(1)
	# for p in dow_plots:
	# 	print(p)
	# 	a.plot([p.get('x'), p.get('x')], [p.get('y')[0], p.get('y')[1]], color=p.get('rgb'), linewidth=.1)
	# plt.show()
	# exit(1)


if __name__ == "__main__":
	i = Item.create_table(2)
	for n,v in i.__dict__.items():
		print(n, v)
	exit(1)
	
	t0, t1 = dt_to_ts(datetime.datetime(2024, 2, 1)), dt_to_ts(datetime.date.today())
	v_step = t1 - t0
	if v_step > 90 * 86400:
		v_step = 30
	elif v_step > 21 * 86400:
		v_step = 7
	else:
		v_step = 1
	
	v_plots = [[(t - t % 86400, t - t % 86400), (0, int32_max)] for t in range(t0, t1, v_step * 86400)]
	plot_items(item_ids=[5952, 12934, 12905], t0=t0, t1=t1, v_step=v_step, y_multiplier=[1, 20, 1])
	exit(12345)
	
	t = int(time.time())
	item_name = "Masori chaps (f)"
	item_id = name_id.get(item_name)
	t_start, t_end = dt_to_ts(datetime.datetime(2024, 1, 1, 1)), dt_to_ts(datetime.datetime(2024, 1, 15, 1))
	# item_prices_entry_as_graph(item=NpyArray(item_id), t0=t-t%86400-86400*7, t1=t-t%14400)
	item_prices_entry_as_graph(item=NpyArray(item_id), t0=t_start, t1=t_end)
	exit(1)
	i = 27238
	# plot_price(item_id=27238, t0=ts_util.dt_to_ts(datetime.datetime(2022, 8, 1)), t1=int(time.time()))
	# plot_leagues(item_id=27238, league_data=league_timestamps)
	con = sqlite3.connect(path.f_db_scraped_src)
	avg5m_sql = 'SELECT timestamp, buy_price, sell_price FROM avg5m WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1'
	wiki_sql = 'SELECT timestamp, price, volume FROM wiki WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1'
	values = {
		'item_id': i,
		't0': ts_util.dt_to_ts(datetime.datetime(2022, 9, 1)),
		't1': int(time.time())
	}
	
	
	t0, t1 = dt_to_ts(datetime.datetime(2022, 11, 1)), int(time.time())
	v_step = t1 - t0
	if v_step > 90*86400:
		v_step = 30
	elif v_step > 21 * 86400:
		v_step = 7
	else:
		v_step = 1
	
	v_plots = [[(t-t%86400, t-t%86400), (0, int32_max)] for t in range(t0, t1, v_step*86400)]
	plot_items(item_ids=[25985, 27241], t0=t0, t1=t1, v_step=v_step)
	exit(12345)
	