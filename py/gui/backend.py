"""
This module contains implementations for gui-related backend operations, designed to supply GUI components with data.
Each GUI listbox has its own method for fetching rows that is defined within this module.
"""
import datetime
import os
import sqlite3
import time

import numpy as np
import pandas as pd

import global_values
import path as p
from ge_util import get_reverse_remaps, dict_factory
from global_values import npyar_items, months_tuple, id_name, realtime_prices, db_itemdb
from ledger import InventoryEntry, Inventory
from path import load_data, f_update_prices_listbox_flag as f_listbox_updating
from runelite_reader import generate_bond_log
from str_formats import format_int
from ts_util import dt_to_ts, ts_to_dt

# from resources import val
reverse_remaps = get_reverse_remaps()


def get_entries_listbox_inventory_top(inventory: Inventory, columns: list):
	"""
	Fetch rows to fill the top inventory listbox with. The output is the inventory viewer that shows stats per item
	grouped.
	:param inventory: Inventory object used as data structure
	:param columns: ListboxColumns used to specify the values needed
	:return:
	"""
	cols = [c.df_column for c in columns]
	entries, augmented, new_ids, rbpi_con, rbpi_c, def_vals = [], [], [], None, None, None
	# Iterate over item_ids; each iteration should produce one entry
	for next_id in list(inventory.results.keys()):
		try:
			e = inventory.results.get(next_id)
			e.update_values()
			e = e.__dict__
			cur = {v: int(e.get(v)) if isinstance(e.get(v), float) else e.get(v) for v in cols if e.get(v) is not None}
			cur['value'] = cur.get('quantity') * cur.get('price')

			cur['margin'] = cur.get('buy_limit') * (cur.get('current_sell') - cur.get('current_buy') -
			                                        min(5000000, int(0.01*cur.get('current_sell'))))
			entries.append(cur)
		except TypeError:
			print('typeerror', e.get('item').__dict__)
			print(cols)
			if next_id in new_ids:
				print(f'{next_id} was already added to rbpi db')
				continue
			if rbpi_con is None:
				rbpi_con = sqlite3.connect(p.f_rbpi_itemdb)
			rbpi_con.row_factory = dict_factory
			if def_vals is None or rbpi_c is None:
				rbpi_c = rbpi_con.cursor()
				def_vals = rbpi_c.execute("SELECT * FROM itemdb WHERE item_id=2").fetchone()
			values = {k: global_values.db_default_values.get(global_values.itemdb_dtypes.get(k).lower())
			          for k in list(def_vals.keys())}
			values['id'], values['item_id'] = next_id, next_id
			tuple_str, sql_dict = '(', '('
			for el in list(values.keys()):
				tuple_str += f':{el}, '
			tuple_str = tuple_str[:-2] + ')'
			sql = f'INSERT INTO itemdb ' + tuple_str.replace(':', '') + ' VALUES ' + tuple_str
			
			try:
				rbpi_c.execute(sql, values)
				rbpi_con.commit()
				print(f'added {next_id} to rbpi itemdb')
				time.sleep(2)
			except sqlite3.IntegrityError:
				pass
			new_ids.append(next_id)
	if isinstance(rbpi_con, sqlite3.Connection):
		rbpi_con.close()
	
	return entries


def get_entries_listbox_inventory_bottom(target_id: int, inventory: Inventory, columns: list):
	""" Fetch a list of entries for the bottom Inventory listbox.
	The returned list is composed of transactions involving the specified `target_id`.
	
	Parameters
	----------
	target_id : int
		The item_id for which all transactions should be returned
	inventory : Inventory
		The inventory object that contains all transactions
	columns : list
		A list of listbox columns that dictate which elements should be included in each row.

	Returns
	-------
	list
		A list of dicts, where each dict contains the elements specified by columns

	"""
	transactions = inventory.results.get(target_id)
	if not isinstance(transactions, InventoryEntry):
		return
	t_list = pd.DataFrame(inventory.ledger.transactions)
	t_list = t_list.loc[t_list['item_id'] == target_id]
	return inventory.augment_transactions(df=t_list, columns=columns)


def entry_from_id(item_id: int, lbe: dict, buy_keys: list, current_prices: list, i: InventoryEntry = None,
                  buy_list_entry: int = None, min_ts: int = int(time.time()-86400*14), force_ts: int = None):
	""" Combine given data into a listbox entry dict for the top item prices listbox in the Inventory tab """
	try:
		lbe['current_sell'] = max(current_prices) if isinstance(current_prices, tuple) else current_prices
	except OSError:
		lbe['current_sell'] = 0
	lbe['item_name'] = id_name[item_id]
	if buy_keys is None:
		buy_keys = [k for k in list(lbe.keys()) if k[:2] == 'b_']
		print(f'Using buy_keys {buy_keys}')
	buy_prices = [lbe.get(k) for k in buy_keys if lbe.get(k) is not None]
	try:
		lbe['buy_low'], lbe['buy_high'] = min(buy_prices), max(buy_prices)
	except TypeError:
		print(id_name[item_id])
		print(buy_prices)
		exit(-1)
		# pass
	except ValueError:
		lbe['buy_low'], lbe['buy_high'] = -1, -1
	
	lbe['last_buy'], lbe['last_sell'], lbe['last_traded'] = -1, -1, -1
	lbe['buy_list'] = -1 if buy_list_entry is None else buy_list_entry
	if isinstance(i, InventoryEntry):
		try:
			t_df = i.df.sort_values(by=['timestamp'], ascending=[False])
			t_df = t_df.loc[(t_df['timestamp'] >= min_ts)]
			if len(t_df) > 0:
				lbe['last_traded'] = t_df['timestamp'].max()
				last_buy = t_df.loc[t_df['is_buy']]
				if len(last_buy) > 0:
					lbe['last_buy'] = last_buy.iloc[0]['price']
				last_sell = t_df.loc[~t_df['is_buy']]
				if len(last_sell) > 0:
					lbe['last_sell'] = last_sell.iloc[0]['price']
		except AttributeError:
			pass
	if force_ts is not None:
		lbe['last_traded'] = force_ts
	return lbe
	
	
def get_entries_listbox_prices_24h_top(item_ids: list = None, inv: Inventory = None, buy_list: dict = None):
	""" Fetch data regarding price developments for the list of item_ids over the past 24 hours
	
	Parameters
	----------
	item_ids : list, optional
		List of item_ids the listbox should be filled with, by default derive this list from tracked items csv file.
	inv : Inventory, optional, by default
		Inventory object with the current Inventory computed from all transactions
	buy_list : dict, optional, by default is None
		Dict with manually configured buy_list prices

	Returns
	-------
	list
		A list of dicts that can be further formatted into listbox rows

	"""
	# if item_ids is None:
	# 	item_ids = npy_archive_items
	
	cp, entries, min_ts = realtime_prices, {}, int(time.time()-86400*14)
	data = get_prices_listbox_entries()
	
	# data is empty, somehow. Generate new entries
	if len(data) == 0:
		data = get_prices_listbox_entries()
	
	buy_keys = []
	# print(data)
	for k in list(data.get(2)[0].keys()):
		try:
			if k[:2] == 'b_':
				buy_keys.append(k)
		except TypeError:
			print(f'typeerror buy_list {k}')
	for item_id in list(data.keys()) if item_ids is None else item_ids:
		# If the entry already exists or if it is an item that would be remapped, skip it
		if entries.get(item_id) is not None:
			continue
		
		# buy list entry
		ble = None if buy_list is None else buy_list.get(id_name[item_id])
		entries[item_id] = entry_from_id(item_id=item_id, lbe=data.get(item_id)[0], buy_keys=buy_keys,
		                                 current_prices=cp.get(item_id),
		                                 i=inv.results.get(item_id) if inv is not None else None,
		                                 buy_list_entry=ble, min_ts=min_ts)
		if reverse_remaps.get(item_id) is not None:
			try:
				rev_id = reverse_remaps.get(item_id)
				ble = None if buy_list is None else buy_list.get(id_name[rev_id])
				entries[rev_id] = entry_from_id(item_id=rev_id, lbe=data.get(rev_id)[0], buy_keys=buy_keys,
				                                current_prices=cp.get(rev_id), i=None, buy_list_entry=ble,
				                                min_ts=min_ts, force_ts=entries.get(item_id).get('last_traded'))
			except TypeError:
				pass
	return [entries.get(k) for k in list(entries.keys())]
	
	
def get_entries_listbox_prices_24h_bot(item_id: int):
	""" Fetch multiple 24h analysis rows for a single item
	
	Almost identical to the top listbox, although the bottom listbox provides a more in-depth insight into a single item
	rather than multiple items. List of rows can be used to see how the price has developed over the past days
	
	Parameters
	----------
	item_id : int
		item_id for which the entries should be fetched

	Returns
	-------
	A list of entries for the given item_id.

	"""
	# cp, cols, entries = get_realtime_prices(), [c.df_column for c in columns] + ['profit'], []
	data = get_prices_listbox_entries()
	return data.get(item_id)


def inventory_listbox_per_month(timeline: dict = load_data(path='data/test_data.dat'), t0: int = 0,
                              t1: int = int(time.time()), sort_by: list = None, sort_asc: list = None):
	"""
	Fetch rows with monthly results for the top listbox in the inventory tab
	:return: A list of values, where each element represents data grouped per month
	"""
	# save_data(timeline, path='data/test_data.dat')
	rows = []
	# print(timeline)
	df = pd.DataFrame([timeline.get(k) for k in list(timeline.keys()) if t1 > dt_to_ts(k) >= t0]).sort_values(by=['date'], ascending=[False])
	bond_log = pd.DataFrame([{'ts': ts_to_dt(el[0]), 'v': el[1] * el[2], 'q': el[2]}
	                         for el in generate_bond_log()])
	bond_log['yyyymm'] = bond_log['ts'].apply(lambda d: d.year * 100 + d.month)
	df['yyyymm'] = df['date'].apply(lambda d: d.year * 100 + d.month)
	
	for yyyymm in np.flip(np.unique(df['yyyymm'].to_numpy())):
		sub = df.loc[df['yyyymm'] == yyyymm]
		del sub['date'], sub['yyyymm'], sub['day']
		row = {'month_id': yyyymm}
		row.update({col: int(np.sum(sub[col].to_numpy())) for col in sub.column_list})
		b_sub = bond_log.loc[bond_log['yyyymm'] == yyyymm]
		row['bond_value'], row['n_bonds'] = int(np.sum(b_sub['v'].to_numpy())), int(np.sum(b_sub['q'].to_numpy()))
		# print(sub)
		rows.append(row)
	sort_by = ['month_id'] if sort_by is None else sort_by
	sort_asc = [False for _ in sort_by] if sort_asc is None else sort_asc
	print(sort_by, sort_asc)
	return pd.DataFrame(rows).sort_values(
		by=['month_id'] if sort_by is None else sort_by,
		ascending=[False for _ in sort_by] if sort_asc is None else sort_asc).to_dict('records')


def get_entries_listbox_daily_top(timeline: dict = load_data(path='data/test_data.dat'), t0: int = 0,
                                  t1: int = int(time.time()), sort_by: list = None, sort_asc: list = None,
                                  group_by: str = 'd'):
	"""	Fetch transaction data and group it by day and sort it according to input parameters
	The order and subset of returned entries can be specified with sort_by, sort_asc, t0 and t1 parameters. By default,
	entries are sorted chronologically from new to old and they are not filtered within a timestamp frame.
	
	Parameters
	----------
	timeline : dict
		Input timeline dict with one entry for each day.
	t0 : int
		Lower timestamp bound
	t1 : int
		Upper timestamp bound
	sort_by : list, optional
		Value the resulting rows should be sorted on
	sort_asc : list, optional
		List of bools to indicate for the corresponding sort_by value whether it should be sorted ascending or not
	group_by : str, optional, 'd' by default
		Char indicating by which time unit the rows should be grouped; 'd' for 'day', 'm' for month or 'y' for year

	Returns
	-------
	stats : dict
		Dict with various aggregated stats, grouped by day.

	"""
	# print(list(timeline.keys()))
	df = pd.DataFrame([timeline.get(day).get('stats') for day in list(timeline.keys())
	                   if t1 > day >= t0])
	# print(df.columns)
	# print(df['n_buy'])
	# print(df.iloc[:5])
	if sort_by is None or False in [s in df.columns for s in sort_by]:
		sort_by = ['t0']
		sort_asc = [False]
	# print(df.iloc[-5:])
	stats, subset = [], []
	df = df.sort_values(by=['t0'] if sort_by is None else sort_by,
	                    ascending=[False for _ in sort_by] if sort_asc is None else sort_asc)
	
	if group_by == 'd':
		for row in df.to_dict('records'):
			as_dt = ts_to_dt(row.get('t0'))
			row['day'] = as_dt.weekday()
			stats.append(row)
			# subset.append(str(el.get('subset')))
	elif group_by == 'm':
		columns = ['n_buy', 'invested', 'n_sell', 'returns', 'profit', 'tax']
		df['month'] = df['date'].apply(lambda d: d[-5:])
		for m in np.unique(df['month'].to_numpy()):
			df_ = df.loc[df['month'] == m]
			t0 = dt_to_ts(datetime.datetime(int('20'+m.split('-')[-1]), int(m.split('-')[0]), 2))
			temp = {col: int(np.sum(df_[col].to_numpy())) for col in columns}
			temp.update({'date': m, 't0': t0-t0%86400, 't1': df_['t1'].max()})
			stats.append(temp)
	elif group_by == 'y':
		columns = ['n_buy', 'invested', 'n_sell', 'returns', 'profit', 'tax']
		df['year'] = df['date'].apply(lambda d: d[-2:])
		for y in np.unique(df['year'].to_numpy()):
			df_ = df.loc[df['year'] == y]
			t0 = dt_to_ts(datetime.datetime(int(f'20{y}'), 1, 2))
			temp = {col: int(np.sum(df_[col].to_numpy())) for col in columns}
			temp.update({'date': y, 't0': t0-t0%86400, 't1': df_['t1'].max()})
			stats.append(temp)
			
	return stats


def sum_stats(rows, m, y):
	n_buy, invested, n_sell, returns, profit, tax = 0, 0, 0, 0, 0, 0
	for e in rows:
		n_buy += e.get('n_buy')
		invested += e.get('invested')
		n_sell += e.get('n_sell')
		returns += e.get('returns')
		profit += e.get('profit')
		tax += e.get('tax')
	return {
		'date': f"{months_tuple[m][:3]} {y % 100}",
		'n_buy': format_int(n_buy),
		'invested': format_int(invested),
		'n_sell': format_int(n_sell),
		'returns': format_int(returns),
		'profit': format_int(profit),
		'tax': format_int(tax)
	}


def get_entries_listbox_monthly_top(timeline: dict = load_data(path='data/test_data.dat'), t0: int = 0,
                                  t1: int = int(time.time()), sort_by: list = None, sort_asc: list = None):
	"""	Fetch transaction data and group it by month and sort it according to input parameters
	The order and subset of returned entries can be specified with sort_by, sort_asc, t0 and t1 parameters. By default,
	entries are sorted chronologically from new to old and they are not filtered within a timestamp frame.
	Example entry:
	'date': 'Oct 23', 'n_buy': '810 ', 'invested': '6875M', 'n_sell': '504 ', 'returns': '5533M',
	'profit': '425M', 'tax': '50M'
	to do: add bonds
	
	Parameters
	----------
	timeline : dict
		Input timeline dict with one entry for each day.
	t0 : int
		Lower timestamp bound
	t1 : int
		Upper timestamp bound
	sort_by : list, optional
		Value the resulting rows should be sorted on
	sort_asc : list, optional
		List of bools to indicate for the corresponding sort_by value whether it should be sorted ascending or not

	Returns
	-------
	stats : dict
		Dict with various aggregated stats, grouped by month.

	"""
	# print(list(timeline.keys()))
	df = pd.DataFrame([timeline.get(day).get('stats') for day in list(timeline.keys())
	                   if t1 > day >= t0])
	# print(df.columns)
	# print(df['n_buy'])
	# print(df.iloc[:5])
	if sort_by is None or False in [s in df.columns for s in sort_by]:
		sort_by = ['t0']
		sort_asc = [False]
	# print(df.iloc[-5:])
	stats, subset = [], []
	df = df.sort_values(by=['t0'] if sort_by is None else sort_by,
	                    ascending=[False for _ in sort_by] if sort_asc is None else sort_asc)
	previous_month, previous_year, per_month = -1, -1, []
	for row in df.to_dict('records'):
		as_dt = ts_to_dt(row.get('t0'))
		row['day'] = as_dt.weekday()
		m = as_dt.month

		stats.append(row)

		if previous_month == -1:
			previous_month, previous_year = m, as_dt.year
		elif previous_month != m:
			stats.append(sum_stats(rows=per_month, m=previous_month, y=previous_year))
			print(stats[-1])
			previous_month, previous_year, per_month = m, as_dt.year, []
		per_month.append(row)
	stats.append(sum_stats(rows=per_month, m=previous_month, y=previous_year))
	print(stats[-1])
	return stats


def inventory_totals(timeline: dict = load_data('data/test_data.dat'), t0: int = 0, t1: int = int(time.time())):
	""" Given a timeline dict, return statistics originating from entries within the given lower/upper timestamp bounds
	Parameters
	----------
	timeline : dict
		Input timeline dict from which the data is drawn
	t0 : int, optional
		Lower timestamp bound (def=0)
	t1 : int, optional
		Upper timestamp bound (def=int(time.time())

	Returns
	-------
	dict
		Dict with entries that are the result of summing every entry sharing its name from the timeline

	"""
	bond_log = pd.DataFrame([{'ts': el[0], 'v': el[1] * el[2], 'q': el[2]}
	                         for el in generate_bond_log() if t1 > el[0] >= t0])
	df = pd.DataFrame([timeline.get(k) for k in list(timeline.keys()) if t1 > dt_to_ts(k) >= t0])
	dts = [d for d in df['date'].to_list()]
	df['days_passed'] = (max(dts) - min(dts)).days
	df['bond_value'] = df['date'].apply(lambda d: int(np.sum(
		bond_log.loc[(bond_log['ts'] >= dt_to_ts(d)) & (dt_to_ts(d)+86400 > bond_log['ts'])]['v'].to_numpy())))
	del df['day'], df['date']
	df.to_csv('data/testdf.csv')
	return {col: int(np.sum(df[col].to_numpy(na_value=0))) for col in df.columns}


def get_transactions_by_dt(items: dict, target_dt: datetime.datetime, buy: bool = True, sell: bool = True):
	transactions = []
	for next_item in list(items.keys()):
		cur = items.get(next_item).transactions
		to_add = [cur.get(t) for t in list(cur.keys()) if cur.get(t).get('timestamp').date() == target_dt.date()]
		for t in to_add:
			t.update({'item_id': next_item})
			transactions.append(t)
		
	df = pd.DataFrame(transactions).sort_values(by=['timestamp', 'item_id'])
	if not buy:
		df = df.loc[~df['buy']]
	if not sell:
		df = df.loc[df['buy']]
	# for t in df.to_dict('records'):
	# 	print(t)
	return df.to_dict('records')


def get_prices_listbox_entries() -> dict:
	"""
	Fetch entries for the price listbox, return the loaded file as it is and run a check if it should be updated. If the
	 file is eligible for updating, print this.
		
	Returns
	-------
	dict
		Return a dict with an entry for each item_id, where each entry is a list of listbox rows, with `n_days` rows
		
	See Also
	--------
	data_preprocessing.update_
	
	Notes
	-----
	Creating the entries can take quite some time, depending on amount of items and days. The most convenient moment to
	do so is probably right after updating numpy arrays. As update time depends on amount of items and days, it may be
	useful to create a subset of potentially useful items and use that as input, rather than creating entries for each
	possible item.

	"""
	results = None
	try:
		results = load_data(p.f_tracked_items_listbox)
		
		# Update the listbox entries asynchronously, return the out-of-date version while the entries are updating
		if os.path.getmtime(p.f_tracked_items_listbox) < os.path.getmtime(p.f_np_archive_columns) and not \
				os.path.exists(f_listbox_updating) or time.time()-os.path.getmtime(f_listbox_updating) > 600:
			# save_data(time.time(), flag_file)
			# thread = AsyncTask(task=update_listbox_entries, callback_oncomplete=remove_update_listbox_flag)
			# thread.start()
			print('\t*** Listbox entries are eligible for updating! ***')
	except FileNotFoundError:
		pass
	return results


# exit(0)


if __name__ == "__main__":
	pass