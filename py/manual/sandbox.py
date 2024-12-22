import datetime
import os.path
import sqlite3
import time

import numpy as np
import pandas as pd

# import ts_util
# from filter import evaluate_numerical_filter
# from ge_util import augment_itemdb_entry
# from global_values import id_name, remap_dict, db_wiki, db_avg5m, db_itemdb
# from ledger import Ledger
# from model_item import Item
# from path import f_outliers_csv, f_item_list_csv, f_runelite_loot_tracker_parsed, load_data, f_db_scraped_src
# from runelite_reader import generate_bank_tag, merge_runelite_jsons


def get_reverse_remaps():
	
	temp = {}
	for el in list(remap_dict.keys()):
		temp[remap_dict.get(el)[0]] = el
	return temp


def get_merched_items(thresholds: dict = None, include_reverse_remap: bool = False):
	"""
	Get a subset from items that have been traded, filtered based on specified thresholds. Filters are expected to be
	based on numerical attributes
	:param thresholds: Filter criteria to apply.
	:param include_reverse_remap: If an item is added, also include its reverse remapped counterpart
	:return:
	"""
	output = []
	
	reverse_remaps = get_reverse_remaps()
	# if include_reverse_remap:
	# 	for remapp
	# 	res.remap_dict
	
	# bond_log, bond_id, new_bonds = generate_bond_log(), False, 13190
	
	for e in Ledger().transactions.to_dict('records'):
		print(e.get('item_name'))
		if isinstance(thresholds, dict):
			if not evaluate_numerical_filter(values=e, filters=thresholds):
				continue
		output.append(e.get('item_id'))
		remapped = reverse_remaps.get(e.get('item_id'))
		if remapped is not None:
			output.append(remapped)
	return output


def find_outliers():
	result = []
	item_list_full = db_itemdb.read_db()
	item_list = [int(i) for i in item_list_full.loc[item_list_full['buy_limit'] > 100]['item_id'].to_list()]
	newest_ts = int(max(db_avg5m.read_db('WHERE item_id=2 AND timestamp > :min_ts', {'min_ts': time.time()-86400*7})['timestamp'].to_list()))
	print(f'Newest ts is {newest_ts} {ts_util.ts_to_dt(newest_ts)}')
	time_frame_days = 2
	ts_threshold = newest_ts - 86400*time_frame_days
	wiki_price_threshold = newest_ts - 86400*7
	where_clause = 'WHERE item_id = :item_id AND timestamp > :min_ts'
	max_rows = 86400*time_frame_days//300
	# print(db_wiki.read_db('WHERE item_id=:id AND timestamp > '+str(ts_threshold), values_dict={'id': 2}))
	# exit(-2)
	
	for item_id in item_list:
		w = db_wiki.read_db(f'WHERE item_id=:id AND timestamp > :min_ts', values_dict={'id': int(item_id), 'min_ts': wiki_price_threshold})
		wiki_price = int(np.average(w['price'].to_numpy()))
		wiki_volume = np.sum(w['volume'].to_numpy())
		subset = db_avg5m.read_db(where_clause=where_clause, values_dict={'item_id': int(item_id), 'min_ts': wiki_price_threshold})
		
		if 50 > len(subset) > 0:
			buy_prices, sell_prices = subset['buy_price'].to_list(), subset['sell_price'].to_list()
			prices = buy_prices + sell_prices
			volumes = subset['buy_volume'].to_list() + subset['sell_volume'].to_list()
			values = [prices[idx] * volumes[idx] for idx in range(len(volumes))]
			values.sort()
			top_values_avg = np.average(values[-min(5, len(values)):])
			if top_values_avg < 100000:
				continue
			avg5m_prices = buy_prices + sell_prices
			avg5m_prices.sort()
			n = min(5, len(avg5m_prices))
			high_price = np.average(avg5m_prices[-n:])
			volume = np.sum(subset['buy_volume'].to_list() + subset['sell_volume'].to_list())
			if wiki_volume > 10 and high_price > wiki_price*20:
				# print(item_id, id_name[item_id], wiki_price, high_price)
				msg = f'item_id: {item_id} name: {id_name[item_id]}\n' \
				      f'\twiki_price_avg7d: {wiki_price:.0f}\n' \
				      f'\twiki_volume_sum7d: {wiki_volume:.0f}\n' \
				      f'\tavg_top_{n} runelite_prices: {high_price:.0f}\n' \
				      f'\ttop_runelite_values_avg: {top_values_avg:.0f}\n'
				print(msg)
				result.append({
					'item_id': item_id,
					'item_name': id_name[item_id],
					'wiki_price_avg7d': int(wiki_price),
					'wiki_volume_sum7d': int(wiki_volume),
					'top_n_runelite_prices': int(high_price),
					'top_runelite_values_avg': top_values_avg,
					'newest_runelite_submission': ts_util.ts_to_dt(subset['timestamp'].max()),
					'price_index_url': f'https://prices.runescape.wiki/osrs/item/{item_id}',
					'OSRS_ge_url': f'https://secure.runescape.com/m=itemdb_oldschool/_+%28p%2B%2B%29/viewitem?obj={item_id}#90'
				})
	pd.DataFrame(result).sort_values(by=['top_runelite_values_avg'], ascending=[False]).to_csv(f_outliers_csv, index=False)
	exit(404)
		

def generate_items_list_csv(update_frequency: int = 10):
	item_list_full = db_itemdb.read_db()
	item_list = item_list_full.loc[item_list_full['buy_limit'] > 100]['item_id'].to_list()
	# con_avg5m, cursor_avg5m = db_avg5m.sql.connect()
	# newest_ts = cursor_avg5m.execute('SELECT timestamp FROM avg5m WHERE item_id=2 ORDER BY timestamp DESC').fetchall()[0][0]
	newest_ts = max(db_avg5m.read_db('WHERE item_id=2 AND timestamp > :min_ts', {'min_ts': time.time()-86400*7})['timestamp'].to_list())
	print(f'Newest ts is {newest_ts} {ts_util.ts_to_dt(newest_ts)}')
	time_frame_days = 2
	csv_value_sum_col = f'value_m_sum_{time_frame_days}d'
	# db_avg5m.sql.connect()
	ts_threshold = newest_ts - 86400*time_frame_days
	where_clause = 'WHERE item_id = :item_id AND timestamp > ' + str(ts_threshold)
	filtered_list = []
	n_items, cur = len(item_list), 0
	# df_full = db_avg5m.read_db(where_clause='WHERE timestamp > '+str(ts_threshold))
	rows_max = 86400 * time_frame_days // 300
	
	for item_id in item_list:
		cur += 1
		if cur % update_frequency == 0:
			print(f'Current id: {item_id} ({cur}/{n_items})')
		subset = db_avg5m.read_db(where_clause=where_clause, values_dict={'item_id': int(item_id)})
		# subset = df_full.loc[df_full['item_id'] == item_id]
		def compute_value(row: pd.Series):
			return (row['buy_price'] * row['buy_volume'] + row['sell_price'] * row['sell_volume']) // 1000
		subset['value'] = subset.apply(lambda r: compute_value(r), axis=1)
		if len(subset) == 0:
			continue
		value_sum = np.sum(subset['value'].to_numpy())
		csv_row = {'item_id': item_id,
		           'item_name': id_name[item_id],
		           csv_value_sum_col: value_sum//1000}
		if csv_row.get(csv_value_sum_col) >= 100:
			buy_limit = item_list_full.loc[item_list_full['item_id'] == item_id]['buy_limit']
			
			def compute_value(row: pd.Series):
				return row['buy_volume'] + row['sell_volume']
			
			subset['volume'] = subset.apply(lambda r: compute_value(r), axis=1)
			csv_row.update({'zeros_buy': len(subset.loc[subset['buy_volume'] == 0]),
			                'non_zeros_buy': len(subset.loc[subset['buy_volume'] != 0]),
			                'zeros_sell': len(subset.loc[subset['sell_volume'] == 0]),
			                'non_zeros_sell': len(subset.loc[subset['sell_volume'] != 0])})
			n_zeros = 2*rows_max - (csv_row.get('non_zeros_buy') + csv_row.get('non_zeros_sell'))
			csv_row['zeros_all'] = n_zeros
			csv_row['coverage (%)'] = f"{(1-n_zeros / (2*rows_max))*100:.2f}"
			csv_row['volume (buy_limits)'] = int(np.sum(subset['volume'].to_numpy()) // buy_limit)
			filtered_list.append(csv_row)
	pd.DataFrame(filtered_list).to_csv(f_item_list_csv, index=False)


def parse_loot_tracker_export():
	loot_tracker_data = load_data(f_runelite_loot_tracker_parsed)
	if not os.path.exists(loot_tracker_data):
		merge_runelite_jsons(regex=r'^loot-tracker.*\.json', output_file=loot_tracker_data, sort_cols=['name'],
		                     drop_dup_subset=['price', 'name', 'count'])
	return pd.read_pickle(loot_tracker_data)
	

def extract_dropped_items(loot_tracker_data: pd.DataFrame, output_csv_file: str = None, add_item_metadata: bool = True):
	dropped_items = []
	for el in loot_tracker_data['drops'].to_list():
		dropped_items += el
	dropped_items = pd.DataFrame(dropped_items, columns=['name', 'id', 'qty', 'price']).sort_values(by=['name'])
	dropped_items = dropped_items.drop_duplicates(subset=['name'], ignore_index=True)
	del dropped_items['qty'], dropped_items['price']
	
	if add_item_metadata:
		itemdb = db_itemdb.read_db()
		dropped_items = [itemdb.loc[itemdb['id'] == item_id].to_dict('records') for item_id in dropped_items['id'].to_list()]
		
		temp = []
		for el in dropped_items:
			try:
				temp.append(el[0])
			except IndexError:
				pass
		dropped_items = pd.DataFrame(temp)
		del dropped_items['item_id'], dropped_items['update_ts']
		
		def is_rune(item: pd.Series):
			split_name = item['item_name'].split(' ')
			return len(split_name) == 2 and split_name[1] == 'rune' and item['stackable'] and not item['equipable']
		
		# potion_weights = [0.0199999995529651, 0.0299999993294477]
		
		def is_potion(item: pd.Series):
			# return 0.03 > item['weight'] >= 0.02 and item['item_name'][-3:] in ['(1)', '(2)', '(3)', '(4)'] and not item['equipable'] and not item['stackable']
			return 0.0299999993294477 >= item['weight'] >= 0.0199999995529651 and item['item_name'][-3:] in ['(1)', '(2)', '(3)', '(4)']
		
		dropped_items['is_rune'] = dropped_items.apply(lambda i: is_rune(item=i), axis=1)
		dropped_items['is_potion'] = dropped_items.apply(lambda i: is_potion(item=i), axis=1)
		
	if output_csv_file is not None:
		dropped_items.to_csv(output_csv_file, index=False)
	return dropped_items


def filter_dropped_items(csv_file: str = None, extract_loot_tracker_jsons: bool = True):
	"""
	Hard-coded filter for filtering out items from the loot tracker that should not be included in the bank tag
	:param csv_file: path to csv file generated from the loot tracker jsons
	:param extract_loot_tracker_jsons: True to get input data from (newly) merged loot tracker data
	:return: Filtered dataframe
	"""
	if extract_loot_tracker_jsons or csv_file is None:
		df = extract_dropped_items(parse_loot_tracker_export(), output_csv_file=None)
	elif csv_file is not None:
		id_list = pd.read_csv(csv_file).astype(dtype={'name': 'string', 'id': 'int'})['id'].to_list()
		df = pd.DataFrame([el for el in db_itemdb.read_db().to_dict('records') if el.get('id') in id_list])
	else:
		raise RuntimeError(f'Either provide a path to a valid CSV file or set extract_loot_tracker_jsons to True')
	
	def filter_names(item: pd.Series):
		n = item['item_name']
		return n[-4:] in [' bar', ' ore'] or n[-5:] in [' rune', ' logs']
	
	df['filter'] = df.apply(lambda i: filter_names(i), axis=1)
	df = df.loc[~df['filter']]
	del df['filter']
	# return generate_bank_tag(tag_name='drops', tag_icon=995, keep_previous=False, item_list=df['item_id'].to_list())
	return df
	

if __name__ == "__main__":
	import util.unix_time as ut
	print(ut.loc_dt_unix(datetime.datetime(2024,10,20, 16)))
	print(ut.loc_dt_unix(datetime.datetime(2024,10,21, 2)))
	
	exit(2)
	
	for next_id in (2, 6, 8):
		i = Item.create_table(next_id)
		
		for k, v in i.__dict__.items():
			if v is not None:
				print(k, v)
		print('\n')
	
	exit(404)
	idb_sql = "SELECT * FROM itemdb"
	insert_replace = "INSERT OR REPLACE INTO itemdb (id, item_id, item_name, members, alch_value, buy_limit, " \
	                 "release_date, stackable, equipable, weight, update_ts, augment_data, remap_to, remap_price, " \
	                 "remap_quantity, target_buy, target_sell, item_group) VALUES (:id, :item_id, :item_name, " \
	                 ":members, :alch_value, :buy_limit, :release_date, :stackable, :equipable, :weight, :update_ts, " \
	                 ":augment_data, :remap_to, :remap_price, :remap_quantity, :target_buy, :target_sell, :item_group)"
	
	con = sqlite3.connect(f_db_scraped_src)
	entries = pd.read_sql(sql=idb_sql, con=con).to_dict('records')
	c = con.cursor()
	for e in entries:
		e = augment_itemdb_entry(item=e, overwrite_data=True)
		c.execute(insert_replace, e)
	# con.commit()
	# con.close()
	
	exit(1)
	find_outliers()
	# subset = db_avg5m.read_db(where_clause=where_clause, values_dict={'item_id': int(item_id)})
	
	
	# get_reverse_remaps()
	generate_items_list_csv()
	
	# print('>' in '=>')
	# exit(-1)
	threshold_list = {
		'buy_limit': '> 30',
		'profit': ('> 5000000', '< -5000000'),
		'n_sales': '> 10',
		'n_purchases': '> 10'
	}
	item_ids = get_merched_items(thresholds=threshold_list)
	for next_id in item_ids:
		print(next_id, id_name[next_id])
	
	generate_bank_tag(tag_name='merc', tag_icon=2, item_list=item_ids)
	