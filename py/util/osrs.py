"""
This module contains various osrs-related util methods


"""
import sqlite3
import time

import pandas as pd
from venv_auto_loader.active_venv import *
import backend.download as dl
import global_variables.path as gp
import util.file as uf
import sqlite.executable
__t0__ = time.perf_counter()

debug = False


def check_for_new_items() -> bool:
    """ Download the wiki mapping and check if it contains new items. Submit new items using a default row to db. """
    
    # Template dict for itemdb rows
    def get_row(e) -> dict:
        """ Convert the wiki_mapping entry to a sqlite row with default values for missing data. """
        item = Item._types()
        item.update({
            'id': e.get('id'),
            'item_id': e.get('id'),
            'item_name': e.get('name'),
            'members': e.get('members'),
            'alch_value': e.get('highalch')})
        return item
    
    wiki_mapping = {el.get('id'): el for el in dl.wiki_mapping()}
    con = sqlite3.connect(gp.f_db_item)
    con.row_factory = lambda row, c: row[0]
    commit_data = False
    for item_id in frozenset(wiki_mapping.keys()).difference(con.execute('SELECT item_id FROM item').fetchall()):
        row = get_row(wiki_mapping[item_id])
        con.execute(sqlite.sql_exe.sql_insert(row=row, table='item', replace=False), row)
        print(f'Inserted item data for id={item_id}', row)
        commit_data = True
        con.commit()
    # con.close()
    return commit_data


def get_daily_market_values(reference_timestamp: int = int(time.time()-86400*14)):
    """ Generate a dataframe with all items from the Wiki timeseries table and add the averaged daily market value. """
    con = sqlite3.connect(db_scraped)
    df = pd.read_sql(
        sql=r'SELECT item_id, price, volume FROM wiki WHERE timestamp > :min_ts',
        con=con,
        
        # Reference timestamp dictates the rows the average market value is computed from
        params={'min_ts': reference_timestamp}
    )
    
    def compute_daily_value(row: pd.Series):
        """ Compute the market value by multiplying the price with volume """
        return row['price'] * row['volume']
    
    df['daily_value'] = df.apply(lambda r: compute_daily_value(r), axis=1)
    return df


# def buy_price(item_id: int) -> int:
#     """ Return the realtime buy price of `item_id` """
#     return min(gl.rt_prices_snapshot.get_price(item_id=item_id))
#
#
# def sell_price(item_id: int) -> int:
#     """ Return the realtime sell price of `item_id` """
#     return max(gl.rt_prices_snapshot.get_price(item_id=item_id))
#
#
# def prices(item_id: int) -> tuple:
#     """ Return the prices tuple from the realtime snapshot """
#     return gl.rt_prices_snapshot.get_price(item_id)


def ge_tax(price: int) -> int:
    """ Return the ge tax that will be applied when selling an item for `price` gp """
    return min(5000000, int(.01*price))


def assign_augmented_item_tag(item: dict):
    """ Generate a flag that indicates whether this item should be augmented or not """
    cur = item.get('augment_data')
    
    # Manual tag override for item augments; leave value as-is
    if not 0 >= cur >= 1:
        return cur
    
    # TODO complete implementation of this method
    #  Has this item been frequently traded?
    #  Average daily market value of this item?
    #  Estimated daily profit for this item?
    
    return cur


def get_tax(price: int) -> int:
    """ Return the GE tax rate that would be applied to `price` """
    return min(5000000, int(price * .01))


def get_nature_rune_price() -> int:
    """ Return the current price of nature runes """
    try:
        rt = uf.load(uf.get_newest_file([gp.local_file_rt_prices, gp.f_rbpi_rt]))
        p = rt.get('561').get('high') if rt.get(561) is None else max(rt.get(561))
        if isinstance(p, int):
            return p
    except AttributeError:
        if debug:
            print('AttributeError in get_nature_rune_price')
    except FileNotFoundError:
        if debug:
            print('FileNotFoundError in get_nature_rune_price')
    except TypeError:
        print(uf.load(uf.get_newest_file([gp.local_file_rt_prices, gp.f_rbpi_rt])))
        print([gp.local_file_rt_prices, gp.f_rbpi_rt])
    try:
        print(f'Attempting to get nature rune price from avg5m database...')
        con = sqlite3.connect(gp.f_db_timeseries)
        max_wiki_ts = con.execute("SELECT MAX(timestamp) FROM wiki WHERE item_id=561").fetchone()[0]
        return con.execute("SELECT price, MAX(timestamp) FROM avg5m "
                           "WHERE timestamp > :ts AND item_id=561", {'ts': max_wiki_ts}).fetchone()[0]
    finally:
        print(f' *** Failed to determine price of nature runes, returning 0... ***')
        return 0
        
    #     if time.time() - gc.rt_update_frequency:
    #         nature_rune_price = max(gp.load_data(gp.local_file_rt_prices).get(561))
    # finally:
    #     if nature_rune_price == 0:
    #         con = sqlite3.connect(gp.f_test_db)
    #         max_wiki_ts = con.execute("SELECT MAX(timestamp) FROM wiki WHERE item_id=561").fetchone()[0]
    #         nature_rune_price = con.execute("SELECT price, MAX(timestamp) FROM realtime "
    #                                         "WHERE timestamp > :ts AND item_id=561", {'ts': max_wiki_ts}).fetchone()[0]
    # return nature_rune_price

if __name__ == "__main__":
    print(dl.wiki_mapping())
    time.sleep(10)