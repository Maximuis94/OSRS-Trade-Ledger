""" #TODO Insert data download methods
Module with various methods for downloading data from a specific source.
Used for downloading data, but also serves as reference for how the rbpi downloads data.


Used as import backend.download as dl
"""
import datetime
import json
import os
import sqlite3
import time
import urllib.error
import urllib.request
from typing import Tuple, Optional, Dict, Literal

import requests
from bs4 import BeautifulSoup

from venv_auto_loader.active_venv import *
import global_variables.configurations as cfg
# import global_variables.osrs as go
import global_variables.path as gp
import global_variables.values as val
import util.str_formats as fmt
import util.unix_time as ut
__t0__ = time.perf_counter()

_request_header = {
    'User-Agent': 'Homemade GE trading GUI/DB | Disc: Maximuis94'
}


# This method should be accessed through global_variables.local_file.RealtimePricesSnapshot
def realtime_prices(check_rbpi: bool = False, force_rbpi: bool = False) -> dict:
    """
    Download and return a realtime prices snapshot. Alternatively, download it from the raspberry pi if an updated
    version is available there.
    
    Parameters
    ----------
    check_rbpi : bool, optional, False by default
        If True, first check if there is an updated version on the raspberry pi
    force_rbpi : bool, optional, False by default
        If True, only attempt to get data from the raspberry pi

    Returns
    -------
    dict
        Realtime prices dict, with item_id as key and low, high prices as entries
    
    Raises
    ------
    
    """
    data, price_keys = None, ('low', 'high')
    
    #
    try:
        if (check_rbpi or force_rbpi) and time.time() - gp.f_rbpi_rt.mtime() < cfg.rt_update_frequency:
            # print(f'loading data from rbpi')
            data = gp.f_rbpi_rt.load()
            # print(data)
    finally:
        if force_rbpi and data is None:
            raise RuntimeError(f'Failed to fetch data from the Raspberry Pi, while force_rbpi=True')
    
    if data is None:
        url = "https://prices.runescape.wiki/api/v1/osrs/latest"
        req = urllib.request.Request(url, headers=_request_header)
        try:
            with urllib.request.urlopen(req) as handle:
                data = json.loads(handle.read().decode()).get('data')
                print(f"[{fmt.dt_(fmt_str='%H:%M:%S')}] Downloading realtime prices snapshot")
        except urllib.error.HTTPError as e:
            print(f"ERROR in download_wiki_prices_latest, url={url}\n{e}")
            return {}
        data = {int(item_id): tuple((data.get(item_id).get(p) for p in price_keys)) for item_id in list(data.keys())}
    return data


def wiki_item_ids() -> Tuple[Optional[str], ...]:
    """
    Fetches item ID to item name mapping from the Old School RuneScape Wiki.

    This function retrieves a JSON file containing item ID to name mappings directly
    from the Old School RuneScape Wiki. It filters out any entries where the name
    starts with the '%' character and generates a sequenced tuple of item names,
    indexed by the item IDs. If an item ID is missing in the data, its corresponding
    index in the tuple will be `None`.

    Returns
    -------
    tuple of Optional[str]
        A tuple where the index corresponds to the item ID, and the value is either
        the name of the item or `None` if the item ID is not available/mapped.

    Raises
    ------
    urllib.error.HTTPError
        If an error occurs while attempting to download the JSON data from the
        specified URL.
    
    Examples
    --------
    >>> ids = wiki_item_ids()
    >>> ids[2] == "Cannonball"

    """
    url = "https://oldschool.runescape.wiki/?title=Module:GEIDs/data.json&action=raw&ctype=application%2Fjson"
    req = urllib.request.Request(url, headers={'User-Agent': 'High-res price data scraper'})

    try:
        with urllib.request.urlopen(req) as handle:
            _json = json.loads(handle.read().decode())
            id_name = {_id: _name for _name, _id in _json.items() if not _name.startswith('%')}

            return tuple([id_name.get(_id, None) for _id in range(max(list(id_name.keys()))+1)])
    except urllib.error.HTTPError as e:
        print(f"ERROR downloading the item_id : item_name mapping in download.wiki_item_ids()\n\tURL: {url}")
        raise e


# This method should be accessed through global_variables.local_file.ItemWikiMapping
def wiki_mapping() -> Dict[int, Dict[str, int | str]]:
    """
    Fetches and parses the item mapping data from the RuneScape Wiki API.

    This function sends a GET request to the provided RuneScape Wiki API endpoint
    to retrieve the item mapping data. The data is decoded and returned as a
    dictionary, where the key is the item's numeric ID and the value is the
    corresponding item mapping data.

    Returns
    -------
    Dict[int,
        Dict[Literal['examine'] | Literal['id'] | Literal['members'] | Literal['lowalch'] | Literal['limit'] |
        Literal['value'] | Literal['highalch'] | Literal['icon'] | Literal['name'], int | str]
        The result from the call to the Wiki Mapping API url

    Raises
    ------
    urllib.error.HTTPError
        If there is an issue with the HTTP request, such as an invalid URL or
        server error, the error will be caught and logged, and an empty dictionary
        will be returned.
        
    Examples
    --------
    >>> ids = wiki_mapping()
    >>> ids.get(0) is None
    >>> ids[2].get("name") == "Cannonball"
    """
    url = "https://prices.runescape.wiki/api/v1/osrs/mapping"
    req = urllib.request.Request(url, headers={'User-Agent': 'High-res price data scraper'})
    try:
        with urllib.request.urlopen(req) as handle:
            return {int(el.get('id')): el for el in json.loads(handle.read().decode())}
    except urllib.error.HTTPError as e:
        print(f"ERROR in download_wiki_mapping, url={url}")
        print(e)
        return {}


def wiki_graph(item_id: int or str, min_ts: int = 1427500800):
    """ Download historical wiki price data for the `item_id` specified, starting at `min_ts` """
    if isinstance(item_id, int):
        item_id = str(item_id)
    api_url = f"https://api.weirdgloop.org/exchange/history/osrs/all?id={item_id}"
    j = None
    req = urllib.request.Request(api_url, headers=_request_header)
    with urllib.request.urlopen(req) as url:
        j = json.loads(url.read().decode())
    n_days = int((time.time() - min_ts + 240000) // 86400)
    j, graph = j.get(item_id), []
    j = j[-n_days:]
    first = True
    print(f"n_days = {n_days}")
    for d in j:
        ts = d.get('timestamp') // 1000
        if ts < min_ts:
            continue
        if d.get('volume') == 'null':
            graph.append((ts, d.get('price')))
        else:
            graph.append((ts, d.get('price'), d.get('volume')))
    return graph


def scrape_wiki(url: str, min_ts: int or float = 0):
    """
    Scrape the html code of the URL. Note that download_wiki_graph() is much faster / more efficient...
    :param url: A URL of a wiki item exchange page
    :param min_ts: The newest logged TS in the wiki table for the item of the url
    :return: The scraped entries, with the oldest entry TS being equal to min_ts
    """
    try:
        page = requests.get(url)
        bs = BeautifulSoup(page.content, 'html.parser')
        results = bs.find(id='mw-content-text')
        dat = results.find('div', class_='GEdataprices')['data-data']
        dat = dat.split('|')
        output = []
        
        if min_ts > 0:
            dat.reverse()
            for p in dat:
                str_list = p.split(':')
                el = [int(s) for s in str_list]
                if el[0] < min_ts:
                    output.reverse()
                    return output
                if len(el) == 2:
                    el += [-1]
                output.append(el)
            output.reverse()
            return output
        else:
            for p in dat:
                str_list = p.split(':')
                el = [int(s) for s in str_list]
                if len(el) == 2:
                    el += [-1]
                output.append(el)
            return output
    except AttributeError:
        print(f"AttributeError for url {url}, URL might be corrupted!")
        return None
    except TypeError:
        if min_ts is None:
            return scrape_wiki(url=url, min_ts=1)
        print("********** TYPE ERROR IN WIKI PRICE SCRAPER **********")
        print('URL: ', url)
        print('Newest TS: ', min_ts)
        return None


def osrs_graph_official(item_id: int):
    """
    Scrape item info from the official osrs price itemdb page.

    Parameters
    ----------
    item_id :

    Returns
    -------
    graph_data : dict
        dict with time tuple (year, month, day) as key and (guide price, price trend, trade volume) as value


    """
    db = sqlite3.connect(f"file:{gp.f_db_local}?mode=ro", uri=True)
    item_name = db.execute("SELECT item_name FROM item WHERE item_id=?", (item_id,)).fetchone()
    db.close()
    item_name_url = item_name.replace(' ', '+').replace("'", '%27').replace("(", '%28').replace(")", '%29')
    
    url = f"https://secure.runescape.com/m=itemdb_oldschool/{item_name_url}/viewitem?obj={item_id}"
    # exit(123)
    page = requests.get(url)
    bs = BeautifulSoup(page.content, 'html.parser')
    # chart_section = bs.find('div', class_='chart')
    # trade_data = bs.find_all('script')
    # prices, trends, volumes = {}, {}, {}
    # pc = bs.find('div', class_='stats')
    # price_changes, pc_changes = {}, {}
    
    # def strip_str(s: str, to_remove: str = '+%km ,'):
    #     """ Remove all characters in `to_remove` from string `s` """
    #     for c in to_remove:
    #         s = s.replace(c, '')
    #     return s
    
    # for el in pc.find_all('li'):
    #     # Amount of months relative to change; can be 0, 1, 3, 6
    #     try:
    #         m = int(el.text[0])
    #     except ValueError:
    #         m = 0
    #     price_changes[m] = int(strip_str(el.find('span', class_='stats__gp-change')['title']))
    #     pc_changes[m] = int(strip_str(el.find('span', class_='stats__pc-change').text))
    for el in bs.find_all('script'):
        txt = el.get_text()
        if len(txt) > 5000:
            graph_data, cur, pe = {}, {}, None
            for s in txt.split('\n'):
                try:
                    s = s.strip().split('.')[1]
                    # l = l[16:-3]
                    # x, y = l[:10], l[14:]
                    # l = l[16:-3]
                    # x, y = s[16:26], s[30:-3]
                    y = s[30:-3]
                    # year, month, day = x.split('/')
                except ValueError:
                    continue
                except IndexError:
                    continue
                # x = datetime.datetime(int(year), int(month), int(day))
                if y.isdigit():
                    cur['volume'] = int(y)
                    year, month, day = s[16:26].split('/')
                    dt = datetime.datetime(int(year), int(month), int(day))
                    # pe = graph_data.get(dt-datetime.timedelta(days=1))
                    if pe is not None:
                        cur['price_change'] = cur.get('price') - pe.get('price')
                        cur['trend_change'] = cur.get('trend') - pe.get('trend')
                    graph_data[dt], pe = cur, cur
                    cur = {}
                else:
                    try:
                        p, t = y.split(', ')
                    except ValueError:
                        continue
                    # prices[x], trends[x] = int(p), int(t)
                    cur['price'], cur['trend'] = int(p), int(t)
            # graph_data = []
            # for dt in list(prices.keys()):
            #     graph_data.append({
            #         'price': prices.get(dt),
            #         'trend': trends.get(dt),
            #         'volume': volumes.get(dt)
            #     })
            return graph_data


def process_row(txt: str):
    pass


def graph_realtime_timeseries(item_id: int):
    """ Download realtime price data averaged per 5 minutes and per hour for the `item_id` specified """
    # base_url = f"https://prices.runescape.wiki/api/v1/osrs/timeseries?timestep=X&id={item_id}"
    base_url = f"https://prices.runescape.wiki/osrs/X?id={item_id}"
    output = {'5m': None, '1h': None}
    
    for next_timestep in list(output.keys()):
        try:
            req = urllib.request.Request(base_url.replace('X', next_timestep), headers=_request_header)
            with urllib.request.urlopen(req) as handle:
                # output[next_timestep] = json.loads(handle.read().decode()).get('data')
                output[next_timestep] = json.loads(handle.read().decode())
        except urllib.error.HTTPError as e:
            pass
    return output


def download_wiki_prices_rt_averaged(timespan: str = '5m', timestamp: int = None):
    """
    Download realtime pricelist from the Wiki for each item. All data returned is averaged over the specified timespan
    Example entry: "2":{"avgHighPrice":174,"highPriceVolume":1285429,"avgLowPrice":171,"lowPriceVolume":747735}
    :return: A dict with an entry for each item ID as described above
    """
    if timestamp < val.min_avg5m_ts_query_online:
        raise ValueError(f'When querying for avg5m data online, the given timestamp should be at least '
                         f'{val.min_avg5m_ts_query_online} (={fmt.unix_(val.min_avg5m_ts_query_online)})')
    
    minutes = {'5m': 300, '1h': 3600, '3h': 10800}
    minutes_averaged = minutes.get(timespan)
    if minutes_averaged is None:
        print('Invalid timespan passed to download_wiki_prices_rt_averaged! Should be 5m, 1h or 3h...')
        return
    url = f"https://prices.runescape.wiki/api/v1/osrs/{timespan}"
    if timestamp is not None:
        if timestamp % minutes_averaged == 0:
            url += f"?timestamp={timestamp}"
        else:
            # print(f"Unable to download averaged realtime prices with timespan={timespan} for timestamp {timestamp}")
            return None
    req = urllib.request.Request(url, headers={'User-Agent': 'High-res price data scraper'})
    try:
        with urllib.request.urlopen(req) as handle:
            j = json.loads(handle.read().decode())
            # print(j)
            return j.get('data'), j.get('timestamp')
    except urllib.error.HTTPError as e:
        print(f"ERROR in download_wiki_prices_latest, url={url}")
        print(e)
        return None


def graph_wiki_historical(item_id: int, t1: int or float = 0, t2: int or float = time.time()):
    """ Download historical wiki data for the given `item_id` within the specified time frame """
    url = f"https://api.weirdgloop.org/exchange/history/osrs/all?id={item_id}"
    req = urllib.request.Request(url, headers=_request_header)
    try:
        with urllib.request.urlopen(req) as handle:
            temp = json.loads(handle.read().decode()).get(str(item_id))
            graph = []
            for e in temp:
                timestamp = e.get('timestamp') // 1000
                ts_as_dt = ut.loc_unix_dt(timestamp=timestamp)
                if not t1 <= timestamp <= t2:
                    continue
                e.update_transaction({
                    'timestamp': timestamp,
                    'date': ts_as_dt,
                    'day_of_week': ts_as_dt.weekday()
                })
                graph.append(e)
            return graph
    except urllib.error.HTTPError as e:
        return None

