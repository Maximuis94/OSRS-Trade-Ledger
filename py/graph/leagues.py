"""
This module contains various methods for generating leagues graphs

"""

import numpy as np
import sqlite3
import pandas as pd

import global_variables.osrs as go
import global_variables.path as p


def league_analysis(item_list: list = item_ids, market_value_threshold: int = 50000000,
                    output_dir: str = path.dir_plot_archive, benchmark_ts: int = int(time.time()-86400*10)):
    league_timestamps = [
        # Trailblazer 2 15-11-2023 - 10-1-2024
        ('tb2', 'Trailblazer 2', dt_to_ts(datetime.datetime(2023, 11, 15)), dt_to_ts(datetime.datetime(2024, 1, 10))),
        
        # Shattered relics 19-1-2022 - 16-3-2022
        ('sr', 'Shattered relics', dt_to_ts(datetime.datetime(2022, 1, 19)), dt_to_ts(datetime.datetime(2022, 3, 16))),
        
        # Trailblazer 1 28-10-20 - 6-1-21
        ('tb1', 'Trailblazer 1', dt_to_ts(datetime.datetime(2020, 10, 28)), dt_to_ts(datetime.datetime(2021, 1, 6))),
        
        # Twisted league 14-11-2019 - 16-1-2020
        ('tw', 'Twisted', dt_to_ts(datetime.datetime(2019, 11, 14)), dt_to_ts(datetime.datetime(2020, 1, 16)))
    ]
    # plot_leagues(item_id=item_id, league_data=league_timestamps, output_dir='league_plots/')
    start_time = time.time()
    global val
    # First, get a filtered list of ids
    df = pd.read_sql(
        sql=r'SELECT item_id, price, volume FROM wiki WHERE timestamp > :benchmark_ts AND item_id = :item_id',
        con=sqlite3.connect(path.f_db_timeseries),
        params={'benchmark_ts': benchmark_ts, 'item_id': 2}
    )
    def compute_daily_value(row: pd.Series):
        """ Compute the market value by multiplying the price with volume """
        return row['price'] * row['volume']
    df['daily_value'] = df.apply(lambda r: compute_daily_value(r), axis=1)
    
    print(f'Using a daily market value threshold of {format_n(market_value_threshold)}')
    print(f'Output folder set to {output_dir}')
    skipped, sql_columns, processed, errors = [], 'item_id, item_name, buy_limit, release_date', 0, 0
    # for cur_id in list(id_name.keys()):
    print(item_list)
    exit(1)
    for cur_id in item_list:
        print(f'Processed {processed}/{len(item_list)} items with {len(skipped)} skips and {errors} errors', end='\r')
        # con = sqlite3.connect(path.f_db_scraped_src)
        
        try:
            
            if market_value_threshold == 0:
                plot_leagues(item_id=cur_id, league_data=league_timestamps, output_dir=output_dir)
            elif market_value_threshold > 0:
                daily_market_value = int(np.average(df.loc[df['item_id'] == cur_id]['daily_value'].to_numpy()))
                if daily_market_value > market_value_threshold:
                    plot_leagues(item_id=cur_id, league_data=league_timestamps, output_dir=output_dir)
            # if True:
            # 	plot_price(
            # 		item_id=cur_id,
            # 		t0=dt_to_ts(league_timestamps[0][0])-86400*7,
            # 		output_dir='plots_npy/',
            # 		vertical_plots=vertical_plot
            # 	)
            else:
                skipped.append((cur_id, int(np.average(df.loc[df['item_id'] == cur_id]['daily_value'].to_numpy()))))
        except ValueError:
            errors += 1
        except IndexError:
            errors += 1
        finally:
            processed += 1
    # df = pd.DataFrame(item_list)
    # df.to_excel('output/league_analysis_full.xlsx')
    print(len(item_list), skipped, len(item_list)+len(skipped), '\n')
    
    print(f'Errors:', errors)
    
    print('The following items have been skipped due to low daily value:')
    for item_id, daily_value in skipped:
        print(f'{id_name[item_id]} ({format_n(daily_value)})')
    print(f'Execution time: {format_time(int(time.time()-start_time))}')
    input('press ENTER to close this screen')
    exit(1)


def generate_league_graphs(daily_value_threshold: int = 0, include_transaction_ids: bool = True,
                           include_npy_array_ids: bool = True):
    if include_transaction_ids:
        transaction_ids = list(np.unique(pd.read_sql(con=sqlite3.connect(path.f_db_local),
                                                     sql='SELECT item_id FROM transactions').to_numpy()))
    else:
        transaction_ids = []
    
    npy_ids = npyar_items if include_npy_array_ids else []
    
    if len(transaction_ids) + len(npy_ids) > 0:
        # Initial set: transaction items and/or npy archive items
        item_subset = list(set(transaction_ids + npyar_items))
        item_subset.sort()
    else:
        item_subset = []
    
    con = sqlite3.connect(path.f_db_timeseries)
    c = con.cursor()
    
    # Expand list with items with daily value that exceeds 50M
    wiki_sql = f'SELECT price, volume FROM wiki WHERE item_id = :item_id AND timestamp > {int(time.time() - 14 * 86400)}'
    for item_id in item_ids:
        if item_id in skip_ids:
            continue
        try:
            avg_value = int(np.average([r[0] * r[1] for r in c.execute(wiki_sql, {'item_id': item_id}).fetchall()]))
            if avg_value < 50000000:
                continue
            # print(f'{item_id} {id_name[item_id]} {format_n(avg_value)}')
            if item_id not in item_subset:
                item_subset.append(item_id)
        except ValueError:
            continue
    print('Generating graphs for', len(item_subset), 'items')
    con.close()
    league_analysis(item_list=item_subset, market_value_threshold=0, output_dir=path.dir_plot_archive[:-1] + '_11_12/')


def generate_graphs():
    id_list = path.load_as_dp(path.f_np_array_ids)
    processed, n_total, skipped, errors = 0, len(id_list), [], 0
    t = int(time.time())
    con = sqlite3.connect(path.f_db_timeseries)
    c = con.cursor()
    
    # exit(1)
    info_columns = 'item_name, buy_limit, members, release_date'
    avg5m_sql = 'SELECT timestamp, buy_price, sell_price FROM avg5m WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1'
    wiki_sql = 'SELECT timestamp, price, volume FROM wiki WHERE item_id=:item_id AND timestamp BETWEEN :t0 AND :t1'
    values = {
        't0': league_timestamps[-1][2] - 86400 * 14,
        't1': min(int(time.time()), league_timestamps[0][3] + 86400 * 14)
    }
    for i in id_list:
        if processed % 50 == 0:
            print(
                f'[{format_time(time.time() - t)}] Processed {processed}/{len(id_list)} items with {len(skipped)} '
                f'skips and {errors} errors')  # , end='\r')
        print(f'[{format_time(time.time() - t)}] Processed {processed}/{len(id_list)} items with {len(skipped)} '
              f'skips and {errors} errors', end='\r')
        try:
            values['item_id'] = i
            plot_leagues(item_id=i,
                         league_data=league_timestamps,
                         output_dir=output_dir,
                         item_data=get_item_info(item_id=i, columns=info_columns, con=con).to_dict('records')[0],
                         df_avg5m=pd.read_sql(con=con, sql=avg5m_sql, params=values),
                         df_wiki=pd.read_sql(con=con, sql=wiki_sql, params=values))
        # except:
        #     skipped.append(i)
        finally:
            processed += 1
    print('The following items were skipped:')
    for i in skipped:
        print('\t', i, id_name[i])
    print(skipped)
    plot_archives = [path.dir_root + "league_plots_11_12/", path.dir_root + "league_plots_11_12_jpg/"]
    
    for pa, ext in zip(plot_archives, ['png', 'jpeg']):
        graph_files = path.get_files(src=pa, add_src=True, extensions=[ext])
        size_summed = np.sum([os.db_file.getsize(f) for f in graph_files])
        print(f'Summed file size for {len(graph_files)} {ext} images is {format_n(size_summed)}b')
    input('Press ENTER to close')
    time.sleep(20)


def generate_npy_graphs(ar: NpyArray, t1: int, root: str = str(os.getcwd()).replace('\\', '/') + '/'):
    """
    Generate all graphs as defined at the loop start for the item belonging to the given NpyArray object.

    Parameters
    ----------
    ar : NpyArray
        Loaded NpyArray object with all the relevant data
    t1 : int
        Timestamp upperbound for this graph
    root : str, optional, str(os.getcwd()).replace('\\', '/')+'/')
        root folder in which the graphs will be saved (additional folders will be created within this folder)


    Returns
    -------

    """
    # This loop defines which graph types will be created
    for t0, graph_generator, graph_name in zip(
            # List with t0s
            [t1 - t_d * 2, t1 - t_d * 14, t1 - t_d * 42, t1 - t_d * 42, t1 - t_d * 14],
            
            # List with graph generation methods. None means plotting unix timestamp against buy/sell price.
            [price_graph, price_graph, price_graph, price_graph_by_dow, price_graph_by_hod],
            
            # Name of this specific graph.
            ['prices_2d', 'prices_2w', 'prices_6w', 'prices_dow_6w', 'prices_hod_2w']):
        if not os.db_file.exists(root + graph_name + '/'):
            os.mkdir(root + graph_name + '/')
            print(f'Created folder {root + graph_name + "/"}')
        f = f'{root}/{graph_name}/{ar.item_name}.png'
        if os.db_file.exists(f):
            continue
        # fig, axs = plt.subplots(0)
        try:
            pg = PricesGraph(item=ar, t0=t0, t1=t1, y_values=['buy_price', 'sell_price'], axs_gen=graph_generator,
                             output_file=f)
            
            if pg is not None:
                fig, p = plt.subplots(1)
                
                for g in [pg]:
                    # todo: add abstract superior Graph class that can be used for identifying graphs
                    # todo: predefine graph canvas that can be used directly in global_values like module
                    if not isinstance(g, PricesGraph):
                        raise TypeError(f'Expected typing for passed graphs is a graphs.Graph class')
                    
                    # p = plot_prices(axs=p, np_ar=g.item, t0=t0, t1=t1)
                    p = g.generate_graph(p=p, t0=t0, t1=t1, vplot_frequencies=get_vplot_timespans(delta_t=t1 - t0))
                
                if isinstance(f, str):
                    plt.savefig(f)
                    plt.close('all')
        except ValueError as e:
            print(f'ValueError generating graph {graph_name} for item {ar.item_name}')
            continue
        except IndexError as e:
            print(f'ValueError generating graph {graph_name} for item {ar.item_name}')
            continue


if __name__ == '__main__':
    done = False
    item_list = global_values.npyar_items
    if not os.db_file.exists(root_folder):
        os.mkdir(root_folder)
    
    # Timestamp upperbound is cut-off at 4-hrs utc time (e.g. 12am, 4am, ..., 8pm)
    ct = int(time.time())
    t1 = ct - ct % 14400
    n_graphs = len(item_list)
    while not done:
        try:
            
            for idx, item_id in zip(range(len(item_list)), item_list):
                print(f'Current graph: {id_name[item_id]} {idx}/{n_graphs}')
                generate_npy_graphs(ar=NpyArray(item_id=item_id), t1=t1, root=root_folder)
            done = True
        except IndexError:
            done = False