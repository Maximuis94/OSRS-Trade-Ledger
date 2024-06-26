"""
Within this module, the databases used throughout the project are defined.
Databases are composed via a set of Tables, which are in turn composed via a set of Columns

The databases as they are defined here should correspond with the column orderings as implemented in sqlite.row
"""

from global_variables.path import f_db_local, f_db_timeseries
from model.table import Column, Table

avg5m = Table(table_name='avg5m', db_file=f_db_timeseries,
              columns=[
                  Column(name='timestamp', is_primary_key=True),
                  Column(name='item_id', is_primary_key=True),
                  Column(name='buy_price', add_check=True),
                  Column(name='buy_volume', add_check=True),
                  Column(name='sell_price', add_check=True),
                  Column(name='sell_volume', add_check=True)])


realtime = Table(table_name='realtime', db_file=f_db_timeseries,
                 columns=[
                     Column(name='timestamp', is_primary_key=True),
                     Column(name='item_id', is_primary_key=True),
                     Column(name='is_buy', is_primary_key=True, add_check=True),
                     Column(name='price', add_check=True)
                 ])


wiki = Table(table_name='wiki', db_file=f_db_timeseries,
             columns=[
                 Column(name='timestamp', is_primary_key=True),
                 Column(name='item_id', is_primary_key=True),
                 Column(name='price', add_check=True),
                 Column(name='volume', add_check=True)
             ])


item = Table(table_name='item', db_file=f_db_local,
             columns=[
                 Column(name='id', is_primary_key=True),
                 Column(name='item_id', is_unique=True, is_nullable=False),
                 Column(name='item_name', is_unique=True, is_nullable=False),
                 Column(name='members', add_check=True),
                 Column(name='alch_value', add_check=True),
                 Column(name='buy_limit', add_check=True),
                 Column(name='stackable', add_check=True),
                 Column(name='release_date'),
                 Column(name='equipable', add_check=True),
                 Column(name='weight', add_check=True),
                 Column(name='update_ts'),
                 Column(name='augment_data'),
                 Column(name='remap_to'),
                 Column(name='remap_price', add_check=True),
                 Column(name='remap_quantity', add_check=True),
                 Column(name='target_buy', add_check=True),
                 Column(name='target_sell', add_check=True),
                 Column(name='item_group')
             ])


transaction = Table(table_name='transaction', db_file=f_db_local,
                    columns=[
                        Column(name='transaction_id', is_primary_key=True),
                        Column(name='item_id', is_nullable=False),
                        Column(name='timestamp', is_nullable=False),
                        Column(name='is_buy', is_nullable=False, add_check=True),
                        Column(name='quantity', add_check=True),
                        Column(name='price', add_check=True),
                        Column(name='status', default_value=1),
                        Column(name='tag', is_nullable=False),
                        Column(name='update_ts', is_nullable=False),
                        
                        Column(name='average_buy', is_nullable=False, default_value=0),
                        Column(name='balance', is_nullable=False, default_value=0),
                        Column(name='profit', is_nullable=False, default_value=0),
                        Column(name='value', is_nullable=False, default_value=0),
                        Column(name='n_bought', is_nullable=False, default_value=0),
                        Column(name='n_purchases', is_nullable=False, default_value=0),
                        Column(name='n_sold', is_nullable=False, default_value=0),
                        Column(name='n_sales', is_nullable=False, default_value=0),
                        Column(name='tax', is_nullable=False, default_value=0)
                        
                    ])

tables = {k: t for k, t in dict(locals()).items() if isinstance(t, Table)}
