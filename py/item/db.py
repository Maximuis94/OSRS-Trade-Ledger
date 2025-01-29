"""
Module with the ItemDB implementation.
All database interactions related to items are handled in this module.

"""
import sqlite3
from collections import namedtuple
from typing import overload

import numpy as np

from backend.download import realtime_prices
from file.file import File
from global_variables.importer import *
from item.constants import ITEM_TABLE
from item.controller import Item
from model.data_source import SRC
from model.database import Database


class ItemDB(Database):
    table_name: str = ITEM_TABLE
    wiki = Database(path=gp.f_db_timeseries, read_only=True)
    
    def __init__(self, augment_items: bool = True, read_only: bool = False, **kwargs):
        path = kwargs.pop('path', gp.f_db_local)
        self.table_name = kwargs.pop('table_name', 'item')
        super().__init__(File(path), read_only=read_only, tables=self.table_name,
                         row_tuple=namedtuple("ItemTuple", Item.sqlite_attributes))
        self.table = ITEM_TABLE
        self.tables = {self.table_name: self.table}
        self.row_factory = self.augmented_item_factory if augment_items else self.item_factory
        self.add_cursor(key=Item, rf=self.item_factory)
        # self.add_cursor(key=self.tuple)
        self.augment_items = augment_items
        self.select_by_id = self.table.select + "WHERE item_id=:item_id"
        self.select_by_name = self.table.select + "WHERE item_name=:item_name"
    
    @overload
    def get_item(self, item_id: int, augment_items: bool = None) -> Item:
        """Fetch an Item with item_id=`item_id` from the sqlite database"""
        
        self.set_item_factory(augment_items)
        try:
            return self.execute(self.select_by_id, {'item_id': item_id}).fetchone()
        except OSError as e:
            print(item_id, self.select_by_id)
            raise e
    
    def get_item(self, item_name: str, augment_items: bool = None) -> Item:
        """Fetch an Item with item_id=`item_id` from the sqlite database"""
        self.set_item_factory(augment_items)
        try:
            return self.execute(self.select_by_name, {'item_name': item_name}).fetchone()
        except OSError as e:
            print(item_name, self.select_by_id)
            raise e
    
    def all_items(self, augment_items: bool = None) -> List[Item]:
        """ Load all item rows from the database and return them """
        # print(self.table.select)
        self.set_item_factory(augment_items)
        return self.execute(self.table.select).fetchall()
    
    def insert_item(self, item: Item):
        """ Insert `item` as a new entry into the database. NB this will only create a new row! """
        # raise NotImplementedError("Added @ 26-06")
        self.insert_rows(table_name=self.table_name, rows=[item.sql_row()], replace=False)
    
    def set_item_factory(self, augment_items: bool = None):
        """ Set the row factory to default item factory or augmented item factory, depending on `augment_items` """
        if isinstance(augment_items, bool):
            self.augment_items = augment_items
        self.row_factory = self.augmented_item_factory if self.augment_items else self.item_factory
    
    def get_realtime_price(self, item_id: int, buy_price: bool = None) -> Tuple[int, int] or int:
        """ Get the realtime price(s) for item. If `buy_price` is None, return both prices, else return buy or sell """
        
        if buy_price is None:
            return self.execute("SELECT rt_buy, rt_sell FROM item WHERE item_id=?", (item_id,)).fetchone()
        else:
            return self.execute("SELECT ? FROM item WHERE item_id=?",
                                (f"rt_{'buy' if buy_price else 'sell'}", item_id)).fetchone()
    
    @staticmethod
    def add_data(i: Item) -> Item:
        """ Load scraped price/volume data for this item from the database """
        p = {'item_id': i.item_id}
        rt_prices = gl.rt_prices_snapshot.get_price(i.item_id)
        if rt_prices is not None:
            i.current_buy, i.current_sell = rt_prices
            i.current_avg = int(np.average(rt_prices))
        # wiki.set_datapoint_factory(factory=sqlite.row_factories.factory_single_value)
        i.current_tax = min(5000000, int(.01 * max(i.current_buy, i.current_sell)))
        i.margin = i.current_sell - i.current_buy - i.current_tax
        
        try:
            i.current_ge = ItemController.wiki.execute(f"SELECT price, MAX(timestamp) FROM item{i.item_id:0>5} WHERE "
                                                       f"src={SRC.w}", p, factory=0).fetchone()
        except IndexError:
            i.current_ge = 0
        # print(i.current_ge)
        try:
            volumes = ItemController.wiki.execute(f"SELECT volume FROM item{i.item_id:0>5} WHERE src={SRC.w}", p,
                                                  factory=0).fetchall()
            if len(volumes) == 0:
                i.avg_volume_day = 0
            else:
                i.avg_volume_day = int(np.average(volumes[-1 * min(len(volumes), gc.n_days_volume_avg):]))
        except ValueError:
            i.avg_volume_day = 0
        # # print(f'set avg_volume_day to', i.avg_volume_day)
        return i
    
    @staticmethod
    def item_factory(c: sqlite3.Cursor, row) -> Item:
        """ Convert a parsed sqlite row to an Item """
        # print(row)
        return ItemController.add_data(
            Item(*row)
            # Item(item_id=row[0], from_dict={col[0]: Item.py_dtypes.get(col[0])(row[idx]) for idx, col in enumerate(c.description)})
        )
    
    @staticmethod
    def item_factory_data_transfer(c, row):
        return Item(*row)
    
    @staticmethod
    def augmented_item_factory(c: sqlite3.Cursor, row) -> Item:
        """ Convert a parsed sqlite row to an Item. Augment item data before returning it """
        return augment_itemdb_entry(
            ItemController.add_data(
                Item(*row)
                # Item(item_id=row[0], from_dict={col[0]: Item.py_dtypes.get(col[0])(row[idx]) for idx, col in enumerate(c.description)})
            ), overwrite_data=False
        )
    
    @staticmethod
    def extract_metadata(item: Item, remove_defaults: bool = False) -> dict:
        """ Reduce Item `item` to dict with its meta-data attributes, and optionally remove default values """
        if remove_defaults:
            ...
        else:
            return {c: v for c, v in item.__dict__.items() if c in Item.metadata and (not remove_defaults or item.dt)}
    
    def update_realtime_prices(self) -> bool:
        """ Fetch the realtime prices dict and update the realtime prices database entries for each item """
        rt_prices = realtime_prices()
        sql = f"UPDATE item SET rt_buy=?, rt_sell=?, rt_ts={int(time.time())} WHERE item_id=?"
        self.executemany(sql, [(min(rt_prices.get(i)), max(rt_prices.get(i)), i) for i in list(rt_prices.keys())])
        return True
    
    def __getitem__(self, item: int | str) -> Item:
        """Retrieve an Item from the database"""
        return self.get_item(item)
