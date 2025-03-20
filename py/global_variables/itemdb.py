"""
Module with an Item database manager.

All functions regarding getting item data should be relayed via this class
The ItemDb is instanced as Singleton; it can be imported by importing the itemdb variable from this module.
Can also be used as "from itemdb import *"
"""
import datetime
import sqlite3
import time
from dataclasses import dataclass
from typing import Tuple, Dict, List
from multipledispatch import dispatch

from backend.download import realtime_prices
from file.file import File
from global_variables import path as gp
from global_variables.classes import SingletonMeta
from common.classes.item import Item

ITEM_DB_FILE: File = gp.f_db_item
ITEM_DB_TABLE: str = "item"


@dataclass(slots=True)
class _ItemDb(metaclass=SingletonMeta):
    """Class for providing Item data. Can be called using ItemDb[item_id] or ItemDb[item_name] to get an Item"""
    _init_time: int or float
    _items: Tuple[Item or None, ...]
    _name_item: Dict[str, Item]
    _nature_rune_price: Tuple[int, int]
    _table: str = "item"
    _most_traded_item_ids = 2, 314, 453, 554, 555, 556, 557, 560, 561, 561, 562, 565, 7936, 12934, 21820, 27616
    db_file: File = gp.f_db_local
    _rt = realtime_prices()
    
    def __init__(self):
        global ITEM_DB_FILE, ITEM_DB_TABLE
        db_con = sqlite3.connect(f"file:{ITEM_DB_FILE}?mode=ro", uri=True)
        c = db_con.cursor()
        c.row_factory = lambda _, row: Item(*row)
        
        items = [None for _ in range(db_con.execute(f"""SELECT MAX(id) FROM '{ITEM_DB_TABLE}'""").fetchone()[0] + 1)]
        cursor = db_con.cursor()
        cursor.row_factory = Item.row_factory
        rtp = realtime_prices()
        # for i in cursor.execute(Item.sql_select()).fetchall():
        print(f"""SELECT ({", ".join(Item.sqlite_columns())}) FROM '{ITEM_DB_TABLE}'""")
        sql = f"""SELECT {", ".join(Item.sqlite_columns())} FROM '{ITEM_DB_TABLE}'"""
        for i in cursor.execute(sql).fetchall():
            try:
                i.current_buy, i.current_sell = self._rt.get(i.item_id)
            except TypeError:
                i.current_buy, i.current_sell = 0, 0
            
            items[i.item_id] = i
        self._init_time = time.time()
        self._items = tuple(items)
        self._name_item = {i.item_name: i for i in self._items if i is not None}
        nature_rune = self[561]
        self._nature_rune_price = nature_rune.current_buy, nature_rune.current_sell
    
    @dispatch(int)
    def __getitem__(self, item: int) -> Item | None:
        try:
            return self._items[item]
        except IndexError:
            return None
            # self._item_does_not_exist(item)
    
    @dispatch(str)
    def __getitem__(self, item: str) -> Item:
        try:
            # print(self._name_item)
            return self._name_item[item]
        except KeyError:
            self._item_does_not_exist(item)
            
    @dispatch(Item)
    def __getitem__(self, item: Item) -> Item:
        return self[item.item_id]
    
    @dispatch(int or str, int or float)
    def existed_before(self, item: int or str, timestamp) -> bool:
        """Return True if `item_id` was released before `timestamp`"""
        return self[item].release_date < timestamp
    
    @dispatch(int or str, datetime.datetime)
    def existed_before(self, item: int or str, timestamp: datetime.datetime) -> bool:
        """Return True if `item_id` was released before `timestamp`"""
        return self[item].release_date < timestamp.timestamp()
    
    def reload_data(self, **kwargs):
        """Reload the Item data. Can be used to alter its source/table. Note that this will overwrite existing data."""
        if kwargs.get('db_file') is not None:
            global ITEM_DB_FILE
            ITEM_DB_FILE = kwargs['db_file']
        if kwargs.get('db_table') is not None:
            global ITEM_DB_TABLE
            ITEM_DB_TABLE = kwargs['db_table']
        self.__init__()
    
    @staticmethod
    def _item_does_not_exist(item: int or str):
        """Internal method for raising a descriptive KeyError in case an item could not be found"""
        e = None
        if isinstance(item, int):
            e = f"""Unable to find an existing Item that was mapped to item_id={item}"""
        if isinstance(item, str):
            e = f"""Unable to find an existing Item that was mapped to item_name={item}"""
        raise RuntimeError("Internal method was called with invalid input...") if e is None else KeyError(e)
    
    @property
    def most_traded(self) -> Tuple[Item, ...]:
        """The Items that are typically traded most frequently, according to https://prices.runescape.wiki/osrs/"""
        return tuple(self[i] for i in self._most_traded_item_ids)
    
    def high_alch_profit(self, item: int or str) -> int:
        """High alch profit of `item`, based on its HA value, current buy price and the nature rune buy price"""
        item = self[item]
        return item.alch_value - item.current_buy - min(self._nature_rune_price)
    
    def exists(self, item) -> bool:
        """Return True if `item` exists in the ItemDatabase"""
        try:
            return self[item] is not None
        except KeyError:
            return False
        
    def print_info(self):
        """Print class metadata"""
        print("\n *** Itemdb info ***\n\t* " + "\n\t* ".join((
            f"File path: {ITEM_DB_FILE}",
            f"Database table: '{ITEM_DB_TABLE}'",
            f"Initialization timestamp: {time.strftime('%d-%m-%y %X', time.localtime(self._init_time))}\n")))


itemdb = _ItemDb()
"""Singleton instance of the ItemDb; import and use this instead of ItemDb"""

__all__ = ["itemdb"]


if __name__ == '__main__':
    i = itemdb["Cannonball"]
    print([i.item_id for i in itemdb._items if isinstance(i, Item) and i.equipable])
    print([i.item_name for i in itemdb._items if isinstance(i, Item) and i.equipable])
    
    
    # print(i.__repr__())
    # print(i.sqlite_columns())
    # exit(1)
    
