"""
This module contains the model for an OSRS item

"""
import pickle
import sqlite3
from collections import namedtuple
from collections.abc import Iterable
from typing import List, Tuple, overload

import numpy as np

from venv_auto_loader.active_venv import *
from backend.download import realtime_prices
from file.file import File
from global_variables.importer import *
from global_variables.variables import columns
from model.data_source import SRC
from model.database import Database
from model.item import Item
from sqlite.row_factories import factory_dict

__t0__ = time.perf_counter()

target_prices_eval_t0 = 1714065700 #int(time.time() - gc.timespan_target_prices_eval*86400)
eval_t1 = int(time.time())

_itemdb = sqlite3.connect(gp.f_db_local)


def create_item(item_id: int, itemdb: sqlite3.Connection = None, read_only: bool = False) -> Item:
    """
    Create a new Item instance, provided `item_id` does not exist within the item database.
    The item should have a reference id logged somewhere to validate its existence. If it does, collect data from
    various sources and update its attributes.
    After doing so, submit the new item to the database.
    
    Parameters
    ----------
    item_id : int
        The item_id of the Item that is to be created
    itemdb : sqlite3.Connection, optional, None by default
        Can be passed to use a different connection instead
    read_only : bool, optional, False by default
        If True, itemdb connection will be used for SELECT statements only

    Returns
    -------
    Item
        Item instance of `item_id`
    
    Raises
    ------
    ValueError
        If there is no existing reference
        that this is a valid OSRS item_id, a ValueError will be raised.
    
    Notes
    -----
    While this method can be invoked manually, it should be already integrated in the rbpi data scraper. This method
    mostly serves as reference on how Item data is scraped, much like the timeseries download methods in the
    backend.download module. Alternatively, the item scraper update frequency can be increased to ensure item updates
    are inserted as soon as possible.
    """
    # TODO step 1: check if item reference exists
    if itemdb is None:
        itemdb = _itemdb
    elif not itemdb:
        itemdb = sqlite3.connect(database=f"file:{gp.f_db_local}?mode=ro", uri=True)
    
    try:
        # exists = go.id_name[item_id] is not None
        # print(itemdb.execute("""SELECT * FROM item WHERE item_id=?""", (item_id,)).fetchone())
        # print(item_id, type(item_id))
        return Item(*itemdb.execute("""SELECT * FROM item WHERE item_id=?""", (item_id,)).fetchone())
        # if isinstance(i, Item):
        #     return i
        # else:
        #     raise TypeError(f"Unexpected type {type(i)} in create_item()...")
    except IndexError:
        raise ValueError(f'No valid reference could be found for item_id={item_id}. If the item was added recently, '
                         f'item data')
    except TypeError as e:
        print(f'TypeError for item_id={item_id} ')
        if read_only:
            raise e
        # db = pickle.load(open(gp.f_db_rbpi_item, 'rb'))
        # db = db.loc[db['item_id'] == item_id].iloc[0].to_dict()
        # con = sqlite3.connect(gp.f_db_local)
        from sqlite.databases import item
        # print(item.columns)
        db = sqlite3.connect(gp.f_db_rbpi_item)
        db.row_factory = factory_dict
        e = augment_itemdb_entry(Item(**db.execute("SELECT * FROM 'itemdb' WHERE item_id=?", (item_id,)).fetchone())).__dict__
        # print({k: e.get(k) for k in item.columns})
        # exit(123)
        print(item.insert_dict, {k: e.get(k) for k in item.columns})
        itemdb.execute(item.insert_dict, {k: e.get(k) for k in item.columns})
        itemdb.commit()
        # con.close()
        raise e
    
    # TODO step 2: define item attributes and assign values
    
    # At this point, item_id yields an indexerror / None from id_name, which means this item_id was not parsed
    # from the database.
    # item = Item(#Item._types
    # id = item_id,
    # item_id = item_id,
    # item_name = '',
    # members = False,
    # alch_value = 0,
    # buy_limit = 0,
    # stackable = False
    # release_date = 0
    # equipable = False
    # weight = 0.0
    # augment_data = False
    # remap_to = 0
    # remap_price = 0.0
    # remap_quantity = 0.0
    # target_buy = 0
    # target_sell = 0
    # item_group = ''
    # update_ts = 0
    #
    # # Live trade data
    # current_ge = 0
    # current_buy = 0
    # current_sell = 0
    # current_avg = 0
    # avg_volume_day = 0
    # current_tax = 0
    # margin = 0
    
    
def update_item(i: Item):
    """ Update the item database entry for item_id=`i.item_id` with values from Item `i` """
    ...


def remap_item(item: Item, price, quantity) -> Tuple[Item, int, int]:
    if item.remap_to > 0:
        return create_item(item.remap_to), int(price*item.remap_price), int(quantity*item.remap_quantity)
    else:
        return item, price, quantity


def augment_itemdb_entry(item: Item, overwrite_data: bool = False) -> Item:
    """
    Augment the item dict with an item_group and remap data, if applicable.
    
    Parameters
    ----------
    item : Item
        Item that is to be augmented
    overwrite_data : bool, optional, False by default
        True if existing data should be overwritten, if an entry exists. Acts as safeguard for overwriting manually
        defined groups.
        

    Returns
    -------
    item : Item
        item with modified values (if any)
    
    
    Notes
    -----
    Unless this implementation was altered at some point or you wish to restore default values, there is no benefit for
    augmenting item data multiple times. If values have been altered manually, executing this method again may result in
    values being modified/reverted without any warning.
    TODO: Add flag to exclude item from augmentation to prevent messing with manual configs
        Best approach is to add a column to item database that ensures item data wont get modified.
    """
    name = item.item_name
    
    # todo encode item_groups as integers with a string mapped to each integer(?)
    item_group = item.item_group
    if item_group is None:
        item_group = ''
    
    # potion group; remap 1-, 2-, 3-dosed potions to their 4-dosed equivalent and remap price+quantity accordingly
    if name[-3:] in ['(1)', '(2)', '(3)', '(4)'] and \
            False not in [go.name_id.get(name[:-3]+f'({d})') is not None for d in (1, 2, 3, 4)]:
        n_doses = int(name[-2])
        if n_doses < 4:
            item.remap_to = go.name_id.get(name[:-3]+'(4)')
            item.remap_price = 4/n_doses
            item.remap_quantity = n_doses/4
        item_group = 'potion'
    
    # cooking group
    elif name[:4] == 'Raw ':
        
        item_group = 'cooking'
    
    # food group
    elif go.name_id.get('Raw '+name.lower()) is not None:
        item_group = 'food'
    
    # herblore group
    elif name[-12:] == 'potion (unf)' or name[:6] == 'Grimy ' or go.name_id.get('Grimy '+name.lower()) is not None:
        item_group = 'herblore'
        
    # tablet group
    elif name[-8:] == '(tablet)':
        item_group = 'tablet'
        
    # Farming group; remap tree seeds to saplings
    elif name[-4:] == 'seed' and 'crystal' not in name.lower() or name[-7:] == 'sapling':
        item_group = 'farming'
        
        if name[-7:] != 'sapling' and go.name_id.get(
                name.replace('tree ', '').replace('seed', 'sapling')) is not None:
            item.remap_to = go.name_id.get(name.replace('tree ', '').replace('seed', 'sapling'))
            item.remap_price = 1.0
            item.remap_quantity = 1.0
            
    # smithing group
    elif name[-4:] in (' ore', ' bar', 'Coal') and name.split(' ')[0] not in ('Chocolate', 'Monkey'):
            item_group = 'smithing'
    
    # prayer group
    elif name[-5:].lower() == 'bones' and item.weight >= .1:
        item_group = 'prayer'
    
    # todo fletching group
    
    # todo ammo group
    
    # todo crafting group
    
    elif name[-5:] == ' rune':
        item_group = 'runes'
    # if item_group != '':
    #     print(name, item_group)
    
    # If augment_data has another value, it is to be considered immutable; truth is evaluated with n%2==1
    if item.augment_data in (0, 1):
        item.augment_data = uo.assign_augmented_item_tag(item=item.__dict__)
    
    # Only fill empty entries when not overwriting
    if overwrite_data or item.item_group == '':
        item.item_group = item_group
        
    return item


def get_item(item_id: int, a: str = None) -> Item or any:
    """ Return an Item of the given `item_id`, or a specific attribute if `a` is specified """
    return idb.get_item(item_id) if a is None else idb.get_item(item_id).__dict__.get(a)


def item_exists(item_id: int) -> bool:
    """ Return True if an item with item_id=`item_id` exists in the database or as wiki mapping entry """
    if go.itemdb.get(item_id) is not None:
        return True
    else:
        return gl.item_wiki_mapping.get_item_mapping(item_id=item_id) is not None


class ItemController(Database):
    table_name: str
    tuple = var.Item
    wiki = Database(path=gp.f_db_timeseries, read_only=True)
    
    def __init__(self, augment_items: bool = True, read_only: bool = False, **kwargs):
        path = kwargs.pop('path', gp.f_db_local)
        self.table_name = kwargs.pop('table_name', 'item')
        super().__init__(File(path), read_only=read_only, tables=self.table_name, row_tuple=namedtuple("ItemTuple", Item.sqlite_columns()))
        self.table = self.tables.get(self.table_name)
        self.tables = {self.table_name: self.table}
        self.row_factory = self.augmented_item_factory if augment_items else self.item_factory
        self.add_cursor(key=Item, rf=self.item_factory)
        self.add_cursor(key=self.tuple)
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
    
    def update_item(self, item: Item, attribute_subset: Iterable = None, replace: bool = True,
                    return_row: bool = False):
        """ Overwrite db entry using values from `item`, pass an Iterable with attributes to update only a subset """
        raise NotImplementedError("Added @ 26-06")
        if attribute_subset is not None and replace:
            row = self.get_item(item_id=item.item_id).sql_row()
            row.update({k: item.__dict__.get(k) for k in attribute_subset})
        else:
            row = item.sql_row()
        if return_row:
            return row
        else:
            self.insert_rows(table_name=self.table_name, rows=[row], replace=replace)
    
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
                         (f"rt_{'buy'if buy_price else 'sell'}", item_id)).fetchone()
        
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
            volumes = ItemController.wiki.execute(f"SELECT volume FROM item{i.item_id:0>5} WHERE src={SRC.w}", p, factory=0).fetchall()
            if len(volumes) == 0:
                i.avg_volume_day = 0
            else:
                i.avg_volume_day = int(np.average(volumes[-1*min(len(volumes), gc.n_days_volume_avg):]))
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
            

if __name__ == '__main__':
    idb = ItemController(path=gp.f_db_local)
    idb["Mahogany logs"].print_item_info()
    
    
