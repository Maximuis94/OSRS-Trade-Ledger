"""
Module with Item Controller class

"""
import sqlite3
from typing import Literal, Tuple, Dict, Callable

from interfaces.db_entity import DbEntity
from item.model import Item as ItemModel
import global_variables.path as gp


class Item(ItemModel, DbEntity):
    """Item class with additional functionality"""
    sqlite_table: Literal['item'] = "item"
    sqlite_attributes: Tuple[str, ...] = ItemModel.__match_args__[:19]
    sqlite_insert = f""""INSERT OR REPLACE INTO "{sqlite_table}" {sqlite_attributes} VALUES
                    (:{", :".join(sqlite_attributes)})"""
    
    @property
    def sqlite_path(self) -> str:
        return gp.f_db_local
    
    @staticmethod
    def sqlite_table() -> Literal['item']:
        return "item"
    
    @property
    def sqlite_select(self) -> str:
        """SELECT an Item from the database. Requires an item_id to be passed as param"""
        return f"""SELECT * FROM item WHERE item_id=?"""
    
    @property
    def sqlite_insert(self) -> str:
        # return f""""INSERT OR REPLACE INTO item (id, item_id, item_name, members, alch_value, buy_limit, release_date,
        #             stackable, equipable, weight, update_ts, augment_data, remap_to, remap_price, remap_quantity,
        #             target_buy, target_sell, item_group, count_item) VALUES (:id, :item_id, :item_name, :members,
        #             :alch_value, :buy_limit, :release_date, :stackable, :equipable, :weight, :update_ts, :augment_data,
        #             :remap_to, :remap_price, :remap_quantity, :target_buy, :target_sell, :item_group, :count_item)"""
        return f""""INSERT OR REPLACE INTO "{self.sqlite_table()}" {self.sqlite_attributes()} VALUES
                    (:{", :".join(self.sqlite_attributes())})"""
    
    @staticmethod
    def sqlite_attributes() -> Tuple[str, ...]:
        return ItemModel.__match_args__[:19]
    
    @property
    def sqlite_row(self) -> Dict[str, any]:
        return {key: self.__getattribute__(key) for key in self.sqlite_attributes()}
    
    @property
    def sqlite_row_factory(self) -> Callable[[sqlite3.Cursor, tuple], any]:
        return lambda c, row: ItemModel(*row)
