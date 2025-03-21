import sqlite3
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Tuple

_item_table = "item"


@dataclass(order=True, match_args=True)
class Item:
    """
    Representation of an OSRS Item. An Item has various types of variables;
    - Static: Properties that define the item ingame that are rarely/never updated.
    - Augmentations: Can be both manually or automatically defined. Update frequency may vary, although generally they
        are assigned and updated infrequently.
    - Scraped variables: Derived from scraped database. Restricted to numerical values like prices / volumes.

    Item attributes are available for all items and are therefore restricted to particular attributes.
    """
    id: int = field(compare=True)
    item_id: int = field(compare=False)
    item_name: str = field(compare=False)
    members: bool = field(compare=False)
    alch_value: int = field(compare=False)
    buy_limit: int = field(compare=False)
    stackable: bool = field(compare=False)
    release_date: int = field(compare=False)
    equipable: bool = field(compare=False)
    weight: float = field(compare=False)
    update_ts: int = field(compare=False)
    augment_data: int = field(default=0, compare=False)
    remap_to: int = field(default=0, compare=False)
    remap_price: float = field(default=0, compare=False)
    remap_quantity: float = field(default=0, compare=False)
    target_buy: int = field(default=0, compare=False)
    target_sell: int = field(default=0, compare=False)
    item_group: str = field(default='', compare=False)
    count_item: bool = field(default=1, compare=False)
    
    # Live trade data -- Not from local db item table
    current_ge: int = field(default=0, compare=False)
    current_buy: int = field(default=0, compare=False)
    current_sell: int = field(default=0, compare=False)
    current_avg: int = field(default=0, compare=False)
    avg_volume_day: int = field(default=0, compare=False)
    current_tax: int = field(default=0, compare=False)
    margin: int = field(default=0, compare=False)
    
    n_wiki: int = field(default=-1, compare=False)
    n_avg5m_b: int = field(default=-1, compare=False)
    n_avg5m_s: int = field(default=-1, compare=False)
    n_rt_b: int = field(default=-1, compare=False)
    n_rt_s: int = field(default=-1, compare=False)
    
    @staticmethod
    def sqlite_columns() -> Tuple[str, ...]:
        """ Return the columns of the Item as stored in the sqlite database """
        return Item.__match_args__[:19]
    
    @staticmethod
    def sql_select() -> str:
        """SQL select statement for fetching all item attributes for all items from an SQLite db"""
        return f"""SELECT ({", ".join(Item.sqlite_columns())}) FROM "{_item_table}" """
    
    @staticmethod
    def row_factory(c: sqlite3.Cursor, row: sqlite3.Row):
        """row factory method that can be set to an SQLite cursor"""
        return Item(*row)
    
    def cast(self, var: str) -> datetime | int | float | str | bool:
        """Convert attribute `var` to the correct type"""
        if var in ["release_date", "update_ts"]:
            return datetime.fromtimestamp(self.__getattribute__(var))
        return self.__annotations__.get(var)(self.__getattribute__(var))
    
    def __int__(self):
        return self.item_id
    
    def __str__(self):
        return self.item_name
    
    def __repr__(self):
        return "\n\t* ".join([f"OSRS Item {self.item_name} (id={self.item_id})"]+\
                             [f"{el.capitalize()}: {self.cast(el)}"
                              for el in self.__match_args__[2:11]])
    
    def __eq__(self, other):
        return self.item_id == other.item_id
    
    def __ne__(self, other):
        return self.item_id != other.item_id
    
    def print_item_info(self) -> str:
        """Print all attributes of this Item"""
        s = [f"Item {self.item_name} (id={self.item_id})"]
        
        for a in fields(self):
            s.append(f"{a.name}: {a.type.__name__} = {getattr(self, a.name)}")
        print("\n\t".join(s), end="\n\n")
