"""
Module with the Database Entity implementation of an OSRS Item
"""
import math
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Callable, Dict, Literal, Optional, Tuple

import global_variables.path as gp
from entity.localdb import LocalDbEntity
from global_variables.local_file import rt_prices_snapshot as realtime


@dataclass(slots=True, match_args=False)
class Item(LocalDbEntity):
    """
    Item class that describes how an Item is represented in the associated SQLite database.
    A subset of attributes is defined via timeseries data using lazy loading (i.e. attributes are initialized when
    requested, but not by default)
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
    
    # Attributes derived from timeseries data - initialized lazily
    _current_ge: Optional[int] = field(default=None, init=False, compare=False)
    _current_buy: Optional[int] = field(default=None, init=False, compare=False)
    _current_sell: Optional[int] = field(default=None, init=False, compare=False)
    _current_avg: Optional[int] = field(default=None, init=False, compare=False)
    _avg_volume_day: Optional[int] = field(default=None, init=False, compare=False)
    _current_tax: Optional[int] = field(default=None, init=False, compare=False)
    _margin: Optional[int] = field(default=None, init=False, compare=False)
    _n_wiki: Optional[int] = field(default=None, init=False, compare=False)
    _n_avg5m_b: Optional[int] = field(default=None, init=False, compare=False)
    _n_avg5m_s: Optional[int] = field(default=None, init=False, compare=False)
    _n_rt_b: Optional[int] = field(default=None, init=False, compare=False)
    _n_rt_s: Optional[int] = field(default=None, init=False, compare=False)
    _live_data_loaded: bool = field(default=False, init=False, compare=False)
    
    def _load_live_data(self) -> None:
        """Lazy-loads live trade data attributes from the SQLite database."""
        c = sqlite3.connect(f"file:{gp.f_db_timeseries}?mode=ro", uri=True)
        c.row_factory = lambda _c, _r: _r[0]
        table = f"item{self.item_id:0>5}"
        rt_entry = realtime[self.item_id]
        
        self._current_ge = c.execute(
            f"""SELECT price FROM "{table}" WHERE src=0 ORDER BY timestamp DESC"""
        ).fetchone()
        self._current_buy = min(rt_entry)
        self._current_sell = max(rt_entry)
        self._current_avg = int(c.execute(
            f"""SELECT AVG((SELECT price FROM "{table}"
                            WHERE price > 0 AND src > 0
                            ORDER BY timestamp DESC LIMIT 7)) """
        ).fetchone())
        self._avg_volume_day = int(c.execute(
            f"""SELECT AVG(volume) FROM "{table}" WHERE src=0
                            ORDER BY timestamp DESC LIMIT 7"""
        ).fetchone())
        self._current_tax = min(5000000, int(math.floor(self._current_sell * 0.01)))
        self._margin = self._current_sell - self._current_buy
        
        sql_count = f"""SELECT COUNT(*) FROM "{table}" WHERE src=? """
        self._n_wiki = c.execute(sql_count, (0, )).fetchone()
        self._n_avg5m_b = c.execute(sql_count, (1, )).fetchone()
        self._n_avg5m_s = c.execute(sql_count, (2, )).fetchone()
        self._n_rt_b = c.execute(sql_count, (3, )).fetchone()
        self._n_rt_s = c.execute(sql_count, (4, )).fetchone()
        
        self._live_data_loaded = True

    @property
    def current_ge(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._current_ge

    @property
    def current_buy(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._current_buy

    @property
    def current_sell(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._current_sell

    @property
    def current_avg(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._current_avg

    @property
    def avg_volume_day(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._avg_volume_day

    @property
    def current_tax(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._current_tax

    @property
    def margin(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._margin

    @property
    def n_wiki(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._n_wiki

    @property
    def n_avg5m_b(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._n_avg5m_b

    @property
    def n_avg5m_s(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._n_avg5m_s

    @property
    def n_rt_b(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._n_rt_b

    @property
    def n_rt_s(self) -> int:
        if not self._live_data_loaded:
            self._load_live_data()
        return self._n_rt_s
    
    @property
    def sqlite_table(self) -> Literal['item']:
        """The name of the table in the sqlite database"""
        return "item"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], 'Item']]:
        """The row factory that is specifically designed for this DbEntity"""
        return lambda c, row: Item(*row)
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        """Attributes in the order they are defined in the associated dataclass and database"""
        return ("id", "item_id", "item_name", "members", "alch_value", "buy_limit",
                "stackable", "release_date", "equipable", "weight", "update_ts",
                "augment_data", "remap_to", "remap_price", "remap_quantity", "target_buy",
                "target_sell", "item_group", "count_item")
    
    @property
    def sqlite_row(self) -> Tuple[any, ...] | Dict[str, any]:
        """Tuple/dict representation of the class that can be inserted into the database; Complements sqlite_insert"""
        return tuple(getattr(self, a) for a in self.sqlite_attributes)
    
    @property
    def sqlite_select(self) -> str:
        """Executable SQLite SELECT statement. By default, select `sqlite_attributes` for all rows in the table."""
        return f"""SELECT {", ".join(self.sqlite_attributes)} FROM "{self.sqlite_table}" WHERE item_id=?"""
    
    @property
    def sqlite_insert(self) -> str:
        """Returns an executable SQLite INSERT statement"""
        placeholders = str(tuple("?" for _ in self.sqlite_attributes))
        return f""""INSERT OR REPLACE INTO "{self.sqlite_table}" {self.sqlite_attributes} VALUES {placeholders}"""
    
    @property
    def sqlite_update(self) -> str:
        """Returns an executable SQLite UPDATE statement"""
        raise NotImplementedError("Override this property if you wish to use it")
    
    @property
    def sqlite_delete(self) -> str:
        """Returns an executable SQLite DELETE statement"""
        raise NotImplementedError("Override this property if you wish to use it")
    
    @property
    def sqlite_create(self) -> Optional[str]:
        """CREATE TABLE statement for this entity"""
        return None
    
    @property
    def sqlite_trigger(self) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N triggers related to this entity to the database"""
        return None
    
    @property
    def sqlite_index(self) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N indices related to this entity to the database"""
        return None
    
    @classmethod
    @property
    def sqlite_view(cls) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N views used to display information about this entity to the database"""
        return None
    
    @staticmethod
    def create(item_id, c: Optional[sqlite3.Cursor] = None):
        """Creates an instance of Item with item_id=`item_id`"""
        # Note: this example uses a read-only connection.
        c = sqlite3.connect(f"file:{Item.sqlite_path.fget(Item)}?mode=ro", uri=True)
        c.row_factory = lambda cursor, row: Item(*row)
        query = f"""SELECT {", ".join(Item.sqlite_attributes.fget(Item))} FROM item WHERE item_id=?"""
        return c.execute(query, (item_id,)).fetchone()
