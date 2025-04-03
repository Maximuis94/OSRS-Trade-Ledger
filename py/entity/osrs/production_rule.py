"""
Module with Production Rule class

"""
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Callable, Dict, List, Literal, Optional, Tuple

from entity.localdb import LocalDbEntity

_inner_sep  = "_"
"""The inner separator. Used to separate the item_id from the quantity."""

_outer_sep = " "
"""The outer separator. Used to different (item_id, quantity) pairs"""

@dataclass(slots=True, match_args=False)
class ProductionRule(LocalDbEntity):
    """
    
    
    """
    name: str
    input : Iterable[Tuple[int, int]]
    output : Iterable[Tuple[int, int]]
    gp: int = 0
    
    @property
    def sqlite_table(self) -> Literal['production_rule']:
        """The name of the table in the sqlite database"""
        return "production_rule"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], 'ProductionRule']]:
        """The row factory that is specifically designed for this DbEntity"""
        return lambda c, row: ProductionRule(row[0], self.decode(row[1]), self.decode(row[2]), row[3])
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        """Attributes in the order they are defined in the associated dataclass and database"""
        return "name", "input", "output", "gp"
    
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
    
    @property
    def sqlite_view(self) -> Optional[str | Iterable[str]]:
        """Executable SQL for adding 0-N views used to display information about this entity to the database"""
        return None
    
    @staticmethod
    def encode(to_encode: Iterable[Tuple[int, int]]) -> str:
        """
        Encode one or more production rule elements, passed as an Iterable of item_id, quantity pairs.
        
        Parameters
        ----------
        to_encode : Iterable[Tuple[int, int]]
            An iterable of (item_id, quantity) pairs.

        Returns
        -------
        str
            The encoded string.
        """
        return _outer_sep.join([f"{el[0]}{_inner_sep}{el[1]}" for el in to_encode])
    
    @staticmethod
    def decode(to_decode: str) -> List[Tuple[int, int]]:
        """
        Decode a production-rule element. It is passed as a string that can be separated into one or more
        (item_id, quantity) tuples.
        
        Parameters
        ----------
        to_decode : str
            The string that is to be decoded
            
        Returns
        -------
        List[Tuple[int, int]]
            The decoded elements, returned as a list of item_id, quantity pairs
        """
        return [tuple([int(value) for value in el.split(_inner_sep)[:2]]) for el in to_decode.split(_outer_sep)]
    