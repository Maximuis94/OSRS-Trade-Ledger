"""
This module contains the model for an OSRS item

"""

from typing import Tuple, Dict
import global_variables.osrs as go
import global_variables.path as gp
from common.classes.item import Item as _Item
# import global_variables.variables as var
from global_variables.variables import get_dtype, item_columns, item_metadata


class Item(_Item):
    """
    Representation of an OSRS Item. An Item has various types of variables;
    - Static: Properties that define the item ingame that are rarely/never updated.
    - Augmentations: Can be both manually or automatically defined. Update frequency may vary, although generally they
        are assigned and updated infrequently.
    - Scraped variables: Derived from scraped database. Restricted to numerical values like prices / volumes.
    
    Item attributes are available for all items and are therefore restricted to particular attributes.
    """

    _types: Dict[str, any] = {c: get_dtype(c) for c in item_columns}
    sql_dtypes: Dict[str, any] = {c: dt.sql for c, dt in _types.items()}
    py_dtypes: Dict[str, any] = {c: dt.py for c, dt in _types.items()}
    df_dtypes: Dict[str, any] = {c: dt.df for c, dt in _types.items()}
    metadata: Tuple[str] = item_metadata
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = 'item'
        self.db_path = gp.f_db_item
    
    @staticmethod
    def exists(item_id: int) -> bool:
        """ Assess whether there is an existing item with `item_id` as id """
        try:
            return go.id_name[item_id] is not None
        except IndexError:
            return False


if __name__ == "__main__":
    i = Item(2)
    for k, v in i.__dict__.items():
        print(k, v)