"""
An item production rule is a process that can be applied. Applying the rule may consume certain amounts of specific
items for applying the rule once. Subsequently, it may produce a certain quantity of an item. The majority (if not all)
of the production rules produce a specific item, although this does not necessarily imply that this is always the case.



"""
from abc import ABC, ABCMeta

from typing import NamedTuple


class ProductionItem(NamedTuple):
    """
    An atomic unit used to describe a ProductionRule. It is a certain amount of item that is produced/consumed.
    """
    
    item_id: int
    """The item_id of the item. The item_id should refer to an existing item."""
    
    quantity: int
    """The quantity of the item. The quantity is expected to be a positive integer."""
    

class ProductionRuleMeta(ABCMeta):
    """
    Template ProductionRule class
    
    
    """
    def __subclasscheck__(cls, subclass):
        """
        TODO: Implement subclass check for ProductionRule.
        The subclass should have an item_id that refers to an existing item, and it should have a quantity greater than
        0.
        """
        