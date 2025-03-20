"""
This module contains all logic applied when executing transaction.

The logic that is applied, is routed via a single function that will identify the appropriate function.

"""
from typing import Dict


def execute_transaction(entry, **kwargs) -> Dict[str, any]:
    """
    Execute the transaction as described by the keyword args. Use the values from entry as starting point.
    
    Parameters
    ----------
    entry
        The InventoryEntry that is to be updated
    
    Other Parameters
    ----------------
    item_id : int
        The item_id of the item that is traded
    price : int
        The price at which the item is traded
    quantity : int
        The amount of items that are being traded
    

    Returns
    -------

    """

