"""
A stock count is a special type of transaction of which the rules are based on the inventory it is applied to. It
describes an anchored state in the ledger. It results from a manually submitted stock count, and should therefore be
treated as a ground truth value, effectively overriding whatever the computed inventory at that point is.

Stock counts serve to counteract any transactions that have not been submitted by runelite, somehow.
It is defined as the observed quantities of a particular item at a particular time. Whatever the quantity of this item
was before this observation, after applying it, it is set to a specific value.

If there is a mismatch, this raises another question; how to treat the item deficit?
If the counted quantity is greater than the quantity in the inventory, a purchase may have gone unregistered
If the counted quantity is smaller than the quantity in the inventory, a sale may have gone unregistered.
In cases listed above, the desired course of action would be to execute this sale/purchase while applying the stock
count.
But if the item surplus is a result of items being sold before their buy offer was completed, and the purchase was
already inserted manually, there is no valid reason to treat it as a purchase.



"""
from typing import Tuple, Optional


class StockCount:
    """
    The stockcount class represents a user-defined InventoryEntry configurations at a specific point in time. Although
    the statistics leading up to that point will not necessarily be affected, the data will be anchored to specific
    values. Since the stock is manually counted, the values described by the StockCount should be correct, and any
    mismatches are hopefully the result of unsubmitted completed GE offers or something similar.
    
    Keep in mind that this class is designed to correct a mismatch that cannot be directly attributed to specific
    transactions. If the root cause can be identified through a set of transactions, you may want to set status to 0
    instead.
    
    Attributes
    ----------
    item_id : int
        OSRS item id of the item that was counted.
    
    timestamp : int
        UNIX timestamp of when the stock count was counted.
    
    counted_quantity: int
        The amount of items counted
    
    apply_price : Optional[int | Tuple[int, int]], None by default
        (Optional) The price(s) that is/are applied when applying the stock count itself. If one value is passed, this
        is either the buy- or sell- price, depending on whether the counted quantity is smaller or larger than the
        inventory quantity, respectively. If passed as a pair of prices, they describe the buy- and sell- price,
        respectively.
        If omitted, the assumed quantity will be set equal to the observed quantity, without it affecting the inventory
        statistics. From a practical point of view; the deficit in quantity will be corrected by inserting a
        transaction. This attributes controls the price listed in that transaction. If undefined, the transaction will
        not affect the current buy price, not the cumulative profit / other statistics.
    
    post_exe_buy_price : Optional[int], None by default
        (Optional) If given, the average buy price will be equal to this value after executing the transaction. This
        does not affect how the stock correction is implemented in the inventory like `apply_price` does
    
    
    Notes
    -----
    Whatever the underlying reason is, this entity is designed to manually set the value to the currently observed
    parameters. This can easily severely hamper the integrity of the inventory, as it is very hard, if not impossible,
    to automatically detect such a duplicate transaction.
    
    There are example plugins for counting item quantities across offers and various banks, like bank memory. If done
    according to certain guidelines and frequently, stock counts may help account for deficits on a short notice. This
    in turn could also help identify/prevent such deficits.
    
    The issue that this mechanism mitigates does not have a simple fix, as some transactions simply dont get registered.
    Another potential solution could be to track all 8 GE slots per account realtime. That is, create a DB table that
    keeps track of all trades. Given the information the exchange logger and flipping utilities plugins provide, it
    should be feasible to detect a missing transaction the moment it occurs.
    
    """
    item_id: int
    
    timestamp: int
    
    counted_quantity: int
    
    apply_price: Optional[int | Tuple[int, int]] = None
    
    post_exe_buy_price: Optional[int] = None
    
    