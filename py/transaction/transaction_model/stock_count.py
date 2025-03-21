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
    
    item_id: int
    
    timestamp: int
    
    counted_quantity: int
    
    apply_price: Optional[int | Tuple[int, int]] = None
    
    post_exe_buy_price: Optional[int] = None
    
    