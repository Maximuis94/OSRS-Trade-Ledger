"""
This module contains the model of the inventory

"""
from import_parent_folder import recursive_import
from global_variables.importer import *
from model.transaction import Transaction

del recursive_import


class Purchase(Transaction):
    """
    Transaction that represents a purchase of goods. Execution increases the quantity and it alters the average buy
    price.

    """
    
    def __init__(self, transaction: dict, **kwargs):
        super().__init__(transaction_data=transaction, is_buy=True,
                         tag=go.transaction_tag_purchase_m if kwargs.get('manual') else go.transaction_tag_purchase)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     """ Execute this purchase within InventoryEntry `ie` """
    #     self.verify_execution_input(ie=ie)
    #     q0 = ie.quantity if ie.quantity > 0 else 0
    #     ie.price = (q0 * ie.price + self.price * self.quantity) / (q0 + self.quantity)
    #     ie.n_purchases += 1
    #     return self.post_transaction_entry_update(ie=ie, delta_q=self.quantity)
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass


class Sale(Transaction):
    """
    Transaction that represents a purchase of goods. Execution increases the quantity and it alters the average buy
    price.

    """
    
    def __init__(self, transaction: dict, **kwargs):
        transaction['is_buy'] = False
        transaction['tag'] = go.transaction_tag_sale_m if kwargs.get('manual') else go.transaction_tag_sale
        super().__init__(transaction_data=transaction)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     """ Execute this sale within InventoryEntry `ie` """
    #     self.verify_execution_input(ie=ie)
    #     tax = 0 if self.timestamp < gv.ge_tax_min_ts else int(min(5000000.0, self.price * 0.01))
    #     profit = (self.price - ie.price - tax) * self.quantity
    #     ie.n_sales += 1
    #     return self.post_transaction_entry_update(ie=ie, delta_q=-1*self.quantity, tax=tax*self.quantity, profit=profit)
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass


class Correction(Transaction):
    """
    Transaction that is typically generated if a sale would take place, but results in a negative item quantity. This
    Correction is inserted right before said sale to account for the missing stock.

    Note that this was at a point implemented as a naive quick-fix. In some cases it works perfectly, but it will
    affect inventory integrity if a purchase is submitted after a sale.
    """
    
    def __init__(self, transaction: dict, **kwargs):
        transaction['is_buy'] = True
        transaction['tag'] = go.transaction_tag_correction
        super().__init__(transaction_data=transaction)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     """ Execute this correction within InventoryEntry `ie` """
    #     self.verify_execution_input(ie=ie)
    #     q0 = ie.quantity if ie.quantity > 0 else 0
    #     ie.price = (q0 * ie.price + self.price * self.quantity) / (q0 + self.quantity)
    #     ie.n_purchases += 1
    #     return self.post_transaction_entry_update(ie=ie, delta_q=self.quantity)
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass


class Consumption(Transaction):
    """
    Transaction that represents the consumption of goods. Execution decreases the quantity, without affecting registered
    profits for that item.

    """
    
    def __init__(self, transaction: dict):
        transaction['is_buy'] = False
        transaction['tag'] = go.transaction_tag_consumed
        super().__init__(transaction_data=transaction)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     """ Execute item consumption within InventoryEntry `ie` """
    #     self.verify_execution_input(ie=ie)
    #     return self.post_transaction_entry_update(ie=ie, delta_q=-1*self.quantity)
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass


class Production(Transaction):
    """
    Transaction that represents production of goods. Execution increases the quantity and it alters the average buy
    price. In a sense it is identical to a purchase, but it is tagged separately to be able to distinguish it from
    standard purchases. Aside from produced goods, this may also refer to any introduction of items that is not the
    result of a sale.

    """
    
    def __init__(self, transaction: dict, **kwargs):
        transaction['is_buy'] = True
        transaction['tag'] = go.transaction_tag_produced
        super().__init__(transaction_data=transaction)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     self.verify_execution_input(ie=ie)
    #     q0 = ie.quantity if ie.quantity > 0 else 0
    #     ie.price = (q0 * ie.price + self.price * self.quantity) / (q0 + self.quantity)
    #     ie.n_purchases += 1
    #     return self.post_transaction_entry_update(ie=ie, delta_q=self.quantity)
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass


class StockCount(Transaction):
    """
    Transaction submitted after manually counting stock. Post-transaction inventory is fixed using these parameters.
    - The quantity of this item will be set to Transaction.quantity.
    - If Transaction.status > 1, the average buy price of this item will be set to Transaction.status
    - If the Transaction results in a reduction of item quantity and Transaction.price > 0, the quantity deficit will be
        sold at Transaction.price, thus generating profit or loss, depending on the inventory before executing.

    Note that the actual outcome of this transaction is dynamic and may change if preceding transactions are modified.

    Due to the nature of StockCounts, keep in mind that even having to use them is likely to affect the integrity of the
    inventory, as the resulting values are simply anchored to the given input.
    """
    
    def __init__(self, transaction: dict, **kwargs):
        transaction['is_buy'] = False
        transaction['tag'] = go.transaction_tag_counted
        super().__init__(transaction_data=transaction)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     """ Execute this StockCount Transaction for given InventoryEntry `ie` """
    #     self.verify_execution_input(ie=ie)
    #     delta_q = ie.quantity - self.quantity
    #     ie.quantity = self.quantity
    #
    #     # A certain amount of items was sold and the sell_price was explicitly specified.
    #     if self.price > 0 and delta_q > 0:
    #         tax = min(5000000, int(0.01*self.price))
    #         profit = delta_q * (self.price - ie.price - tax)
    #         ie.profit += profit
    #         ie.tax += tax
    #     else:
    #         tax, profit = 0, 0
    #     if self.status > 1:
    #         ie.price = self.status
    #     ie.value = max(0, ie.quantity * ie.price)
    #     ie.log_transaction(t=self, profit=profit, tax=tax)
    #     ie.last_exe_ts = self.timestamp
    #     return ie
    #
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass


class Bond(Transaction):
    """
    Bond purchase transaction. Bond transactions are tracked for providing insight in bond expenses, but as an item
    they are also assumed to be consumed upon purchase.
    """
    
    def __init__(self, transaction: dict, **kwargs):
        transaction['item_id'] = 13190
        transaction['is_buy'] = True
        transaction['tag'] = go.transaction_tag_bond
        super().__init__(transaction_data=transaction)
    
    # def execute_transaction(self, ie: InventoryEntry):
    #     self.verify_execution_input(ie=ie)
    #     q0 = ie.quantity if ie.quantity > 0 else 0
    #     ie.price = (q0 * ie.price + self.price * self.quantity) / (q0 + self.quantity)
    #     ie.quantity += self.quantity
    #     ie.value = max(0, ie.price * ie.quantity)
    #     ie.n_purchases += 1
    #     ie.execution_log[self.transaction_id] = {'balance': ie.quantity, 'buy_price': ie.price, 'profit': 0,
    #                                              'tax': 0, 'execution_ts': ie.update_ts,
    #                                              'totals': ie.get_values_snapshot()}
    #     return ie
    #
    # def undo_transaction(self, ie: InventoryEntry):
    #     pass
