"""
This module contains class definitions of all Transaction objects. Defining a transaction and loading its values is
implemented in the base class.

Subclasses of Transaction are used to assess which logic to apply when executing these transactions in an Inventory.
Subclasses also have some attributes that are class-specific, e.g. the is_buy attribute of a Purchase is always True.



https://blog.teclado.com/python-abc-abstract-base-classes/

"""

import sqlite3
import time
from abc import ABC, abstractmethod

from overrides import override

from global_variables.importer import *
from model.item import Item
from global_variables.data_classes import Transaction as _Transaction


# from model.database import Database as Db

# This transaction_parser is used globally for fetching transactions.
# transaction_db = Db(path=gp.f_db_transaction, parse_tables=True, read_only=True)


class Transaction(_Transaction):
    """
    Abstract Base Class for a Transaction. Serves as a template for transactions.
    
    Database interactions are handled by the Database class
    
    Transactions can be created by passing a dict, or loading sqlite db data. If a large amount of transactions is to be
    read from the database, consider manually parsing the transactions and passing their dicts or defining a connection
    with a corresponding cursor and passing that along with a transaction_id.
    
    Database interactions are defined in this base class, whereas logic specific to types of Transactions is implemented
    in specific class definitions.
    
    Values are assigned a specific typing in this base class, this typing is preserved if values are loaded through
    Transaction.load_values(), provided its attributes have not been altered manually before calling this method.
    
    Transactions should be executed via Transaction.execute(InventoryEntry ie), with ie being the InventoryEntry the
    Transaction should be executed in.
    
    Attributes
    ----------
    transaction_id : int
        A unique identified assigned to each Transaction
    item_id : int
        The item_id of the item that is traded in this Transaction
    timestamp : int
        The unix timestamp of when the transaction occurred
    is_buy : bool
        Flag indicating whether something is bought (True) or sold (False)
    quantity : int
        The amount of items that are traded in this Transaction
    price : int
        The price per item traded in this transaction
    status : int
        Transaction status; 0 means it is disabled, 1 means enabled. If it is 0, it will be ignored.
    tag : str
        A tag that indicates what kind of Transaction this is. Used to assign a Transaction subclass during creation.
        Additionally, an uppercase tag indicates the transaction was submitted automatically, lowercase manually.
    update_ts : int
        Unix timestamp that indicates when this transaction was created or last updated.
    
    """
    # sqlite statement for reading one transaction from the sqlite database
    table_name = "'transaction'"
    columns = _Transaction.__match_args__
    types = _Transaction.__annotations__
    can_update = ('item_id', 'timestamp', 'quantity', 'price')
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.item = kwargs.get('item')
        if isinstance(self.item_id, Item):
            self.item = self.item_id
            self.item_id = self.item.item_id
        
        try:
            self.__dict__.update(self.item.__dict__)
        except AttributeError:
            ...
    
    def update_transaction(self, values: dict):
        """ Update transaction attributes with values from `updated_values`. Certain attributes cannot be updated. """
        print(f'Updating transaction {self.transaction_id}')
        for k, v in {k: v for k, v in values.items()}.items():
            _v = self.__dict__.get(k)
            if self._set_value(var=k, value=v):
                print(f'\t{k}={v} (old value={_v})')
            else:
                print(f'\tDid not set {k}={v}')
        self.update_ts = int(time.time())
    
    def toggle_status(self):
        """ Toggle Transaction status (0 means it will be ignored) """
        self.status = int(not bool(self.status))
    
    def _set_value(self, var: str, value) -> bool:
        """
        Attempt to set the value of  attribute `var` to `value`. If `var` is not allowed to be updated, do not update
        the attribute. If the type of `value` is different than the expected type of `var`, attempt to cast it to that
        type before setting it.
        
        Parameters
        ----------
        var : str
            The name of the attribute that is to be updated.
        value : any
            The value attribute `var` should be updated to.

        Returns
        -------
        bool
            True if the value was updated, False if not
        """
        if var in self.can_update:
            self.__dict__[var] = value if isinstance(value, self.types.get(var)) else self.types.get(var)(value)
            return True
        return False
        
    def sql_row(self):
        """ Return a dict that can be submitted to the database """
        return {c: self.__dict__.get(c) for c in self.columns}
        
        
    # @abstractmethod
    # def execute_transaction(self, ie: InventoryEntry) -> InventoryEntry:
    #     """ Transaction-specific logic that is applied upon execution. """
    #     ...
    #
    # @abstractmethod
    # def undo_transaction(self, ie: InventoryEntry) -> InventoryEntry:
    #     """ Undo this transaction from the given InventoryEntry object and return the modified InventoryEntry """
    #     ...
    
    @staticmethod
    def factory_transaction(cursor: sqlite3.Cursor, r):
        """ Transaction factory that can be used to return database entries as Transactions """
        return Transaction(transaction_id=r[0], kwargs={c[0]: r[i+1] for i, c in enumerate(Transaction.columns[1:])})
    

class Purchase(Transaction):
    """
    Transaction that represents a purchase of goods. Execution increases the quantity and it alters the average buy
    price.
    
    """
    
    def __init__(self, transaction: dict, manual: bool = False, **kwargs):
        transaction['is_buy'] = True
        transaction['tag'] = go.transaction_tag_purchase_m if manual else go.transaction_tag_purchase
        super().__init__(transaction_data=transaction)


class Sale(Transaction):
    """
    Transaction that represents a purchase of goods. Execution increases the quantity and it alters the average buy
    price.
    
    """
    
    def __init__(self, transaction: dict, **kwargs):
        transaction['is_buy'] = False
        transaction['tag'] = go.transaction_tag_sale_m if kwargs.get('manual') else go.transaction_tag_sale
        super().__init__(transaction_data=transaction)


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


class Consumption(Transaction):
    """
    Transaction that represents the consumption of goods. Execution decreases the quantity, without affecting registered
    profits for that item.
    
    """
    
    def __init__(self, transaction: dict):
        transaction['is_buy'] = False
        transaction['tag'] = go.transaction_tag_consumed
        super().__init__(transaction_data=transaction)


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
        self.new_price = transaction.get('status')
    
    @override
    def toggle_status(self):
        """ Status serves a different purpose in StockCount Transactions """
        raise AttributeError(f'Unable to toggle the status of a Stock Correction Transaction')


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


_transactions_by_tag = {
    go.transaction_tag_purchase: Purchase,
    go.transaction_tag_purchase_m: Purchase,
    go.transaction_tag_sale: Sale,
    go.transaction_tag_sale_m: Sale,
    go.transaction_tag_correction: Correction,
    go.transaction_tag_consumed: Consumption,
    go.transaction_tag_produced: Production,
    go.transaction_tag_counted: StockCount,
    go.transaction_tag_bond: Bond
}


def transaction_from_dict(t_dict: dict) -> Transaction:
    """ Convert transaction dict `t` into a Transaction subclass. Converts legacy tags as well. """
    tag = t_dict.get('tag')
    if tag in ('', 'm', 'e', 'p', 'c'):
        if tag == '' or tag == 'm':
            t_dict['tag'] = 'B' if t_dict.get('is_buy') else 'S'
        elif tag == 'e' or tag == 'p':
            t_dict['tag'] = 'b' if t_dict.get('is_buy') else 's'
        elif tag == 'c':
            t_dict['tag'] = 'd'
        tag = t_dict.get('tag')
    try:
        return _transactions_by_tag.get(tag)(transaction=t_dict, manual=tag.isupper())
    except TypeError:
        print('typeerror', t_dict)
        raise TypeError


def factory_transaction_subclass(c: sqlite3.Cursor, row) -> Transaction:
    """ Row factory that generates a subclass of Transaction, based on the given tag """
    return transaction_from_dict(
        t_dict={col[0]: gd.transaction_types.get(col[0]).py(row[idx]) for idx, col in enumerate(c.description)}
    )
