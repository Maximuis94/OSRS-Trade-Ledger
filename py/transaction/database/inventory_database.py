"""
Module with TransactionDatabase handlers for inventory related logic

"""
import sqlite3

from typing import List, Dict

from dataclasses import dataclass, field

from abc import ABC

import global_variables.path as gp
from transaction.controller.inventory_entry import InventoryEntry
from transaction.database.basic_database import BasicTransactionDatabase


@dataclass(slots=True)
class InventoryDatabase(BasicTransactionDatabase):
    """
    Database designed mostly for interacting with the inventory.
    """
    
    path: str = gp.f_db_transaction_new
    """Path to the database"""
    
    inventory_entries: Dict[int, InventoryEntry] = field(default_factory=dict)
    
    def compute_inventory_entry(self, transaction_id: int) -> InventoryEntry:
        """
        'execute' the Transaction with the given ID and compute its cumulative values by executing all transactions
        prior to and including that transaction.
        
        Parameters
        ----------
        transaction_id : int
            ID of the transaction for which the InventoryEntry is to be computed.

        Returns
        -------

        """
        ...
    
    # def load_inventory(self):
    #     """Load all transactions and group them per InventoryEntry"""
    #     result = {}
    #     for item_id, transactions in self.load_transactions().items():
    #         ie = InventoryEntry(
    #             item_id=item_id,
    #             item=create_item(item_id),
    #             transactions=transactions,
    #             timestamp=transactions[-1].timestamp,
    #             balance=0,
    #             average_buy_price=0,
    #             profit=0,
    #             tax=0,
    #             invested_value=0,
    #             current_value=0,
    #             n_purchases=0,
    #             n_bought=0,
    #             n_sales=0,
    #             n_sold=0
    #         )
    #         result[item_id] = ie
    #     return result
    
    @classmethod
    def row_factory(cls, cursor, row):
        return InventoryEntry(**{c[0]: row[i] for i, c in enumerate(cursor.description)})
    
    @classmethod
    def load_inventory(cls):
        """Load all transactions and group them per InventoryEntry"""
        conn = cls._connect()
        c = conn.cursor()
        c.row_factory = cls.row_factory
    
    def execute_all_transactions(self, item_id: int):
        """
        Load and Execute all transactions that are eligible for execution and subsequently update inventory entries.
        Transactions are grouped per item_id and sorted chronologically.
        
        Returns
        -------
        
        
        1. Create a queue of all transactions of the given item_id;
            - Make sure each transaction has the same item_id
            - Make sure the queue starts at the transaction with the smallest timestamp that has executed=0
            - Given the queue start, ensure the queue contains *all* transactions that are to be executed. That is,
            transactions with the same item_id and a timestamp that exceeds the smallest timestamp that has executed=0
            - An exception to this rule are transactions with status=0, which can be ignored or skipped while iterating
        2. Iterate over this queue;
            A. For each transaction make sure the appropriate inventory configuration is applied;
                - If no transactions have been executed for this item so far, start at 0
                - Else, proceed with the configuration that results from executing the preceding transaction.
            B. Execute the transaction, updating the selected configuration with the resulting values.
                - Assume the transaction is a purchase or a sale, unless its tag is known to represent a specific type
                of transaction
                
        
        
        """
        
        


# idb = InventoryDatabase()
# for i, entry in idb.load_inventory().items():
#     print(i, entry)