"""
Various helper functions for inventory-related implementations

"""
import math

from global_variables.values import ge_tax_min_ts
from inventory.constants import transaction_db
from model.database import Database


def db_transactions() -> Database:
    """Returns a Database instance of the default Transaction database"""
    return Database(transaction_db)


def get_tax(transaction) -> int:
    
    return 0 if transaction.timestamp < ge_tax_min_ts else int(transaction.quantity * math.floor(.01*transaction.price))


def delta_n(a, b):
    return max(a, b) - min(a, b)
