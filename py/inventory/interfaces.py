"""
Transaction-related interfaces


"""
from abc import ABC


class ITransaction(ABC):
    transaction_id: int
    item_id: int
    timestamp: int
    is_buy: bool
    quantity: int
    price: int
    status: int
    tag: str
    update_ts: int
    
    def __init__(self, *args, **kwargs):
        raise NotImplementedError
    
    

