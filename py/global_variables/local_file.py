
"""
This module contains all LocalFile and FlagFile subclasses, which serve as an interface for locally saved files that
 are not databases. See model.local_file for the class definition and a more thorough explanation.

All subclasses defined below are Singletons as each instance refers to the same file.
The idea behind this design is that the underlying files are only interacted with through these classes.

LocalFile subclasses represent files that require frequent updating from external sources, e.g. RealtimePricesSnapshot.
As updating this data almost always involves making a request, the LocalFile classes have integrated methods for keeping
 the files updated. This serves as a safeguard against outdated data and excessive updating.

"""
import datetime

from overrides import override

from venv_auto_loader.active_venv import *
import backend.download as dl
import global_variables.configurations as cfg
import global_variables.path as gp
from global_variables.classes import SingletonMeta
from common.classes.local_file import LocalFile, FlagFile
__t0__ = time.perf_counter()
debug = True
    

class RealtimePricesSnapshot(LocalFile, metaclass=SingletonMeta):
    """
    Class used for accessing and updating realtime price data.
    
    An instance of this class is created below. It's recommended usage is though util.osrs.buy_price,
    util.osrs.sell_price and util.osrs.prices.
    
    The updater is integrated in this class; the snapshot is updated every X seconds, with X being the update frequency
    value set below.
    
    
    """
    
    def __init__(self):
        update_frequency = self.get_update_frequency()
        self.download_from_rbpi = update_frequency == cfg.rt_rbpi_update_frequency
        super().__init__(path=gp.local_file_rt_prices, update_frequency=update_frequency)
        print(f'Setting up RealtimePricesSnapshot class (allow_rbpi_download={self.download_from_rbpi})')
    
    @override
    def get_value(self, update_check: bool = False, **kwargs) -> Tuple[int, int]:
        """ Get the realtime low- and high- prices for the item specified with `item_id` """
        # Verify if the data is available, and check for update if applicable
        self.update()
        return self.file_content.get(kwargs.get('item_id'))
    
    @override
    def verify(self) -> bool:
        """ Return True if file_content is a dict and if one of its entries is a tuple of length 2 """
        if not isinstance(self.file_content, dict):
            print(f'Type verification for file {self.path} failed. File type={type(self.file_content)} (Expected=dict)')
            return False
        try:
            el = self.file_content.get(list(self.file_content.keys())[0])
            return isinstance(el, tuple) and len(el) == 2
        except IndexError:
            return False
    
    def updated_content(self, **kwargs) -> dict:
        """ Download a realtime prices snapshot and return it """
        try:
            return dl.realtime_prices(check_rbpi=self.download_from_rbpi, force_rbpi=self.download_from_rbpi)
        except FileNotFoundError:
            self.update_frequency, self.download_from_rbpi = cfg.rt_update_frequency, False
            return self.updated_content()
            
    @override
    def merge_content(self, new_data):
        """ Overwrite old entries with new entries, then return the updated dict. """
        if isinstance(self.file_content, dict):
            self.file_content.update(new_data)
        else:
            self.file_content = new_data
        return self.file_content
    
    def get_price(self, item_id: int) -> Tuple[int, int]:
        """ Get the buy and sell prices for item `item_id` """
        return self.get_value(item_id=item_id)
    
    @staticmethod
    def get_update_frequency() -> int:
        """ If an updated version of the snapshot can be downloaded from rbpi, update more frequently """
        return cfg.rt_rbpi_update_frequency if gp.f_rbpi_rt.exists() else cfg.rt_update_frequency
    
    def __getitem__(self, item: int) -> Tuple[int, int]:
        return self.get_price(item)


rt_prices_snapshot = RealtimePricesSnapshot()


class ItemWikiMapping(LocalFile, metaclass=SingletonMeta):
    """
    Class used for accessing and updating item Wiki mapping data.
    """
    def __init__(self):
        super().__init__(path=gp.local_file_wiki_mapping, update_frequency=86400)
        # print(f'Setting up ItemWikiMapping...')
        mt_dt, dtn = datetime.datetime.fromtimestamp(self.mtime()), datetime.datetime.now()
        
        # Lower update threshold on thursdays past noon, due to increased likelihood of new items
        if dtn.isoweekday() == 4 and dtn.hour > 15 and \
                (mt_dt.isoweekday() != 4 or
                 mt_dt.isoweekday() == 4 and mt_dt.hour <= 15 and (dtn-mt_dt).seconds > 3600):
            self.update(force_update=True)
    
    @override
    def get_value(self, update_check: bool = False, **kwargs) -> dict:
        """ Get the wiki mapping data for item `item_id`  """
        # Verify if the data is available, and check for update if applicable
        self.update()
        return self.file_content.get(kwargs.get('item_id'))
    
    @override
    def verify(self) -> bool:
        """ Return True if file_content is a dict and if one of its entries has length == 9 """
        if not isinstance(self.file_content, dict):
            print(f'Type verification for file {self.path} failed. File type={type(self.file_content)} (Expected=dict)')
            return False
        el = self.file_content.get(list(self.file_content.keys())[0])
        return isinstance(el, dict) and len(el) == 9
    
    def updated_content(self) -> dict:
        """ Download an item wiki mapping and return it """
        return dl.wiki_mapping()
    
    @override
    def merge_content(self, new_data) -> dict:
        """ Overwrite old entries with new entries, then return the updated dict. """
        if isinstance(self.file_content, dict) and isinstance(new_data, dict):
            self.file_content.update(new_data)
        elif isinstance(new_data, dict):
            self.file_content = new_data
        return self.file_content
    
    def get_item_mapping(self, item_id: int):
        """ Get wiki mapping for item_id=`item_id` """
        return self.get_value(item_id=item_id)


item_wiki_mapping = ItemWikiMapping()


class DataTransferFlag(FlagFile):
    """
    Flag to indicate whether data is being imported from the Raspberry Pi
    
    """
    def __init__(self):
        super().__init__(path=gp.flag_importing_data, lifespan=180)


flag_data_transfer = DataTransferFlag()


class NpyUpdaterFlag(FlagFile):
    """
    Flag to indicate whether Npy arrays are being updated
    
    """
    def __init__(self):
        super().__init__(path=gp.flag_npy_updater, lifespan=30)
        # print(f'Setting up NpyUpdaterFlag...')


flag_npy_update = NpyUpdaterFlag()


class TransactionParserFlag(FlagFile):
    """
    Flag to indicate whether new transactions are currently being parsed
    
    """
    def __init__(self):
        super().__init__(path=gp.flag_transaction_parser, lifespan=300)
        # print(f'Setting up TransactionParserFlag...')
        

flag_parsing_transactions = TransactionParserFlag()


if __name__ == "__main__":
    ...
