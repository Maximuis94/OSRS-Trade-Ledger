"""
This module contains template classes for creating asynchronous tasks. These tasks can be used to execute code in a separate thread.
Specific realisations of async classes are listed in separate modules within the tasks folder

"""
import datetime
import threading

import path
# import ts_util


class AsyncTask(threading.Thread):
    def __init__(self, task: callable, callback_oncomplete: callable = None, **kwargs):
        threading.Thread.__init__(self, name=kwargs.get('name'), daemon=kwargs.get('daemon'))
        self.task = task
        self.on_complete = callback_oncomplete
        self.string = ''
    
    def run(self):
        self.string = self.task()
        if self.on_complete is not None:
            self.on_complete()
            print(self.string)


class AsyncDataTransfer(threading.Thread):
    def __init__(self, full_transfer: bool = True, update_arrays: bool = True, update_price_entries: bool = True,
                 archive_items: list = None, callback_oncomplete: callable = None):
        """
        Object for executing an asynchronous data transfer
        
        Parameters
        ----------
        full_transfer : bool, optional, True by default
            True if all steps should ben included in the data transfer (overrides other flags)
        update_arrays : bool, optional, True by default
            True if numpy arrays should be updated
        update_price_entries : bool, optional, True by default
            True if prices listbox entries should be updated
        archive_items : list, optional, None by default
            List of items to include in the update
        callback_oncomplete : callback, optional, None by default
            Code to execute after executing the queued methods
        """
        from global_values import npyar_items
        threading.Thread.__init__(self)
        self.full_transfer = full_transfer
        self.update_arrays = update_arrays and not full_transfer
        self.update_price_entries = update_price_entries and not full_transfer
        self.archive_items = npyar_items if archive_items is None else archive_items
        self.on_completion = callback_oncomplete
    
    def run(self):
        from data_transfer import rbpi_data_transfer, update_npy_database
        from data_preprocessing import update_listbox_entries
        if self.full_transfer:
            print('Transferring data from Raspberry Pi...')
            rbpi_data_transfer()
            print('   Done!')
        if self.update_arrays:
            print('Updating Numpy Arrays...')
            update_npy_database(item_id_list=self.archive_items)
            print('   Done!')
        if self.update_price_entries:
            print('Updating price listbox entries...')
            update_listbox_entries(n_days=14)
            print('   Done!')
        if self.on_completion is not None:
            self.on_completion()


class AsyncJSONParse(threading.Thread):
    def __init__(self, callback_oncomplete: callable = None, parse_exchange_log: bool = True, parse_json_files: bool = False):
        """ json and transaction parser to be executed on a separate thread. """
        threading.Thread.__init__(self)
        self.parse_elog, self.parse_jsons = parse_exchange_log, parse_json_files
        self.oncomplete_callback = callback_oncomplete
    
    def run(self):
        """ Parse json and exchange log transactions. Parsing both is very likely to result in duplicate subs """
        if self.parse_jsons and len([f for f in path.get_files(path.dir_downloads, extensions=['json'])
                                     if f[:14] == 'grand-exchange']) > 0:
            from runelite_reader import parse_json_thread_call
            parse_json_thread_call()
        elif self.parse_elog:
            from transaction_parser import parse_transaction_thread_call
            parse_transaction_thread_call()
        
        if self.oncomplete_callback is not None:
            self.oncomplete_callback()
        
        
if __name__ == '__main__':
    print([f for f in path.get_files(path.dir_downloads, extensions=['json']) if f[:14] == 'grand-exchange'])
    print(ts_util.dt_to_ts(datetime.datetime(2023, 1, 1)))

