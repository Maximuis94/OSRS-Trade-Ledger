"""
This module contains the model for a local file. Local files are pickled .dat files or csv files. They are typically
accessed and/or updated frequently.

The data is written to local files and accessed for usage throughout the project. This does not include files produced
solely as output that is readable by the user (e.g. some csv files) but it does include csv files produced to allow
the user to modify certain configs without altering the source code.

Existing local files that are used are listed as global variables in global_values.path.
"""
import os.path
import pickle
import time
from abc import abstractmethod
from collections.abc import Callable
from typing import List

import numpy as np
import pandas as pd

from import_parent_folder import recursive_import
import global_variables.path as gp
import util.file as uf
import util.unix_time as ut
from file.file import File
from global_variables.classes import SingletonMeta
del recursive_import

debug = True


# class File:
#     """
#     Class for interacting with local files.
#     Used for shorter notations and for configuring specific interactions to a path.
#     """
#
#     def __init__(self, path: str, allow_overwrite: bool = True, read_only: bool = True, eof_handler: Callable = None, file_not_found_handler: Callable = None):
#         self.path = path
#         self.folder = path[:len(path.split('/')[-1])]
#         self.file = path.split('/')[-1]
#         self.extension = path.split('.')[-1]
#         self.allow_overwrite = allow_overwrite and not read_only
#         self.eof_handler = eof_handler
#         self.file_not_found_handler = file_not_found_handler
#
#         self.read_only = False
#         self.save = self._save
#         self.delete = self._delete
#
#         if read_only:
#             self.toggle_read_only()
#
#     def _save(self, data, **kwargs) -> bool:
#         """
#         Save `data` at File.path using pickle.
#
#         Parameters
#         ----------
#         data :
#         verify_file :
#         kwargs :
#
#         Returns
#         -------
#
#         """
#         if not self.exists() or self.allow_overwrite and self.exists():
#             pickle.dump(data, open(self.path, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#             return self.exists()
#         else:
#             raise FileExistsError(f"Attempting to save file at {self.path} while overwriting it is not allowed...")
#         return os.path.exists(self.verify_path)
#
#     def write_operation_read_only(self, *args, **kwargs):
#         """ Override that is applied if the file is set to read-only """
#         raise RuntimeError(f'Instance of File for {self.path} is configured as read-only. Disable read-only for this '
#                            f'file to allow for writing operations such as delete/save.')
#
#     def load(self, **kwargs):
#         """ Load the file at File.path and return its contents. Invoke exception handlers if configured+necessary. """
#         return pickle.load(open(self.path, 'rb'))
#
#     def _delete(self) -> bool:
#         """ Delete the file at File.path. Return True if the file does not exist upon completion. """
#         try:
#             os.remove(self.path)
#         finally:
#             return self.exists()
#
#     def exists(self) -> bool:
#         """ Return True if a file exists at File.path """
#         return os.path.exists(self.path)
#
#     def mtime(self) -> float:
#         """ Return the last modified timestamp of the file at File.path """
#         return os.path.getmtime(self.path)
#
#     def ctime(self) -> float:
#         """ Return the created timestamp of the file at File.path"""
#         return os.path.getctime(self.path)
#
#     def mdt(self) -> datetime.datetime:
#         """ Return the last modified timestamp of the file at File.path as datetime.datetime """
#         return ut.loc_unix_dt(os.path.getmtime(self.path))
#
#     def cdt(self) -> datetime.datetime:
#         """ Return the created timestamp of the file at File.path as datetime.datetime """
#         return ut.loc_unix_dt(os.path.getctime(self.path))
#
#     def copy(self, to: str):
#         """ Copy the file at File.path to `to` """
#         shutil.copy2(self.path, to)
#
#     def size(self):
#         """ Return the file size of the file at File.path as a formatted, abbreviated string """
#         return os.path.getsize(self.path)
#
#     def toggle_read_only(self, read_only: bool = None):
#
#         self.read_only = not self.read_only if read_only is None else read_only
#         if not self.read_only:
#             self.save = self._save
#             self.delete = self._delete
#         else:
#             self.save = self.write_operation_read_only
#             self.delete = self.write_operation_read_only


class LocalFile(File):
    """
    Abstract class for local files. File interaction is handled through the baseclass. This includes updating the local
    file and loading the local file, among others. Specific implementations for fetching updated data, merging old data
    with new data, verifying data and accessing data should be implemented in its subclasses.
    
    Methods
    -------
    load()
        Create or update the file (if applicable) and return its contents.
    
    update(force_update: bool)
        Update the local file using the file_updater (if force_update=True), then update the file metadata.
        After updating data, the resulting data is verified with the passed data verifier. If a data
        verification method was passed and verification fails, a ValueError is raised and the attributes are set as if
        the file does not exist.
    
    get_value(update_check: bool, **kwargs)
        Get a specific value from the content saved in the file this class represents. Loading file content and
        updating it is integrated in this method.
    
    load_and_verify()
        Load the file at self.absolute_path and verify its content, if such an implementation was added in the subclass.
    
    file_updater()
        Method for preparing the data that is to be saved locally at self.absolute_path. The returned contents will be
        saved directly into this file. Note that if the data should be merged with an existing file, the file_updater()
        
    
    """
    
    def __init__(self, path: str, update_frequency: int, eof_handler: Callable = None):
        """
        Create a LocalFile object used as an interface when interacting with the specified file
        
        Parameters
        ----------
        path : str
            Absolute path used for locally saving this file
        update_frequency : int
            File update frequency in seconds. The file_updater method is called if this time since the last update is
            exceeded.
        """
        super().__init__(path=path, eof_handler=self.updated_content)

        # Load file_content when it is requested through LocalFile.load()
        self.file_content = None
        self.update_frequency = update_frequency
        if self.exists() and self.load_and_verify():
            self.next_update = self.mtime() + self.update_frequency
            self.update()
        else:
            print(f'Local file {self.path} does not exist, creating it...')
            self.next_update = 0
            self.update(force_update=True)
    
    def should_update(self) -> bool:
        """ Return True if the file is eligible for updating or verification of data fails. """
        return not self.exists() or \
            time.time() >= self.next_update
        
    def update(self, force_update: bool = False):
        """ Update the file content and verify it, then update the file meta-data attributes. """
        if force_update or self.should_update():
            # debug_printer(msg=f'Updating+saving file...')
            self.save(self.merge_content(self.updated_content()))
            
            # Data verification fails. Set meta-data as if the file does not exist and raise a ValueError.
            if not self.load_and_verify():
                raise ValueError(f"data verification for local file {self.path} failed after its contents were"
                                 f" updated.")
        
            self.next_update = self.mtime() + self.update_frequency
            return True
        return False

    def get_value(self, update_check: bool = False, **kwargs):
        """ Load/update the data (depending on input). Subclasses should implement the actual fetching of data. """
        if update_check:
            self.update(force_update=False)
    
    def verify(self) -> bool:
        """ Verify if file_content is not None. """
        return self.file_content is not None
    
    def load_and_verify(self) -> bool:
        """ Load the locally saved file and verify its content. """
        self.file_content = self.load()
        return self.verify()
    
    @abstractmethod
    def updated_content(self):
        """ Method used for fetching updated content for this LocalFile. """
        ...
    
    def merge_content(self, new_data):
        """ Method for merging new content with existing content. By default, the new content will simply overwrite. """
        return new_data


class FlagFile(File, metaclass=SingletonMeta):
    """
    A file for which its existence/last modified time suggests the underlying process is ongoing or not.
    Used to mitigate concurrency issues

    Methods
    -------
    FlagFile.is_active():
        Used to check whether the underlying process is ongoing or not
    
    FlagFile.save():
        Save file, should be used at start of process or during process to refresh flag
    
    FlagFile.remove():
        Remove file, should be used when the process is completed
    """
    
    def __init__(self, path: str, lifespan: int):
        super().__init__(path=path)
        self.lifespan = lifespan
        self.is_active()
    
    def is_active(self) -> bool:
        """ Return True if the process is marked as active, given the (non-)existing file """
        try:
            # lifespan exceeded -> remove the file as it is no longer active + return False
            if time.time() - self.mtime() > self.lifespan:
                self.delete()
                return False
            return True
        except FileNotFoundError:
            return False
    
    def save(self, data=None, verify_file: bool = False, **kwargs) -> bool:
        """
        Save the flag file. By default, only overwrite it if the flag is expired.

        Other Parameters
        ----------
        extend_lifespan : any, optional, None by default
            If passed, overwrite the file regardless of its active status, thus extending its lifespan

        Returns
        -------
        True if the file was saved, False if not

        """
        if kwargs.get('extend_lifespan') is not None or not self.is_active():
            super().save(data=data, path=self.path, verify_file=False)
            return True
        return False


class NpyBatch(File):
    data_types: List[dict]
    rows: List[np.ndarray]
    
    def __init__(self, path: str = None, data_types: list = None, rows=None, **kwargs):
        super().__init__(path=path, **kwargs)
        
        if path is not None and self.exists():
            try:
                self.load_batch()
            except ValueError:
                self.load_batch(rbpi_batch=True)
        
        if data_types is not None and rows is not None:
            self.data_types = data_types
            self.rows = rows
    
    
    def load_batch(self, **kwargs) :
        if kwargs.get('rbpi_batch') is None:
            rbpi_batch = self.path[:2] == '//'
        else:
            rbpi_batch = kwargs.get('rbpi_batch')

        self.data_types, self.rows = [], []
        try:
            self._load_rbpi_batch() if rbpi_batch else self._load_batch()
        except pickle.UnpicklingError as e:
            if 'STACK_GLOBAL' not in str(e):
                raise e
            else:
                return self._load_npy()
    
    def save(self, data=None, verify_file: bool = False, **kwargs) -> bool:
        if kwargs.get('path') is not None:
            self.path = kwargs.get('path')
        # np.save(file=self.path, ar=[(_dt, _ar) for _dt, _ar in zip(self.data_types, self.rows)], allow_pickle=True)
        return super().save(data=[(_dt, _ar) for _dt, _ar in zip(self.data_types, self.rows)])
    
    def to_df(self, index: int = None):
        if index is None:
            return [pd.DataFrame(data=_r, columns=list(_dt.keys())).astype(_dt)
                    for _dt, _r in zip(self.data_types, self.rows)]
        else:
            return pd.DataFrame(data=self.rows[index],
                columns=list(self.data_types[index].keys())).astype(self.data_types[index])
        
    def _load_batch(self):
        for _datatypes, _row in super().load():
            self.data_types.append(dict(_datatypes))
            self.rows.append(_row)
        return True
    
    def _load_rbpi_batch(self):
        for _datatypes, _row in zip(*super().load()):
            self.data_types.append(dict(_datatypes))
            self.rows.append(_row)
        
        return True
    
    def _load_npy(self):
        self.data_types, self.rows = [], []
        for _datatypes, _row in zip(*np.load(file=self.path, allow_pickle=True)):
            self.data_types.append(dict(_datatypes))
            self.rows.append(_row)
        return True

        
    
    @staticmethod
    def convert_rbpi_batch(path, out_file: str = None):
        batch = NpyBatch(path)
        for _dt in batch.data_types:
            if _dt.get('id') is None:
                raise ValueError(f'NpyBatch at {path} is not a rbpi batch')
        
        dfs = []
        times = (batch.mtime(), batch.mtime())
        
        if out_file is None:
            out_file = batch.path
            
        for df in batch.to_df():
            del df['id']
            if 'is_sale' in df.columns:
                df['is_buy'] = df['is_sale'].apply(lambda v: abs(v - 1))
                del df['is_sale']
            dfs.append(df)
        
        uf.save(data=[(_df.dtypes, _df.to_numpy()) for _df in dfs],
                path=out_file)
        os.utime(out_file, times=times)
        return dfs
        # np.save(file=batch.path if out_file is None else out_file,
        #         arr=[(_df.dtypes, _df.to_numpy()) for _df in dfs],
        #         allow_pickle=True)
        
    
if __name__ == '__main__':
    # file = File(path=, read_only=False)
    print(uf.load(gp.dir_rbpi_dat+'batch_001.npy'))#, allow_pickle=True))
    ut.runtime(File, gp.dir_data+'test.dat', _n_trials=10)
    
    # file.save(time.time())
    ...
            