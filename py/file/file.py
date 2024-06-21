"""
This module contains the model class of the LocalFile

"""
import datetime
import os
import pickle
import shutil
from collections.abc import Callable


class File:
    """
    Class for interacting with local files.
    Used for shorter notations and for configuring specific interactions to a path.
    """
    
    def __init__(self, path: str, allow_overwrite: bool = True, read_only: bool = False, eof_handler: Callable = None,
                 file_not_found_handler: Callable = None):
        self.folder, self.file = os.path.split(path)
        self.path = os.path.abspath(path)
        self.extension = os.path.splitext(self.file)[-1][1:]
        self.allow_overwrite = allow_overwrite and not read_only
        self.eof_handler = eof_handler
        self.file_not_found_handler = file_not_found_handler
        
        self.read_only = False
        self.save = self._save
        self.delete = self._delete
        
        if read_only:
            self.toggle_read_only()
    
    def _save(self, data, **kwargs) -> bool:
        """
        Save `data` at File.path using pickle.

        Parameters
        ----------
        data :
        verify_file :
        kwargs :

        Returns
        -------

        """
        if not self.exists() or self.allow_overwrite and self.exists():
            pickle.dump(data, open(self.path, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
            return self.exists()
        else:
            raise FileExistsError(f"Attempting to save file at {self.path} while overwriting it is not allowed...")
        return os.path.exists(self.verify_path)
    
    def write_operation_read_only(self, *args, **kwargs):
        """ Override that is applied if the file is set to read-only """
        raise RuntimeError(f'Instance of File for {self.path} is configured as read-only. Disable read-only for this '
                           f'file to allow for writing operations such as delete/save.')
    
    def load(self, **kwargs):
        """ Load the file at File.path and return its contents. Invoke exception handlers if configured+necessary. """
        return pickle.load(open(self.path, 'rb'))
    
    def _delete(self) -> bool:
        """ Delete the file at File.path. Return True if the file does not exist upon completion. """
        try:
            os.remove(self.path)
        finally:
            return self.exists()
    
    def exists(self) -> bool:
        """ Return True if a file exists at File.path """
        return os.path.exists(self.path)
    
    def mt(self) -> float:
        """ Return the last modified timestamp of the file at File.path """
        return os.path.getmtime(self.path)
    
    def ct(self) -> float:
        """ Return the created timestamp of the file at File.path"""
        return os.path.getctime(self.path)
    
    def mdt(self) -> datetime.datetime:
        """ Return the last modified timestamp of the file at File.path as datetime.datetime """
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.path))
    
    def cdt(self) -> datetime.datetime:
        """ Return the created timestamp of the file at File.path as datetime.datetime """
        return datetime.datetime.fromtimestamp(os.path.getctime(self.path))
    
    def copy(self, to: str):
        """ Copy the file at File.path to `to` """
        shutil.copy2(self.path, to)
    
    def size(self):
        """ Return the file size of the file at File.path as a formatted, abbreviated string """
        return os.path.getsize(self.path)
    
    def toggle_read_only(self, read_only: bool = None):
        """ Toggle read-only mode, or set it to a specific value if `read_only` is passed. """
        self.read_only = not self.read_only if read_only is None else read_only
        if not self.read_only:
            self.save = self._save
            self.delete = self._delete
        else:
            self.save = self.write_operation_read_only
            self.delete = self.write_operation_read_only
    