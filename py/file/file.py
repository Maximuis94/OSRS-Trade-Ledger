"""
This module contains the model class of the LocalFile

"""
import datetime
import os
import shutil
from collections.abc import Callable, Iterable
from typing import Dict

from util import save, load


class File:
    """
    Class for interacting with local files.
    Used for shorter notations and for configuring specific interactions to a path.
    """
    
    def __init__(self, path: str, allow_overwrite: bool = True, read_only: bool = False, eof_handler: Callable = None,
                 file_not_found_handler: Callable = None, verbose: bool = False, **kwargs):
        """
        Object used to interact with the file located at its path.
        
        Parameters
        ----------
        path : str
            Path of this File
        allow_overwrite : bool, optional, True by default
            If False, raise a FileExistsError when attempting to overwrite the file instead
        read_only : bool, optional, False by default
            If True, raise a RuntimeError when attempting to invoke a save method instead
        eof_handler : Callable, optional, None by default
            If passed, invoke this method if an EOFError is raised
        file_not_found_handler : Callable, optional, None by default
            If passed, invoke this method if a FileNotFoundError is raised
        verbose : bool, optional, False by default
            If True, print info on files being saved / loaded
        kwargs
            Any additional kwargs passed will be added to the default_args dict, which is passed to every method call
        """
        self.folder, self.file = os.path.split(path)
        self.path = os.path.abspath(path)
        self.extension = os.path.splitext(self.file)[-1][1:]
        self.allow_overwrite = allow_overwrite and not read_only
        self.eof_handler = eof_handler
        self.file_not_found_handler = file_not_found_handler
        
        self.read_only = False
        self.save = self._save
        self.delete = self._delete
        
        self.default_args = kwargs
        self.verbose: bool = False
        
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
        if self.verbose:
            print(data, f'\n\tSaving data of type {type(data)} file at {self.path}')
        save(data, self.path, overwrite=self.allow_overwrite,
             **{k: self.default_args.get(k) if kwargs.get(k) is None else kwargs[k] for k in frozenset(self.default_args).union(kwargs)})
        return os.path.exists(self.path)
    
    def write_operation_read_only(self, *args, **kwargs):
        """ Override that is applied if the file is set to read-only """
        raise RuntimeError(f'Instance of File for {self.path} is configured as read-only. Disable read-only for this '
                           f'file to allow for writing operations such as delete/save.')
    
    def load(self, **kwargs):
        """ Load the file at File.path and return its contents. Invoke exception handlers if configured+necessary. """
        if self.verbose:
            print(f'Loading file at {self.path}')
        return load(self.path, **{k: self.default_args.get(k) if kwargs.get(k) is None else kwargs[k] for k in frozenset(self.default_args).union(kwargs)})
    
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
    
    def set_default_kwargs(self, add_args: Dict[str, any] = None, del_args: Iterable[str] = None):
        """
        Alter the default kw args configuration by adding or removing keyword args. This kwargs dict is passed as
        **kwargs whenever save() or load() is called.
        
        Parameters
        ----------
        add_args : Dict[str, any], optional, None by default
            Dict with args that are to be added
        del_args : Iterable[str], optional, None by default
            Iterable with keywords that are to be removed from the dict
        """
        if add_args is not None:
            self.default_args.update(add_args)
            
        if del_args is not None:
            for k in frozenset(self.default_args).intersection(del_args):
                del self.default_args[k]
    