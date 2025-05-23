"""
This module contains the model class of the LocalFile

"""
import datetime
import os
import shutil
import time
from collections.abc import Callable, Iterable
from dataclasses import field
from typing import Dict, Tuple, NamedTuple, Any

from .util import save, load, IOProtocol, _get_protocol


class Root(NamedTuple):
    """
    Class with fixed values that represent a root in the roots.json file.


    Attributes
    ----------
    key : str
        A unique key that can be used to identify a specific entry

    """
    key: str = field(compare=False)
    path: str = field(compare=True)
    var: str = field(compare=False)
    
    def __repr__(self):
        return self.path
    
    def exists(self) -> bool:
        """ Return True if the path associated with this Root exists """
        return os.path.exists(self.path)


class File(str):
    """
    Class for interacting with local files.
    Used for shorter notations and for configuring specific interactions to a path.
    """
    
    def __init__(self, path: str, allow_overwrite: bool = True, read_only: bool = False,
                 exception_handler_load: Callable = None, exception_handler_save: Callable = None,
                 verbose: bool = False, io_fail_freeze: float = 5.0, **kwargs):
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
        exception_handler_load : Callable, optional, None by default
            Method to invoke if an exception occurs while executing load(). Is called as;
            return exception_handler_load(e=e, file=self, **kwargs) where e is the Exception, self is this File and
            kwargs is a dict with additional keyword-args that was passed
        exception_handler_save : Callable, optional, None by default
            Method to invoke if an exception occurs while executing save(). Is called as;
            return exception_handler_save(e=e, data=data, file=self, **kwargs)
        verbose : bool, optional, False by default
            If True, print info on files being saved / loaded
        kwargs
            Any additional kwargs passed will be added to the default_args dict, which is passed to every method call
        """
        self.path: str = path
        # print(self.fsize(), self.path)
        self.protocol: IOProtocol or None = _get_protocol(path=path)
        self.folder, self.file = os.path.split(path)
        self.extension: str = os.path.splitext(path)[-1]
        self.allow_overwrite: bool = allow_overwrite and not read_only
        
        self.exception_handler_load: Callable = exception_handler_load
        self.exception_handler_save: Callable = exception_handler_save
        
        self.read_only = False
        self.save = self._save
        self.delete = self._delete
        
        self.default_args = kwargs
        self.verbose: bool = verbose
        self.io_fail_freeze = io_fail_freeze
        
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
        if self.protocol is None:
            print(f'Unable to save data to path {self.path} -- Its extension {self.extension} is not included as '
                  f'IOprotocol')
            time.sleep(self.io_fail_freeze)
            return False
        
        if self.verbose:
            print(data, f'\n\tSaving data of type {type(data)} file at {self.path}')
        kwargs = {k: self.default_args.get(k) if kwargs.get(k) is None else kwargs[k] for k in
                  frozenset(self.default_args).union(kwargs)}
        if self.exception_handler_save is None:
            # print(kwargs)
            save(data, self.path, overwrite=self.allow_overwrite, **kwargs)
            return self.exists()
        else:
            try:
                save(data, self.path, overwrite=self.allow_overwrite, **kwargs)
                return self.exists()
            except Exception as e:
                return self.exception_handler_save(e=e, file=self, **kwargs)
    
    def write_operation_read_only(self, *args, **kwargs):
        """ Override that is applied if the file is set to read-only """
        raise RuntimeError(f'Instance of File for {self.path} is configured as read-only. Disable read-only for this '
                           f'file to allow for writing operations such as delete/save.')
    
    def load(self, **kwargs):
        """ Load the file at File.path and return its contents. Invoke exception handlers if configured+necessary. """
        if self.protocol is None:
            print(f'Unable to save data to path {self.path} -- Its extension {self.extension} is not included as '
                  f'IOprotocol')
            time.sleep(self.io_fail_freeze)
            return None
        if self.verbose:
            print(f'Loading file at {self.path}')
        kwargs = {k: self.default_args.get(k) if kwargs.get(k) is None else kwargs[k] for k in
                  frozenset(self.default_args).union(kwargs)}
        if self.exception_handler_load is None:
            # print(kwargs)
            return load(self.path, **kwargs)
        else:
            try:
                return load(self.path, **kwargs)
            except Exception as e:
                return self.exception_handler_load(e=e, file=self, **kwargs)
            
    def _delete(self) -> bool:
        """ Delete the file at File.path. Return True if the file does not exist upon completion. """
        try:
            os.remove(self.path)
        finally:
            return self.exists()
    
    def exists(self) -> bool:
        """ Return True if a file exists at File.path """
        return os.path.exists(self.path)
    
    def mtime(self) -> float:
        """ Return the last modified timestamp of the file at File.path """
        return os.path.getmtime(self.path)
    
    def ctime(self) -> float:
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
    
    def fsize(self) -> int:
        """ Return the file size of the file at File.path as a formatted, abbreviated string """
        try:
            if os.path.getsize(self.path) is None:
                print(self.path)
                time.sleep(10)
            return os.path.getsize(self.path)
        except FileNotFoundError:
            return 0
    
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
    
    def split_ext(self) -> Tuple[str, str]:
        """ Return this File's path as a tuple of the file path without extension and its extension (including .) """
        return os.path.splitext(self.path)
    
    def has_ext(self, ext: str) -> bool:
        """ Return True if this file ends with extension `ext` """
        return self.extension.endswith(ext)
    
    def __repr__(self):
        return self.path


class IFile:
    file: File
    
    @property
    def path(self) -> str:
        """The full path to the file."""
        return self.file.path

    @property
    def protocol(self) -> IOProtocol | None:
        """The I/O protocol associated with the file."""
        return self.file.protocol

    @property
    def folder(self) -> str:
        """The folder containing the file."""
        return self.file.folder

    @property
    def extension(self) -> str:
        """The file extension."""
        return self.file.extension

    @property
    def allow_overwrite(self) -> bool:
        """Whether overwriting the file is allowed."""
        return self.file.allow_overwrite

    @property
    def exception_handler_load(self) -> Callable:
        """The exception handler for loading operations."""
        return self.file.exception_handler_load

    @property
    def exception_handler_save(self) -> Callable:
        """The exception handler for saving operations."""
        return self.file.exception_handler_save

    @property
    def read_only(self) -> bool:
        """Whether the file is read-only."""
        return self.file.read_only
    
    @read_only.setter
    def read_only(self, value: bool):
        self.file.read_only = value

    @property
    def save(self) -> Callable:
        """The method for saving the file."""
        return self.file.save

    @property
    def delete(self) -> Callable:
        """The method for deleting the file."""
        return self.file.delete

    @property
    def default_args(self) -> Dict[str, Any]:
        """The default arguments for file operations."""
        return self.file.default_args

    @property
    def verbose(self) -> bool:
        """Whether verbose output is enabled."""
        return self.file.verbose

    @property
    def io_fail_freeze(self) -> float:
        """The freeze duration after an I/O failure."""
        return self.file.io_fail_freeze
    
    def _save(self, data, **kwargs) -> bool:
        """Save `data` at `file.path` using pickle."""
        return self.file._save(data, **kwargs)
    
    def write_operation_read_only(self, *args, **kwargs):
        """Override that is applied if the file is set to read-only."""
        return self.file.write_operation_read_only(*args, **kwargs)
    
    def load(self, **kwargs):
        """Load the file at File.path and return its contents. Invoke exception handlers if configured+necessary."""
        return self.file.load(**kwargs)
    
    def _delete(self) -> bool:
        """Delete the file at File.path. Return True if the file does not exist upon completion."""
        return self.file._delete()
    
    def exists(self) -> bool:
        """Return True if a file exists at File.path."""
        return self.file.exists()
    
    def mtime(self) -> float:
        """Return the last modified timestamp of the file at File.path."""
        return self.file.mtime()
    
    def ctime(self) -> float:
        """Return the created timestamp of the file at File.path."""
        return self.file.ctime()
    
    def mdt(self) -> datetime.datetime:
        """Return the last modified timestamp of the file at File.path as datetime.datetime."""
        return self.file.mdt()
    
    def cdt(self) -> datetime.datetime:
        """Return the created timestamp of the file at File.path as datetime.datetime."""
        return self.file.cdt()
    
    def copy(self, to: str):
        """Copy the file at File.path to `to`."""
        return self.file.copy(to)
    
    def fsize(self) -> int:
        """Return the file size of the file at File.path as a formatted, abbreviated string."""
        return self.file.fsize()
    
    def toggle_read_only(self, read_only: bool = None):
        """Toggle read-only mode, or set it to a specific value if `read_only` is passed."""
        return self.file.toggle_read_only(read_only)
    
    def set_default_kwargs(self, add_args: Dict[str, any] = None, del_args: Iterable[str] = None):
        """Alter the default kw args configuration by adding or removing keyword args."""
        return self.file.set_default_kwargs(add_args, del_args)
    
    def split_ext(self) -> Tuple[str, str]:
        """Return this File's path as a tuple of the file path without extension and its extension (including .)."""
        return self.file.split_ext()
    
    def has_ext(self, ext: str) -> bool:
        """Return True if this file ends with extension `ext`."""
        return self.file.has_ext(ext)
