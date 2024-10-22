"""
Module for methods that involve files.
"""
import os.path
import pickle
from collections import namedtuple
from collections.abc import Container, Sequence, Callable, Iterable, Collection
from typing import List
from warnings import warn

import numpy as np
import pandas as pd


def load(path: str, file_not_found_handler: callable = None, eof_handler: callable = None):
    """
    Load the pickled file at `file_name` and return its contents.
    
    Parameters
    ----------
    path : str
        The path to the file that is to be loaded
    file_not_found_handler : callable, optional, None by default
        If passed, execute+return this instead of raising a FileNotFoundError, should this exception occur.
    eof_handler : Callable, optional, None by default
        If passed, execute+return this instead of raising an EOFError, should this exception occur.

    Returns
    -------
    any
        Contents of the loaded pickled file
    
    Raises
    ------
    FileNotFoundError
        Raised if no file could be found at path `file_name`
    EOFError
        Raised if pickle runs out of input, most likely suggests the file is corrupted.
    
    """
    try:
        return pickle.load(open(path, 'rb'))
    except FileNotFoundError as e:
        print(f"{e.__cause__}\n{e.__context__}")
        if file_not_found_handler is None:
            print(f"{e.__cause__}\n{e.__context__}")
            raise FileNotFoundError(f"Unable to load non-existent file at {path}")
        else:
            return file_not_found_handler(path=path, exception=e)
    except EOFError as e:
        if eof_handler is None:
            print(f"{e.__cause__}\n{e.__context__}")
            raise EOFError(f"File at {path} is probably corrupted...")
        else:
            return eof_handler(path=path, exception=e)
    except pickle.UnpicklingError as e:
        if 'STACK_GLOBAL' in str(e) and path[-3:] == 'npy':
            return np.load(path, allow_pickle=True)


def save_npy_batch(path: str, dfs: Iterable[pd.DataFrame], allow_overwrite: bool = False,
                   raise_overwrite_exception: bool = True) -> bool:
    """
    Save a set of dataframes, where each Dataframe is stored as its datatypes dict + its rows as numpy arrays.
    
    Parameters
    ----------
    path : str
        Path to the output file
    dfs : Iterable[pandas.DataFrame]
        An Iterable filled with pandas DataFrames that are to be saved.
    allow_overwrite : bool, optional, False by default
        If True, overwrite the file at `path` without notice if it already exists
    raise_overwrite_exception : bool, optional, True by default
        If True, raise a FileExistsError if `allow_overwrite` is False and there already is a file located at `path`

    Returns
    -------
    bool
        True if the file was saved, False if not.
    
    Raises
    ------
    FileExistsError
        File at `path` exists, while not allowed to overwrite
    """
    # assert batch_file.split('.')[-1] == 'npy' and os.path.exists(batch_file)
    if not os.path.exists(path) or allow_overwrite and os.path.exists(path):
        try:
            np.save(file=path, arr=np.array([(_df.dtypes, _df.to_numpy()) for _df in dfs], dtype=object),
                    allow_pickle=True)
            return True
        except AttributeError as e:
            # dfs is a single DataFrame; make a recursive call and pass it as its appropriate type
            if isinstance(dfs, pd.DataFrame):
                warn(UserWarning('In util.file.save_npy_batch(), dfs was passed as a pandas.DataFrame, while it is '\
                                 'expected to be an Iterable with one or more DataFrames'))
                return save_npy_batch(path=path, dfs=[dfs])
            raise e
    if raise_overwrite_exception:
        raise FileExistsError(f'File {path} already exists, while I am not allowed to overwrite it...')
    return False


def load_npy_batch(path: str) -> List[pd.DataFrame]:
    """ Load file at `path` and Iteratively construct the pandas DataFrames using the datatype dict + np arrays """
    assert path.split('.')[-1] == 'npy' and os.path.exists(path)
    return [pd.DataFrame(rows, columns=list(dtypes.keys())).astype(dtypes)
            for dtypes, rows in np.load(path, allow_pickle=True)]


def load_rbpi_npy_batch(path: str) -> List[pd.DataFrame]:
    """ Load file at `path` and Iteratively construct the pandas DataFrames using the datatype dict + np arrays """
    assert path.split('.')[-1] == 'npy' and os.path.exists(path)
    return [pd.DataFrame(rows, columns=list(dtypes.keys())).astype(dtypes)
            for dtypes, rows in zip(*np.load(path, allow_pickle=True))]


def save(data, path: str, overwrite: bool = True, exception_handler: callable = None) -> bool:
    """
    Save `data` at path `file_path`, using the pickle module.
    
    Parameters
    ----------
    data
        The python object that is to be saved
    path : str
        Path at which the file is to be saved
    overwrite : bool, optional, True by default
        Flag that dictates whether the file will be overwritten or not.
    exception_handler : callable, optional, None by default
        Exception handler in case some exception occurs. Should have **kwargs in its signature or the args specified
        below.

    Returns
    -------
    bool
        True if the file was successfully saved, False if not
    """
    try:
        if overwrite or not overwrite and not os.path.exists(path):
            pickle.dump(data, open(path, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
            return True
        else:
            return False
    except TypeError as e:
        if exception_handler is not None:
            return exception_handler(data=data, file_name=path, overwrite=overwrite, e=e)
        else:
            print(e.__cause__)
            print(e.__context__)
            raise WindowsError(f"Error while attempting to saving data of type {type(data)} at {path}")



def get_files(src: str, ext: (str or Container) = None, full_path: bool = True, add_folders: bool = False) -> List[str]:
    """
    Fetch the contents of `src_folder` and return the subset of files that meets the specified requirements as a list
    
    Parameters
    ----------
    src : str
        A string specifying the folder that is to be searched for files
    ext : str or collections.abc.Container, optional, None by default
        If specified, the returned file list is limited to files with the extension(s) specified
    full_path : bool, optional, True by default
        If True, return the absolute path rather than just the folder contents
    add_folders : bool, optional, False by default
        If True, return folders listed in `src_folders` as well

    Returns
    -------
    List[str]
        A list with all files found in `src_folder`
    
    Raises
    ------
    FileNotFoundError
        Raised if `src_folder` does not exist
    """
    if not os.path.isdir(src):
        raise FileNotFoundError(f'The specified src_folder {src} is not an existing directory')
    if isinstance(ext, str):
        ext = [ext]
    
    def include(el: str) -> bool:
        """ Return True if `el` meets requirements specified by args """
        return (ext is None or ext is not None and os.path.splitext(el)[1][1:] in ext) or \
               add_folders and os.path.isdir(el)
        
    return list([(src + f if full_path else f) for f in os.listdir(src) if include(src + f)])


def f_mt(path: str) -> float:
    return os.path.getmtime(path)


def f_ct(path: str) -> float:
    return os.path.getctime(path)
    

def file_size(path: str) -> (float, str):
    
    _size = os.path.getsize(path)
    if _size > pow(10, 10):
        return _size / pow(10, 9), 'gb'
    if _size > pow(10, 7):
        return _size / pow(10, 6), 'mb'
    if _size > pow(10, 4):
        return _size / pow(10, 3), 'kb'
    return _size, 'b'


# File is a named tuple of a path with some metric (e.g. last modified time)
_File = namedtuple('PathMetric', ['path', 'metric'])


def _compare_files(files: Collection, _compare: Callable, metric: Callable) -> str:
    """
    Compare each file property in `files` and return the file that evaluates the 'best' according to `_compare`.
    See get_newest_file() or get_oldest_file() for an example on how this method is to be used.
    
    Parameters
    ----------
    files : Sequence
        A Sequence of file paths that are to be compared
    _compare : Callable
        A method that accepts 2 args; the metric of file A and the path of file B and returns True if metric A is
        preferred over metric B
    metric : Callable
        Getter for the metric used to compare files with. It accepts a file path as arg and returns the metric.

    Returns
    -------
    str
        The path of the file of which the metric performs best when comparing it with other files using `_compare`
    
    See Also
    --------
    This method is rather generic, for a more specific implementation, see get_newest_file() or get_oldest_file()

    """
    output = None
    for path in files:
        try:
            if output is None:
                output = _File(path=path, metric=metric(path))
            elif _compare(output.metric, path):
                output = _File(path=path, metric=metric(path))
        except OSError:
            pass
    if output is None:
        raise OSError(f"Each path in `files` produced an OSError when attempting to call {metric} with the path as arg")
    return output.path


def get_newest_file(files: Collection, use_last_modified: bool = True) -> str:
    """ Given a Sequence of `files`, return the path of the file that was modified most recently """
    get_time = os.path.getmtime if use_last_modified else os.path.getctime
    return _compare_files(files=files, _compare=lambda m_a, p_b: m_a < get_time(p_b), metric=get_time)


def get_oldest_file(files: Collection, use_last_modified: bool = True) -> str:
    """ Given a Sequence of `files`, return the path of the file that was modified the longest time ago """
    get_time = os.path.getmtime if use_last_modified else os.path.getctime
    return _compare_files(files=files, _compare=lambda m_a, p_b: m_a > get_time(p_b), metric=get_time)


def get_timeseries_batch_path(item_id: int, batch_root: str, src) -> str:
    return batch_root + f'{src}/{item_id:0>5}.dat'


def verify_timeseries_batch_file(item_id: int, batch_root: str, srcs: Iterable):
    try:
        remove = False
        f = load(get_timeseries_batch_path(item_id, batch_root, srcs[-1]))[0]
        if isinstance(f, tuple) and len(f) == 3:
            return True
    except EOFError:
        remove = True
    except TypeError:
        remove = True
    except pickle.UnpicklingError:
        remove = True
    except FileNotFoundError:
        remove = True
    if remove:
        for src in srcs:
            try:
                os.remove(get_timeseries_batch_path(item_id, batch_root, src))
            except FileNotFoundError:
                ...


def backup_localdb(db_path, backup_dir, min_cooldown: int, max_backups: int):
    import time
    try:
        files = get_files(backup_dir)
        if len(files) == 0:
            backup_db = True
        else:
            backup_db = time.time() - os.path.getmtime(get_newest_file(files, use_last_modified=False)) > min_cooldown
    except ValueError:
        backup_db = True
    
    if backup_db:
        import shutil
        shutil.copy2(db_path, backup_dir + f'localdb_{int(time.time())}.db')
        
        # Max backups exceeded -> Remove oldest backup
        while len(get_files(backup_dir)) > max(3, max_backups):
            files = get_files(backup_dir)
            print(f'Removing backup {get_oldest_file(files)}...')
            os.remove(get_oldest_file(files))


if __name__ == '__main__':
    ...
    