"""
This module contains various implementations with json files like serializers and deserializers.

Recommended usage is via File or via invoking save() or load().

References
----------
https://docs.python.org/3/library/json.html
    The official json module docs are listed in the page above
    
Notes
-----
Converting back and forth may not necessarily result in the same object as was initially given. JSON files use a string
as key, which means all keys are converted to strings before outputting the file while encoding. Decoding it will
produce strings as well.
"""
import json
import os
import pickle
from collections import namedtuple
from enum import Enum
from typing import Callable, List, Dict

import numpy as np
import pandas as pd

basic_keys = int, float, str, bool, None


def set_ext(path: str, ext: str) -> str:
    """ Set `ext` as extension of `path` and return it """
    if not ext.startswith('.'):
        ext = f'.{ext}'
    return path if path.endswith(ext) else os.path.splitext(path)[0]+ext


def to_py(value: str):
    """ Cast `value` to a builtin python type (int, float, or bool), if eligible. """
    if value.count('.') == 1 and False not in [v.isnumeric() for v in value.split('.')]:
        return float(value)
    elif value.isnumeric():
        return int(value)
    elif value.lower() in ('true', 'false'):
        return bool(value)
    else:
        return value


def _save_json(data, path: str, **kwargs):
    """
    Saves `data` at `path`
    
    Parameters
    ----------
    data
        The data that is to be saved
    path : str
        Path to save the data at.
    
    Other Parameters
    ----------------
    indent : int, optional, 4 by default
        Level of indentation to use
    default : Callable, optional, None by default
        If specified, default should be a function that gets called for objects that can’t otherwise be serialized.
    cls : Type[JSONEncoder], optional, None by default
        A custom JSON encoder can be specified here
    skip_keys : bool, optional, False by default
        If True, dict entries with keys that are not of a basic type will be skipped
    ensure_ascii : bool, optional, True by default
        If True, escape non-ascii keys from the output file
    allow_nan : bool, optional, True by default
        If False, attempting to serialize values like inf and nan will raise a ValueError
    sort_keys : bool, optional, False by default
        If True, sort the keys before outputting them.
    separators : Tuple[str, str], optional, (',', ': ') by default
        The strings to use to separate items and keys/entries from each other
    check_circular : bool, optional, True by default
        If True, check for circular references at container types

    """
    # Default keyword args
    _kwargs = {
        'indent': 4,
        'skipkeys': False,
        'ensure_ascii': True,
        'allow_nan': True,
        'sort_keys': False,
        'separators': (',', ': '),
        'check_circular': True,
        'cls': None,
        'default': None
    }
    json.dump(data, open(os.path.splitext(path)[0]+'.json', 'w'),
              **{k: kwargs[k] if kwargs.get(k) is not None else v for k, v in _kwargs.items()})
    

def _load_json(path: str, convert_keys: bool = True, **kwargs):
    """
    Load data from the json file at `path`
    
    Parameters
    ----------
    path : str
        Path to save the data at.
    convert_keys : bool, optional, True by default
        If True and the data is parsed as a dict, cast the keys to a basic python type
    
    Other Parameters
    ----------------
    cls : Type[JSONEncoder], optional, None by default
        A custom JSON encoder can be specified here
    object_hook : Callable, optional, None by default
        Method to pass the parsed data to so it can be generated as an object instead.
    object_pairs_hook : Callable, optional, None by default
        An optional function that will be called with the result of any object literal decoded with an ordered list of
        pairs. The return value of object_pairs_hook will be used instead of the dict. This feature can be used to
        implement custom decoders. If object_hook is also defined, the object_pairs_hook takes priority.
    parse_int : Callable, optional, None by default
        Method to call for parsing integers
    parse_float : Callable, optional, None by default
        Method to call for parsing floating point values
    parse_constant : Callable, optional, None by default
        If specified, will be called with one of the following strings: '-Infinity', 'Infinity', 'NaN'.
        This can be used to raise an exception if invalid JSON numbers are encountered.

    """
    keys = ('cls', 'object_hook', 'object_pairs_hook', 'parse_int', 'parse_float', 'parse_constant')
    if convert_keys:
        output = json.load(open(path, 'r'), **{k: kwargs.get(k) for k in keys if kwargs.get(k) is not None})
        return {to_py(k): v for k, v in output.items()} if isinstance(output, dict) else output
    
    else:
        return json.load(open(path, 'r'), **{k: kwargs.get(k) for k in keys if kwargs.get(k) is not None})


def _save_pickle(data, path: str, protocol: int = pickle.HIGHEST_PROTOCOL, **kwargs):
    """
    Save `data` at path `path`, using the pickle module.
    
    Parameters
    ----------
    data
        The python object that is to be saved
    path : str
        Path at which the file is to be saved
    protocol : int, optional, pickle.HIGHEST_PROTOCOL by default
        Protocol to use for pickling objects
    
    Other Parameters
    ----------------
    buffer_callback : Callback, optional, None by default
        Callback that will be invoked with a buffer view (see official docs)
    fix_imports : bool, optional, True by default
    

    See Also
    --------
    https://docs.python.org/3.10/library/pickle.html#pickle.Pickler
    """
    keys = ('buffer_callback', 'fix_imports')
    pickle.dump(data, open(path, 'wb'), protocol=protocol,
                **{k: kwargs.get(k) for k in keys if kwargs.get(k) is not None})


def _load_pickle(path: str, **kwargs):
    """
    Load the file at `path` and return its contents. First, attempt to use pickle, specific errors that tend to occur
    when attempting to unpickle non-pickled objects will trigger a specific action for using an alternative method for
    loading.

    Parameters
    ----------
    path : str
        The path to the file that is to be loaded
    
    Other Parameters
    ----------------
    buffers : Iterable, optional, None by default
        If not specified, aall data necessary for deserialization must be contained in the pickle stream. If specified,
        it should be an Iterable of buffer-enabled objects. Whether or not to pass this depends on buffer_callback arg
        that was given during serialization.
    fix_imports : bool, optional, True by default
        If True, try to map old python2 names to new names used in Python3
    encoding : str, optional, 'ASCII' by default
        Encoding protocol to apply for old python2 files
    errors : str, optional, 'strict' by default
        Error handler protocol to apply for old python2 files
    
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
    keys = ('buffers', 'fix_imports', 'encoding', 'errors')
    return pickle.load(open(path, 'rb'), **{k: kwargs.get(k) for k in keys if kwargs.get(k) is not None})


def _save_npy(data: np.ndarray, path: str, **kwargs):
    """
    Save `data` at path `path` using numpy.save
    
    Parameters
    ----------
    data : np.ndarray
        The numpy array that is to be saved
    path : str
        The location `data` should be saved at
    
    Other Parameters
    ----------------
    allow_pickle : bool, optional, False by default
        If True, allow the data to be pickled
    fix_imports : bool, optional, True by default
        If True, pickle will use names from python 2 for backwards compatibility
    
    References
    ----------
    https://numpy.org/doc/stable/reference/routines.io.html
    """
    _kwargs = {'fix_imports': True, 'allow_pickle': True}
    _kwargs.update({kwargs.get(k) for k in frozenset(_kwargs).intersection(kwargs)})
    np.save(path, data, **_kwargs)


def _load_npy(path: str, **kwargs):
    """
    Load the file at `path` with numpy.load
    
    Parameters
    ----------
    path : str
        Location of the file that is to be loaded
    
    Other Parameters
    ----------------
    mmap_mode : str, optional,
        memory_map mode to use
    allow_pickle : bool, optional, False by default
        If True, allow the file to be unpickled
    fix_imports : bool, optional, True by default
        If True, pickle will use names from python 2 for backwards compatibility
    encoding : str, optional, 'ASCII' by default
        Encoding to use when reading python2 strings.
        
    Returns
    -------
    any
        The loaded data
    
    References
    ----------
    https://numpy.org/doc/stable/reference/routines.io.html

    """
    # keys = ('mmap_mode', 'allow_pickle', 'fix_imports', 'encoding')
    _kwargs = {'fix_imports': True, 'allow_pickle': True}
    _kwargs.update({kwargs.get(k) for k in frozenset(_kwargs).intersection(kwargs)})
    return np.load(path, **_kwargs)


def _save_log(data: List[str], path: str, append: bool = False, **kwargs):
    """
    Save textual `data` at path `path` using builtin methods
    
    Parameters
    ----------
    data : np.ndarray
        The numpy array that is to be saved
    path : str
        The location `data` should be saved at
    append : bool, optional, False by default
        If True, append the file instead of replacing the existing file
    
    Other Parameters
    ----------------
    ...
    """
    with open(path, ('a' if append else 'w')) as log_file:
        for line in data:
            log_file.write(line + ('' if line.endswith('\n') else '\n'))


def _load_log(path: str, min_line_length: int = None, **kwargs) -> List[str]:
    """
    Load the log file at `path` and return it as a list of strings. The newline character at the end of a line is not
    included in the output.
    
    Parameters
    ----------
    path : str
        Location of the file that is to be loaded
    min_line_length : int, optional, None by default
        If passed, line should be at least this many characters long in order to be included in the output.
    
    Other Parameters
    ----------------
    
    Returns
    -------
    List[str]
        A list of individual parsed lines

    """
    output = []
    with open(path, 'r') as log_file:
        for line in log_file.readlines():
            if min_line_length is None or len(line) >= min_line_length:
                output.append(line.rstrip('\n'))
    return output


def _save_csv(data: List[Dict[str, any]], path: str, **kwargs):
    """
    Save textual `data` at path `path` using builtin methods
    
    Parameters
    ----------
    data : np.ndarray
        The numpy array that is to be saved
    path : str
        The location `data` should be saved at
    
    Other Parameters
    ----------------
    ...
    """
    ...


def _load_csv(path: str, **kwargs) -> List[Dict[str, any]]:
    """
    Load the log file at `path` and return it as a list of strings. The newline character at the end of a line is not
    included in the output.
    
    Parameters
    ----------
    path : str
        Location of the file that is to be loaded
    cell_parser : Callable, optional, None by default
        If passed, use this method to format each cell
    
    Other Parameters
    ----------------
    
    Returns
    -------
    List[Dict[str, any]
        A list of individual parsed lines

    """
    return pd.DataFrame(path).to_dict('records')


_io = namedtuple('IO', ['load', 'save', 'extension'])


class IOProtocol(Enum):
    """
    Enumeration of modules that can be used to import or export data. The load and save methods are stored as a
    namedtuple in the enumeration.
    The enumerated names are module names in caps.
    """
    PICKLE = _io(load=_load_pickle, save=_save_pickle, extension='.dat')
    JSON = _io(load=_load_json, save=_save_json, extension='.json')
    NPY = _io(load=_load_npy, save=_save_npy, extension='.npy')
    LOG = _io(load=_load_log, save=_save_log, extension='.log')
    CSV = _io(load=_load_csv, save=_save_csv, extension='.csv')


def _get_protocol(data=None, path: str = '') -> IOProtocol or None:
    """ Determine the most suitable protocol for importing/exporting `data` to `path` and return it """
    if path.endswith('.json'):
        return IOProtocol.JSON
    
    if path.endswith('.dat'):
        return IOProtocol.PICKLE
    
    if isinstance(data, np.ndarray) or path.endswith('.npy'):
        return IOProtocol.NPY
    
    if path.endswith('.csv'):
        return IOProtocol.CSV
    
    if path.endswith('.log'):
        return IOProtocol.LOG
    
    if path.endswith('.txt'):
        return IOProtocol.LOG
    
    # raise ValueError(f'Unable to deduce a IOProtocol from path {path}')
    return None


def save(data, path, force_extension: bool = True, overwrite: bool = True, protocol: IOProtocol = None,
         exception_handler: Callable = None, **kwargs):
    """
    Invoke the save method of `protocol`, passing `data` as to be saved object and `path` as path.
    
    Parameters
    ----------
    data : any
        The data that is to be saved
    path : str
        Path where the file should be saved
    overwrite : bool, optional, True by default
        Flag that dictates whether the file will be overwritten or not, should it exist.
    protocol : IOProtocol, optional, None by default
        Module to use to export `data`. If not specified, determine the IOProtocol via the extension of `path`.
    force_extension : bool, optional, True by default
        If True, set the extension of `path` to the Protocol extension.
    exception_handler : Callable, optional, None by default
        Method to invoke if an Exception is raised. This method will be called with 3 keyword arguments as;
        'return exception_handler(e=e, data=data, path=path)'
    
    Notes
    -----
    For the sake of consistency, extensions affiliated with specific IOProtocols should be used.
    
    """
    protocol = _get_protocol(data, path).value if protocol is None else protocol.value
    # print(f'Saving {data} with protocol {protocol}')

    try:
        if overwrite or not overwrite and not os.path.exists(path):
            return protocol.save(data, set_ext(path, protocol.extension) if force_extension else path, **kwargs)
        raise FileExistsError(f"Unable to save file at path {path} as it already exists and overwrite=False")
    except Exception as e:
        if exception_handler is None:
            raise e
        return exception_handler(e=e, path=path)


def load(path, force_extension: bool = True, protocol: IOProtocol = None, exception_handler: Callable = None, **kwargs):
    """
    Invoke the load method of `protocol`, passing `data` as to be saved object and `path` as path
    
    Parameters
    ----------
    path : str
        Path where the file should be saved
    force_extension : bool, optional, True by default
        If True, set the extension of `path` to the Protocol extension.
    protocol : IOProtocol, optional, None by default
        Module to use to load data at `path`. If not specified, determine the protocol using the extension of `path`.
    exception_handler : Callable, optional, None by default
        Method to invoke if an Exception is raised. This method will be called with 3 keyword arguments as;
        return exception_handler(e=e, data=data, path=path)

    Returns
    -------

    """
    protocol = _get_protocol(None, path).value if protocol is None else protocol.value
    # print(f'Loading file from {path} with protocol {protocol}')
    
    try:
        return protocol.load(set_ext(path, protocol.extension) if force_extension else path, **kwargs)
    except Exception as e:
        if exception_handler is None:
            raise e
        else:
            return exception_handler(e=e, path=path)
