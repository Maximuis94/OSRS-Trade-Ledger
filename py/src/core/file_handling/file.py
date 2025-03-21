import os
from typing import Callable

class File(str):
    """
    Class for interacting with local files.
    Used for shorter notations and for configuring specific interactions to a path.
    """
    
    def __init__(self, path: str, allow_overwrite: bool = True, read_only: bool = False,
                 exception_handler_load: Callable = None, exception_handler_save: Callable = None,
                 verbose: bool = False, io_fail_freeze: float = 5.0, **kwargs):
        self.path: str = path
        self.protocol: IOProtocol or None = _get_protocol(path=path)
        self.folder, self.file = os.path.split(path)
        # ... rest of existing implementation ... 