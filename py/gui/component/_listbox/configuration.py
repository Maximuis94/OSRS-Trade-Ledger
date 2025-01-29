"""Module with the class representation of an applicable configuration"""
from collections.abc import Sequence, Callable
from enum import Enum
from typing import NamedTuple

from gui.component.interface.row import IRow


class ConfigurationType(Enum):
    SORT = 0
    FILTER = 1
    COLOR_SCHEME = 2


class Configuration(NamedTuple):
    """A specific configuration"""
    description: str
    """Concise description of the configuration. Can be used by the user as reference"""
    
    type: ConfigurationType
    """Type of configuration; i.e. sort/filter/color scheme"""
    
    function: Callable[[Sequence[IRow]], Sequence[IRow]]
    """Function that applies this configuration to a set of rows"""

class ListboxConfiguration:
    """
    Class that captures a set of configurations that can be applied to a particular Listbox.
    """
    
    def __init__(self, configurations: Sequence[Configuration]):
        for c in configurations:
            