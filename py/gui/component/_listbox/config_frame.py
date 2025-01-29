"""
Module with a ConfigFrame implementation.
The ConfigFrame is a Frame that allows the user to customize Listbox properties.
The Listbox can be customized on various ways;
- DataStructure, which is a set rows.
- Sort order, which dictates the order in which the rows are displayed
    => Iterable[Tuple[ColumnName, SortAsc]]
- Filters, which dictate whether or not to omit specific values

Customizing;
- fast: set of pre-defined configs one can easily switch between (e.g. set of radiobuttons)
- custom: More detailed configuration, which as a consequence takes longer to define

"""
from collections.abc import Sequence
from dataclasses import dataclass

from gui.objects import ListboxColumn


@dataclass(slots=True, match_args=True, )
class ListboxConfiguration:
    """
    Configurations Frame for the Listbox. It can be filled with options to filter, sort and
    color rows. Additionally, it may also provide a small explanation per option.
    """
    
    columns: Sequence[ListboxColumn]
    
    
    