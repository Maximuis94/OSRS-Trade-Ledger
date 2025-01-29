"""
Module with implementation of Listbox interface

"""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Tuple, Optional

from gui.component.interface.filter import IFilter
from gui.component.interface.row import IRow
from gui.component.sort.sort import Sorts


# class ListboxMeta(ABCMeta):
#     """Metaclass for Listbox interface."""
#
#     def __instancecheck__(self, instance):
#         try:
#             return get_type_hints(instance.get_bgc)['return'] == Optional[str] and hasattr(instance, '__iter__')
#         except AttributeError:
#             return False


class IListbox(ABC):
    """
    Interface for the Listbox class.
    """

    @abstractmethod
    def insert(self, *rows: IRow, index: Optional[int] = None):
        """
        Insert a row at index=`index`

        Parameters
        ----------
        rows : IRow | Iterable[IRow]
            Rows to insert
        index : Optional[int]
            The position to insert the entry at. By default, add them to the very end.
        """
        raise NotImplementedError

    @abstractmethod
    def fill_listbox(self, rows: Iterable[IRow], filters: Optional[IFilter] = None, sorts: Optional[Sorts] = None,
                     extend: bool = True):
        """
        Fill the listbox with `rows` that will be filtered using `filters` and sorted using `sorts`
        
        Parameters
        ----------
        rows: Iterable[IRow]
            Row(s) to fill the listbox with.
        filters : Optional[IFilter]
            Filter(s) to apply
        sorts : Optional[Sorts]
            Sorts(s) to apply
        extend : bool, optional, True by default
            If True, extend the list of Rows the Listbox is currently filled with, rather than clearing them.
        """
        raise NotImplementedError
    
    @abstractmethod
    def refresh_listbox(self):
        """Clear the listbox, then fill it with the formatted elements from `_entries.subset`"""
        raise NotImplementedError

    @abstractmethod
    def clear_listbox(self, start: int = 0, end: Optional[int] = None):
        """
        Clear the listbox from indices `start` to `end`. By default, clear all rows.

        Parameters
        ----------
        start : int, optional
            Starting index, by default 0.
        end : Optional[int]
            Final index, optional, None by default
        """
        raise NotImplementedError

    @abstractmethod
    def get_bgc(self, row: IRow) -> Optional[str]:
        """
        Get the background color for a row

        Parameters
        ----------
        row : IRow
            The listbox row.

        Returns
        -------
        Optional[str]
            Background color in hexadecimal string format, or None.
        """
        raise NotImplementedError
    
    @abstractmethod
    def add(self, rows: Iterable[IRow], extend: bool = True):
        """
        Add `rows` to the list of rows, or make `rows` the list of rows.
        
        Parameters
        ----------
        rows : Iterable[IRow]
            Rows to add
        extend : bool, optional, True by default
            If True, extend the list of Rows the Listbox is currently filled with, rather than clearing.
        """
        raise NotImplementedError

    @abstractmethod
    def sort(self, sorts: Sorts) -> Tuple[IRow, ...]:
        """
        Sort the listbox rows by sequentially applying `sorts`

        Parameters
        ----------
        sorts : Sorts
            Sequence of Sorts to execute.

        Returns
        -------
        Tuple[IRow, ...]
            Tuple with sorted rows.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, *filters: IFilter) -> Tuple[IRow, ...]:
        """
        Apply `filters` to the listbox rows

        Parameters
        ----------
        filters : IFilter, optional
            Filters to apply. If none are passed, use the configured filters instead.

        Returns
        -------
        Tuple[IRow, ...]
            Filtered row subset
        """
        raise NotImplementedError
    
    @abstractmethod
    def configure(self,  filters: Optional[IFilter], sorts: Optional[Sorts]):
        """
        Set which filters and sorts should be applied.
        
        Parameters
        ----------
        filters : Optional[IFilter], None by default
            Filters to apply. This will override existing filters, if any.
        sorts : Optional[Sorts], None by default
            Sorts to apply. This will override existing sorts, if any.
        """
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, *args) -> IRow:
        """
        Get a particular row from the listbox, identified via the args provided

        Returns
        -------
        IRow
            The first row encountered that meets criteria
        """
        raise NotImplementedError
