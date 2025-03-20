"""
Module: sort.py
===============
This module provides classes for sorting ListboxEntries in a streamlined fashion.
It defines two primary classes:

    - Sort: Represents a single sort criterion, which can be applied to a collection of entries.
    - SortSequence: Represents a sequence of Sort instances that can be applied sequentially.

These classes conform to the ISortBy and ISortSequence interfaces.
"""
from collections.abc import Iterable
from dataclasses import dataclass
from typing import List, Optional, Union

from gui.component.interface.sort import ISortBy, ISortSequence
from gui.util.generic import SupportsGetItem


@dataclass(slots=True, order=True)
class Sort(ISortBy):
    """
    Represents a single sort criterion for sorting ListboxEntries.

    Attributes
    ----------
    column : str
        The identifier of the ListboxColumn to sort by.
    reverse : bool, optional
        Whether to reverse the sort order (default is False).

    Methods
    -------
    __call__(to_sort)
        Apply this sort criterion to an iterable of entries and return the sorted list.
    flip()
        Flip the sort order (i.e. reverse the value of `reverse`).
    apply_sorts(entries)
        Alias for __call__, applying this sort to a collection of entries.
    __iter__()
        Yield self to allow iterating over a single Sort as if it were a sequence.
    """

    column: str
    reverse: bool = False

    def __iter__(self):
        """
        Yield self, allowing a single Sort instance to be treated as an iterable.

        Yields
        ------
        Sort
            This sort instance.
        """
        yield self

    def __call__(self, to_sort: Iterable[SupportsGetItem]) -> List[SupportsGetItem]:
        """
        Apply this sort criterion to the provided entries.

        Parameters
        ----------
        to_sort : Iterable[SupportsGetItem]
            An iterable of entries supporting item access.

        Returns
        -------
        List[SupportsGetItem]
            A new list containing the sorted entries.

        Examples
        --------
        >>> sort_crit = Sort("age")
        >>> sorted_entries = sort_crit(entries)
        """
        return list(sorted(to_sort, key=lambda e: e._values[self.column], reverse=self.reverse))

    def __eq__(self, other: object) -> bool:
        """
        Compare this sort with another for equality.

        Parameters
        ----------
        other : object
            Another sort instance to compare against.

        Returns
        -------
        bool
            True if both have the same column and reverse flag, False otherwise.
        """
        if not isinstance(other, Sort):
            return False
        return self.column == other.column and self.reverse == other.reverse

    def flip(self) -> None:
        """
        Flip the sort order by negating the reverse flag.

        Examples
        --------
        >>> sort_crit = Sort("age", reverse=False)
        >>> sort_crit.flip()
        >>> sort_crit.reverse
        True
        """
        self.reverse = not self.reverse

    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> List[SupportsGetItem]:
        """
        Apply this sort to a collection of entries.

        Parameters
        ----------
        entries : Iterable[SupportsGetItem]
            An iterable of entries.

        Returns
        -------
        List[SupportsGetItem]
            The sorted entries.
        """
        return self(entries)


@dataclass(slots=True)
class SortSequence(List, ISortSequence):
    """
    Represents a sequence of sort criteria that can be applied sequentially.

    This class extends Python's built-in list and allows you to combine multiple Sort
    instances. When called, the sort criteria are applied in the order they appear.

    Examples
    --------
    >>> seq = SortSequence(Sort("name"), Sort("age", reverse=True))
    >>> sorted_entries = seq(entries)
    """

    def __init__(self, *sorts: Union[ISortBy, Iterable[ISortBy]]) -> None:
        """
        Initialize a SortSequence with one or more sort criteria.

        Parameters
        ----------
        *sorts : ISortBy or Iterable[ISortBy]
            One or more sort criteria.
        """
        super().__init__()
        for sort in sorts:
            self + sort

    def __add__(self, sort: Union[ISortBy, Iterable[ISortBy]]) -> List[ISortBy]:
        """
        Add a sort criterion or an iterable of criteria to the sequence.

        Parameters
        ----------
        sort : ISortBy or Iterable[ISortBy]
            A sort criterion or an iterable of sort criteria.

        Returns
        -------
        List[ISortBy]
            The updated list of sort criteria.
        """
        if isinstance(sort, Iterable):
            for s in sort:
                self + s
        else:
            if isinstance(sort, str):
                sort = Sort(sort)
            elif isinstance(sort, tuple):
                sort = Sort(*sort)
            self.append(sort)
        return self

    def __sub__(self, el: Union[Sort, str]) -> 'SortSequence':
        """
        Remove a sort criterion from the sequence.

        Parameters
        ----------
        el : Sort or str
            The sort criterion to remove. If a string is provided, a Sort is created from it.

        Returns
        -------
        SortSequence
            The updated sequence after removal.
        """
        self.remove(Sort(el) if isinstance(el, str) else el)
        return self

    def __call__(self, to_sort: Iterable[SupportsGetItem]) -> List[SupportsGetItem]:
        """
        Sequentially apply all sort criteria to the provided entries.

        Parameters
        ----------
        to_sort : Iterable[SupportsGetItem]
            The entries to sort.

        Returns
        -------
        List[SupportsGetItem]
            The sorted entries after all sorts have been applied.
        """
        for _sort in self:
            to_sort = _sort(to_sort)
        return list(to_sort)

    def apply_sorts(self, entries: Iterable[SupportsGetItem]) -> Iterable[SupportsGetItem]:
        """
        Apply all sort criteria to the provided entries.

        Parameters
        ----------
        entries : Iterable[SupportsGetItem]
            The entries to sort.

        Returns
        -------
        Iterable[SupportsGetItem]
            The sorted entries.
        """
        return self(entries)


Sorts = Optional[Union[Sort, SortSequence]]
"""
Type alias representing either a single Sort or a SortSequence.
"""
