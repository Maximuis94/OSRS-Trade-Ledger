"""
Module for the TkGrid, which is a grid that accepts a list of strings and assigns widgets listed in these strings via a
tag the appropriate space in the GUI.

The idea behind the grid is to allow one to easily alter the way the gui is organized, without having to alter variables
across multiple instances of widgets. Instead, alter the list of strings.





"""
from collections import namedtuple
from collections.abc import Iterable, Collection, Sequence
from typing import Tuple, Dict

WidgetDimensions = namedtuple('WidgetDimensions', ['column', 'row', 'columnspan', 'rowspan'])


class TkGrid:
    def __init__(self, grid: Sequence[str]):
        """
        An alternative approach for representing tk elements in a grid-like fashion
        Object for automatically creating a grid-like representation for tkinter elements. The input is a list of Y
        strings of length X, tkinter elements are tagged with single characters (e.g. ['AABB', 'CCCC']). A grid-like
        representation is derived from this list of strings, in which each string within the list represents a row and
        the amount of characters per row can be derived to the width of the smallest atom.
        There is also a wild-card character * that can be used for filling unassigned spaces.

        Parameters
        ----------
        grid : list
            List of Y strings of equal length X, where identical characters dictate how much space is assigned to this
            tag

        Attributes
        ----------
        grid : dict
            A dict with a key for each single character tag passed in the input list of strings. Each dict contains a
            (x, y) tuple and a (w, h) tuple.

        Methods
        -------
        get_dims(tag: str) -> WidgetDimensions
            Get the parsed dimensions for widget `tag`


        Raises
        ------
        ValueError
            A ValueError will be raised if the strings within the list have varying lengths.

        Notes
        -----
        Given a row of AAABB, 3/5th of the space will be assigned to tk widget A on the left, and the remaining 2/5th
        to tk widget B on the right. This system was implemented to automatically assign proper X, Y, W and H
        coordinates, without having to tweak coordinates/dimensions for individual widgets. Note that each abstract
        object defined below can accept a TkGrid object used to derive its coordinates and size without having to
        explicitly define each one of them.
        """
        self.grid = grid
        self.widgets = {}
        
        self.width = len(grid[0])
        self.height = len(grid)
        
        for tag in self.identify_tags():
            self.parse_grid(tag)
        
    def identify_tags(self) -> Tuple[str]:
        """ Extract a list of unique tags from the grid """
        tags = []
        for line in self.grid:
            for char in line:
                if char != '*' and char not in tags:
                    tags.append(char)
        return tuple(tags)
        
    def parse_grid(self, tag: str):
        """ Parse the grid to identify the dimensions of `tag`, store the values as a WidgetDimensions tuple """
        w, h, x, y, n = None, None, None, None, None
        for _y, bar in enumerate(self.grid):
            if n is None:
                n = len(bar)
            elif len(bar) != n:
                raise ValueError(f'The amount of characters in row {y} does not match the amount of characters in '
                                 f'preceding rows. All rows should be equally sized.')
            if tag in bar and w is None:
                w = bar.count(tag)
                x = bar.index(tag)
                y = _y
                h = 1
            elif tag in bar and y is not None:
                h += 1
            elif tag not in bar and y is not None:
                break
        self.widgets[tag] = WidgetDimensions(x, y, w, h)
    
    def get_dims(self, tag, padx: int = 0, pady: int = 0, sticky: str = 'N', **kwargs) -> Dict[str, int]:
        """ Get the dimensions for widget `tag` and update dict w/ kwargs. Output can be passed directly to ttk.grid(). """
        if self.widgets.get(tag) is None:
            raise RuntimeError(f"Unable to find tag {tag} in this grid")
        output = self.widgets.get(tag)._asdict()
        output.update({'padx': padx, 'pady': pady, 'sticky': sticky})
        return output


if __name__ == '__main__':
    tkg = TkGrid(['AAAAAaaaaabbbbbbb',
            'AAAAAaaaaabbbbbbb',
            'ccccccccccccccccc'])
    print(tkg.dims)