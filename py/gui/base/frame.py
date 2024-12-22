"""
Module with base Frame definition used throughout the project.
The Frame is an augmented version of the ttk.Frame; it uses an underlying grid that is used to dictate how frames
should be organized.

"""
from collections import namedtuple
import tkinter as tk
from collections.abc import Callable
from tkinter import ttk, IntVar
from typing import Sequence, Tuple, Dict, List

from gui.util.constants import Alignment, letters, Side
from sqlite.models import Index
from util.data_structures import remove_dict_entries

WidgetDimensions = namedtuple('WidgetDimensions', ['column', 'row', 'columnspan', 'rowspan'])


class TkGrid:
    __slots__ = 'tags', 'grid', 'widgets', 'width', 'height'
    
    tags: Tuple[str, ...]
    grid: Sequence[str]
    widgets: Dict[str, WidgetDimensions]
    width: int
    height: int
    
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
        tags: Tuple[str, ...]
            An exhaustive list of unique tags that can be found within the Grid. Corresponds to the key list of widgets.
        widgets: Dict[str, WidgetDimensions]
            A dict with widget tags as keys, and corresponding dimensions as values.
        width: int
            Width of the grid. Note that this is not the absolute width, but the amount of characters per row
        height: int
            Height of the grid. Note that this is not the absolute height, but the amount of rows

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
        
        See Also
        --------
        https://anzeljg.github.io/rin2/book2/2405/docs/tkinter/grid-methods.html
            Additional information on getting/setting tk grid properties.
        
        """
        self.grid = grid
        self.widgets = {}
        
        self.width = len(grid[0])
        self.height = len(grid)
        
        for tag in self.identify_tags():
            self.parse_grid(tag)
        
    def identify_tags(self) -> Tuple[str, ...]:
        """ Extract a list of unique tags from the grid, excluding the wildcard tag * """
        tags = []
        for line in self.grid:
            for char in set(line):
                if char != '*' and char not in tags:
                    tags.append(char)
        self.tags = tuple(sorted(list(tags)))
        return self.tags
        
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
        # print(self.widgets)
    
    def get_dims(self, tag, padx: int = None, pady: int = None, sticky: str = None, **kwargs) -> Dict[str, int]:
        """ Get the dimensions for widget `tag`. Output can be given to ttk.grid() as kwargs"""
        if self.widgets.get(tag) is None:
            raise RuntimeError(f"Unable to find tag {tag} in this grid")
        output = self.widgets.get(tag)._asdict()
        # print(padx, pady)
        # print(output)
        output.update({'padx': padx, 'pady': pady, 'sticky': sticky})
        # print(output)
        return output
    
    def has_tag(self, tag: str) -> bool:
        """ Return True if `tag` exists within the underlying grid layout """
        return self.widgets.get(tag) is not None
    
    @staticmethod
    def generic_layout(n_widgets: int, alignment: Alignment) -> Tuple[str, ...]:
        """ Return a grid layout of `n_widgets` equally sized widgets aligned as a horizontal or vertical bar """
        try:
            if alignment == Alignment.VERTICAL:
                return tuple((c for c in letters[:n_widgets]))
            elif alignment == Alignment.HORIZONTAL:
                return tuple([letters[:n_widgets]])
            raise ValueError("Invalid input value for `alignment`; it can be Alignment.VERTICAL or Alignment.HORIZONTAL")
        except IndexError as e:
            if n_widgets > len(letters):
                raise IndexError("Amount of widgets is not allowed to exceed 52")
            raise e
    
    

        
class GuiFrame(ttk.Frame):
    _grid: TkGrid
    _grid_args: Tuple = ("column", "columnspan", "row", "rowspan", "in", "ipadx", "ipady", "padx", "pady", "sticky")
    __slots__ = "width", "height", "_grid"
    
    def __init__(self, parent, grid_layout: Sequence[str] or TkGrid, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.width: int = self.winfo_width()
        self.height: int = self.winfo_height()
        self._grid = grid_layout if isinstance(grid_layout, TkGrid) else TkGrid(grid_layout)
        
    def init_widget_start(self, tag: str, text: str = None, text_variable: tk.StringVar = None, **kwargs):
        """ Method that is to be called before initializing the superclass in the subclass __init__() """
        self.tag = tag
        
        self._text = tk.StringVar() if text_variable is None else text_variable
        if text is not None:
            self._text.set(text)
            
        kwargs = self._set_padding(**kwargs)
    
    def get_widget_dimensions(self, tag: str) -> Tuple[int, int]:
        """ Returns the width and height of the widget associated with `tag` in the widget layout. """
        try:
            d = self._grid.widgets[tag]
            return int((d.w/self._grid.width)*self.width), int((d.h/self._grid.height)*self.height)
        except KeyError:
            e = f"Tag '{tag}' does not exist within this grid layout;\n\t"
            e += "\n\t".join(self._grid.grid)
            raise ValueError(e)
    
    def get_grid_kwargs(self, widget_tag: str, **kwargs) -> Dict[str, any]:
        """
        Returns a kwargs dict that can be passed directly to Widget.grid(). Extra kwargs passed will be added to the
        returned dict, which can in turn be passed on to the grid() call, provided that kwarg is accepted by grid().
        
        Parameters
        ----------
        widget_tag : str
            The tag assigned to the widget that will call the grid() method
        
        Other Parameters
        ----------------
        pad : int or None or Tuple[int or None, int or None], optional, None by default
            A tuple that describes the padding in pixels that is to be applied. 1 value will be applied vertically and
             horizontally; 2 values define horizontal and vertical padding, respectively.
            These values will be translated to padx and pady.
        ipad: int or None or Tuple[int or None, int or None], optional, None by default
            Same as padding, but applies to internal padding instead. These values will be translated to ipadx and ipady
        sticky : str, optional, None by default
            The sticky arg to pass to the grid call. Dictates to which side the widget will 'stick'. Can be composed of
            multiple characters (e.g. EW indicates it should stick to both eastern and western side).
            The string passed here should consist of the following characters; N, E, S, W. It may also be an empty str,
            in which case it will be placed in the centre of the cell.
            
        Returns
        -------
        Dict[str, any]
            A keyword-args dict that can be passed on to grid()

        """
        
        if len(widget_tag) != 1:
            raise ValueError("widget_tag has to be of length 1")
        
        try:
            value = kwargs['pad']
            if isinstance(value, int):
                kwargs.update({'padx': value, 'pady': value})
            elif isinstance(value, tuple):
                if value[0] is not None:
                    kwargs['padx'] = value[0]
                if value[1] is not None:
                    kwargs['pady'] = value[1]
            del kwargs['pad']
        except KeyError:
            ...
        
        try:
            value = kwargs['ipad']
            if isinstance(value, int):
                kwargs.update({'ipadx': value, 'ipady': value})
            elif isinstance(value, tuple):
                if value[0] is not None:
                    kwargs['ipadx'] = value[0]
                if value[1] is not None:
                    kwargs['ipady'] = value[1]
            del kwargs['ipad']
        except KeyError:
            ...
        
        kwargs.update(self._grid.get_dims(widget_tag))
        
        return {k: kwargs[k] for k in frozenset(self._grid_args).intersection(kwargs.keys())}
    
    def _set_padding(self, **kwargs):
        """ Set x- and y-padding for this Widget, either via padxy as a tuple, or padx and pady separately """
        keys = frozenset(('padxy', 'padx', 'pady')).intersection(tuple(kwargs.keys()))
        # print(kwargs, keys)
        if len(keys) > 0:
            for next_pad in frozenset(('padxy', 'padx', 'pady')).intersection(kwargs):
                value = kwargs.get(next_pad)
                if value is not None:
                    if next_pad == 'padxy':
                        self.padx, self.pady = value
                        break
                    else:
                        self.__setattr__(next_pad, value)
            kwargs = remove_dict_entries(_dict=kwargs, keys=keys)
        return kwargs
        
    def _set_bindings(self, event_bindings: List[Tuple[str, Callable]] = None):
        if event_bindings is not None:
            self.event_bindings += event_bindings
        
        for trigger, command in self.event_bindings:
            self.bind(trigger, command)
    
    def add_tk_var(self, var: tk.Variable, key: str, value: any = None):
        if value is not None:
            var.set(value)
        self._tkinter_variable_dict[key] = var
        
    # def set_event_bindings(self, bindings: List[tuple] = None, replace_bindings: bool = True):
    def set_event_bindings(self, event_bindings: List[Tuple[str, Callable] or EventBinding] = None,
                           replace_bindings: bool = True):
        """
        Iterate over the configured event bindings list and bind them. If additional bindings are supplied, append them
        to the existing list or replace the existing bindings with them.
        
        Parameters
        ----------
        event_bindings : List[tuple], optional, None by default
            An Iterable with tuples that specify an event listener and the command to execute if the event triggers.
        replace_bindings : bool, optional, True by default
            Flag that dictates whether `bindings` will replace self.event_bindings, or whether it will be added to it.

        Returns
        -------

        """
        if event_bindings is not None:
            self.event_bindings += event_bindings
        
        for trigger, command in self.event_bindings:
            self.bind(trigger, command)
        if isinstance(event_bindings, list) and len(event_bindings[0]) == 2:
            self.event_bindings = event_bindings if replace_bindings else self.event_bindings + event_bindings
            
        for next_binding in self.event_bindings:
            try:
                if isinstance(next_binding, EventBinding):
                    self.bind(next_binding.tag.value(), next_binding.callback)
            except AttributeError:
                self.bind(next_binding[0], next_binding[1])
        
