"""
Module with the implementation of a labelled widget, which is a Frame with a Label and another widget on it.

"""
import tkinter as tk
from collections.abc import Callable
from dataclasses import dataclass
from tkinter import ttk
from typing import Dict, Type

from typing_extensions import NamedTuple

from gui.base.frame import TkGrid, GuiFrame
from gui.base.widget import GuiWidget
from gui.component.button import GuiButton
from gui.component.label import GuiLabel
from gui.util.constants import Side, tk_var
from gui.util.font import Font, FontSize, FontFamily
from gui.util.colors import Color


class Label(NamedTuple):
    """ Container class used to provide data for constructing a tk.Label with """
    text: str
    text_variable: tk.StringVar = None
    font: Font = Font(FontSize.NORMAL, FontFamily.CONSOLAS)


class Widget(NamedTuple):
    ...
    
    
class LabelledWidget(GuiFrame):
    """
    A GuiFrame with exactly two widgets on top of it; a GuiLabel and another GuiWidget.
    """
    label: ttk.Label
    frame: GuiFrame
    tag: str
    _label_tag: str = 'L'
    _widget_tag: str = 'W'
    # widget: Callable[GuiWidget]
    
    def __init__(self, parent: GuiFrame, tag: str, label: str or tk.StringVar, widget_type: Callable[GuiWidget],
                 label_side: Side = Side.LEFT, common_kwargs: Dict[str, any] = None,
                 label_kwargs: Dict[str, any] or Label = None, widget_kwargs: Dict[str, any] or Widget = None,
                 **kwargs):
        """
        Construct a GuiFrame with two widgets on top of it, one of them being a GuiLabel and the other one being another
        GuiWidget. Additional kwargs parameters will be passed to the superclass constructor.
        For some examples w.r.t. providing input, see the Examples section.
        
        Parameters
        ----------
        parent : tk.Frame
            The root Frame on top of which this GuiFrame will be placed
        label : str or tk.StringVar
            The text to display onto the label OR the StringVar to assign to it.
        widget_type : Callable[GuiWidget]
            The other Widget class; a GuiWidget subclass (e.g. GuiLabel / GuiButton / GuiEntry)
        label_side : Side, optional, Side.LEFT by default
            The side on which the label will be placed, relative to the other widget. By default, the label is placed to
            the left of the other widget.
        common_kwargs : Dict[str, any], optional, None by default
            keyword args to pass to both the label AND the other widget constructor
        label_kwargs : Dict[str, any], optional, None by default
            keyword args to pass only to the label constructor
        label_kwargs : Label, optional, None by default
            Container class that may help to specify various keyword args that can be passed
        widget_kwargs : Dict[str, any], optional, None by default
            keyword args to pass only to the widget constructor
        widget_kwargs : Widget, optional, None by default
            Container class that may help to specify various keyword args that can be passed
        **kwargs
            keyword args to pass to the superclass constructor
            
        Examples
        --------
        """
        self.frame = parent
        self.tag = tag
        super().__init__(self.frame, self._generate_grid(label_side), **kwargs)
        
        # Merge kwarg dicts and ensure that common_kwargs are present in both, while maintaining indidual kwags
        if common_kwargs is not None:
            if label_kwargs is None:
                label_kwargs = common_kwargs
            else:
                label_kwargs = {**common_kwargs, **label_kwargs}
            if widget_kwargs is None:
                widget_kwargs = common_kwargs
            else:
                widget_kwargs = {**common_kwargs, **widget_kwargs}
        else:
            if label_kwargs is None:
                label_kwargs = {}
            if widget_kwargs is None:
                widget_kwargs = {}
        
        self.label = GuiLabel(self, self._label_tag,
                              text_variable=tk_var(str, self, label) if isinstance(label, str) else label,
                              **label_kwargs)
        self.widget = widget_type(self, self._widget_tag, **widget_kwargs)
        super().grid(**self.frame.get_grid_kwargs(self.tag, **kwargs))
    
    def set_label_text(self, text: str):
        """ Set the text displayed by the label to `text` """
        self.label.text = text
    
    def _generate_grid(self, label_side: Side) -> TkGrid:
        """
        Return a labelled widget in which the label position is determined via `label_side`. Note that this method
        assumes there are exactly two widgets.

        Parameters
        ----------
        label_side : Side
            The side of the widget the label will be located at

        Returns
        -------
        Tuple[Dict[str, int], Dict[str, int]]
            A set of 2 dicts with the label position and widget position, respectively. The positions are defined in
            terms of a row and column, which are .grid() kwargs and can be passed onto grid as such.

        """
        if label_side == Side.LEFT:
            # label_position, widget_position = {'row': 0, 'column': 0}, {'row': 0, 'column': 1}
            g = [f"{self._label_tag}{self._widget_tag}"]
        elif label_side == Side.TOP:
            g = [f"{self._label_tag}", f"{self._widget_tag}"]
            # label_position, widget_position = {'row': 0, 'column': 0}, {'row': 1, 'column': 0}
        elif label_side == Side.RIGHT:
            g = [f"{self._widget_tag}{self._label_tag}"]
            # label_position, widget_position = {'row': 0, 'column': 1}, {'row': 0, 'column': 0}
        elif label_side == Side.BOTTOM:
            g = [f"{self._widget_tag}", f"{self._label_tag}"]
            # label_position, widget_position = {'row': 1, 'column': 0}, {'row': 0, 'column': 0}
        else:
            raise ValueError("Invalid value passed for label_side")
        return TkGrid(g)


    
    