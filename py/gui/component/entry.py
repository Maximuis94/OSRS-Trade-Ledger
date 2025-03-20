"""
Module: gui_entry.py
====================
Refactored GuiEntry class.

This module implements a composite entry widget with an associated label.
The label can be positioned on the left, right, top, or bottom of the entry.
Grid positions for the label and entry are determined based on the label_side parameter.
All docstrings follow the NumPy documentation standard.
"""

import tkinter as tk
import tkinter.ttk as ttk
from typing import Union, Tuple, List
from gui.base.widget import GuiWidget
from gui.base.frame import GuiFrame
from gui.util.constants import Side
from gui.util.font import Font


class GuiEntry(tk.Entry, GuiWidget):
    """
    Composite GUI Entry widget with an associated label.

    This widget consists of a label and an entry field grouped together.
    The label's position relative to the entry is determined by the `label_side` parameter.
    The widget supports resetting its content to a default value.

    Parameters
    ----------
    gui_frame : GuiFrame
        The parent GuiFrame in which the widget is placed.
    label_text : str or tk.StringVar, optional
        The text to display in the label. If a StringVar is provided, it is used directly.
    label_side : Side, optional
        The side on which to place the label relative to the entry.
        Valid values are Side.LEFT, Side.RIGHT, Side.TOP, or Side.BOTTOM. Default is Side.LEFT.
    entry_variable : tk.StringVar, optional
        The Tkinter variable to bind to the entry's text.
    value : str, optional
        The initial value to set in the entry. If not provided, the default_value is used.
    default_value : str, optional
        The default value for the entry, used when resetting.

    Attributes
    ----------
    label : tk.Label
        The label widget.
    _entry_text : tk.StringVar
        The Tkinter variable holding the entry's text.
    default_value : str
        The default value used for resetting the entry.
    """

    __slots__ = ("label_text", "label", "_entry_text", "_default_value")
    _default_value: str = ""

    def __init__(self,
                 gui_frame: GuiFrame,
                 label_text: Union[str, tk.StringVar] = None,
                 label_side: Side = Side.LEFT,
                 entry_variable: tk.StringVar = None,
                 value: str = None,
                 default_value: str = None,
                 **kwargs):
        # Determine grid positions based on label_side.
        if label_side == Side.LEFT:
            label_position, entry_position = {'row': 0, 'column': 0}, {'row': 0, 'column': 1}
            grid_layout = ["AB"]
        elif label_side == Side.TOP:
            grid_layout = ["A", "B"]
            label_position, entry_position = {'row': 0, 'column': 0}, {'row': 1, 'column': 0}
        elif label_side == Side.RIGHT:
            grid_layout = ["BA"]
            label_position, entry_position = {'row': 0, 'column': 1}, {'row': 0, 'column': 0}
        elif label_side == Side.BOTTOM:
            grid_layout = ["B", "A"]
            label_position, entry_position = {'row': 1, 'column': 0}, {'row': 0, 'column': 0}
        else:
            raise ValueError("Invalid value passed for label_side")

        if default_value is not None:
            self.default_value = str(default_value)

        # Ensure a valid parent GuiFrame is provided.
        if not gui_frame or not hasattr(gui_frame, "frame"):
            raise ValueError("A valid GuiFrame instance must be provided as the parent.")

        # Create a subframe within the parent frame to contain the label and the entry.
        sub_frame = ttk.Frame(gui_frame.frame)

        # Initialize the label.
        if isinstance(label_text, tk.StringVar):
            self.label_text = label_text
        else:
            self.label_text = tk.StringVar(value=label_text)
        self.label = tk.Label(sub_frame, textvariable=self.label_text)
        self.label.grid(**label_position)

        # Initialize the entry text variable.
        self._entry_text = entry_variable if entry_variable is not None else tk.StringVar()
        self._entry_text.set(self._default_value if value is None else value)

        # Initialize common widget properties using GuiWidget's helper.
        # This sets self.frame and processes padding options.
        self.init_widget_start(frame=gui_frame, tag="entry", text=None, text_variable=self._entry_text, **kwargs)

        # Initialize the entry widget by calling the superclass initializer.
        super().__init__(sub_frame, textvariable=self._entry_text, font=self.font.tk, **kwargs)
        self.init_widget_end(apply_grid=False, **kwargs)

        # Place the entry and the subframe using grid geometry.
        self.grid(**entry_position)
        sub_frame.grid(gui_frame.get_grid_kwargs(self.tag, **kwargs))

    def set_label_text(self, text: str = None, text_variable: tk.StringVar = None) -> None:
        """
        Update the text displayed by the label and/or its associated variable.

        Parameters
        ----------
        text : str, optional
            New text to display in the label.
        text_variable : tk.StringVar, optional
            A new StringVar to use as the label's text variable.
        """
        if text_variable is not None:
            self.label_text = text_variable
        if text is not None:
            self.label_text.set(text)

    def parse(self) -> str:
        """
        Retrieve the current value from the entry.

        Returns
        -------
        str
            The text contained in the entry.
        """
        return self._entry_text.get()

    def reset(self) -> None:
        """
        Reset the entry's text variable to its default value.
        """
        self._entry_text.set(self._default_value)

    @property
    def default_value(self) -> str:
        """
        Get the default value of the entry.

        Returns
        -------
        str
            The default text value.
        """
        return self._default_value

    @default_value.setter
    def default_value(self, value: str) -> None:
        """
        Set the default value for the entry.

        Parameters
        ----------
        value : str
            The new default value.
        """
        self._default_value = str(value)
