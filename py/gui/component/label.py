"""
Module: gui_label.py
====================
Refactored GuiLabel class.

This module implements a label widget that integrates common functionality provided
by the GuiWidget mixin. It is designed to be placed within a GuiFrame and supports
automatic grid placement, text management, and event binding.

Classes
-------
GuiLabel
    A composite label widget with built-in support for dynamic text updates.

Examples
--------
Updating the label text via the property:

    >>> # Assume gui_frame is an instance of GuiFrame properly initialized.
    >>> label = GuiLabel(gui_frame, tag="mylabel", text="Initial text")
    >>> print(label.text)
    Initial text
    >>> label.text = "Updated text"
    >>> print(label.text)
    Updated text
"""
import tkinter as tk
import tkinter.ttk as ttk
from typing import Union, Sized, Iterable, Tuple
from gui.base.widget import GuiWidget
from gui.base.frame import GuiFrame
from gui.util.str_formats import shorten_string


class GuiLabel(ttk.Label, GuiWidget):
    """
    A refactored label widget that extends ttk.Label and integrates GuiWidget's features.

    This widget is designed to be instantiated within a GuiFrame. It automatically manages
    text via a Tkinter StringVar and supports grid placement and event binding through
    inherited helper methods.

    Parameters
    ----------
    gui_frame : GuiFrame
        The parent frame in which the label is placed.
    tag : str
        A tag used for grid placement.
    text : str, optional
        The text to display in the label.
    text_variable : tk.StringVar, optional
        An existing StringVar to use for the label's text. If not provided, one is created.
    event_bindings : iterable of tuple, optional
        An iterable of (event, callback) pairs to be bound to the widget.
    **kwargs : dict
        Additional options (e.g., padding, style) passed for grid layout and widget configuration.

    Attributes
    ----------
    label_text : tk.StringVar
        The StringVar that holds the label's text.

    Examples
    --------
    Updating the label text via the property:

        >>> # Assume gui_frame is an instance of GuiFrame properly initialized.
        >>> label = GuiLabel(gui_frame, tag="mylabel", text="Initial text")
        >>> print(label.text)
        Initial text
        >>> label.text = "Updated text"
        >>> print(label.text)
        Updated text
    """
    __slots__ = ()

    def __init__(self,
                 gui_frame: GuiFrame,
                 tag: str,
                 text: str = None,
                 text_variable: tk.StringVar = None,
                 event_bindings: Union[Sized, Iterable[Tuple[str, callable]]] = None,
                 **kwargs):
        """
        Initialize a GuiLabel instance.

        Parameters
        ----------
        gui_frame : GuiFrame
            The parent GuiFrame.
        tag : str
            The tag used for grid placement.
        text : str, optional
            The initial text to display.
        text_variable : tk.StringVar, optional
            A pre-existing StringVar for the label's text.
        event_bindings : iterable of tuple, optional
            Additional event binding pairs (event, callback).
        **kwargs : dict
            Additional keyword arguments for padding, styling, or grid configuration.
        """
        # Initialize common widget properties (sets self.frame, self.tag, and self._text)
        self.init_widget_start(gui_frame, tag, text=text, text_variable=text_variable, **kwargs)

        # Create the underlying ttk.Label widget; use the public frame property
        super().__init__(gui_frame.frame, textvariable=self._text, font=self.font.tk, **kwargs)

        # Finalize initialization: apply event bindings and grid placement.
        self.init_widget_end(event_bindings=event_bindings, **kwargs)

    def set_text(self, new_text: str):
        """
        Update the text displayed by the label.

        If a maximum text length is defined (_max_length), the text will be shortened accordingly.

        Parameters
        ----------
        new_text : str
            The new text to display.
        """
        if self._max_length is not None and len(new_text) > self._max_length:
            new_text = shorten_string(new_text, self._max_length)
        self._text.set(new_text)

    @property
    def text(self) -> str:
        """
        Get the current text of the label.

        Returns
        -------
        str
            The text stored in the label's StringVar.
        """
        return self._text.get()

    @text.setter
    def text(self, new_text: str) -> None:
        """
        Set the label's text.

        Parameters
        ----------
        new_text : str
            The new text to display.
        """
        self.set_text(new_text)
