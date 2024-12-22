"""
Module with the implementation of a Radio Button Panel


"""
import tkinter as tk
from collections.abc import Iterable, Callable, Sized
from dataclasses import dataclass
from tkinter import ttk
from typing import Tuple

from gui.base.frame import GuiFrame, TkGrid
from gui.base.widget import GuiWidget
from gui.component.radio_button import GuiRadiobutton
from gui.util.constants import letters, Alignment, tk_var, empty_tuple


@dataclass(slots=True, frozen=True)
class RadioButtonModel:
    """
    Simplified representation of a RadioButton that will be used to construct one from, specifically designed for the
    button panel. If no value is assigned, this will be a value equal to the index of that specific radiobutton.
    """
    label: str = ""
    value: any = None
    event_bindings: Iterable[Tuple[str, Callable]] = ()


class RadioButtonPanel(GuiFrame, GuiWidget):
    """
    Class based on a ttk.Frame with a multitude of GuiRadiobuttons.
    The Radiobuttons share a tkinter Variable that captures the currently selected button.
    """
    frame: ttk.Frame
    radio_buttons: Tuple[GuiRadiobutton, ...]
    _value: tk.Variable
    
    def __init__(self, parent: ttk.Frame, radio_buttons: Iterable[RadioButtonModel] and Sized,
                 variable: tk.Variable = None, alignment: Alignment = Alignment.VERTICAL, default_value: any = None):
        """
        Construct the hosting Frame, as well as a set of GuiRadiobuttons.
        
        Parameters
        ----------
        parent : ttk.Frame
            The Frame that will have the button panel placed on top of it
        radio_buttons : Iterable[GuiRadiobutton]
            A set of simplified radio button representations that will be iterated over to construct the radio buttons
        variable : tk.Variable, optional, None by default
            The tkinter Variable to use for radio button values. If not specified, generate one based on the default
            value.
        """
        n = len(radio_buttons)
        super().__init__(parent,
                         grid_layout=TkGrid.generic_layout(n, alignment))
        
        if default_value is None:
            self.default_value = 0 if radio_buttons[0].value is None else radio_buttons[0].value
        else:
            self.default_value = default_value
            self._value.set(default_value)
        self._value = tk_var(type(self.default_value), master=self.frame) if variable is None else variable
        
        self.radio_buttons = tuple((
            GuiRadiobutton(
                frame=self,
                tag=tag,
                text=rb.label,
                value=rb.value if rb.value is not None else idx,
                variable=self._value,
                event_bindings=rb.event_bindings
            ) for idx, rb, tag in zip(range(n), radio_buttons, letters[:len(radio_buttons)])
        ))
        
    @property
    def selected(self) -> any:
        """ The value of the selected radio button within the panel """
        return self._value.get()
    
    def reset(self):
        """ Reset the underlying tkinter variable to its default state """
        self._value.set(self.default_value)
    
        
        