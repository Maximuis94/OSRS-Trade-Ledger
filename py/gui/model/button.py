"""
Module with models for Gui Buttons (Button, Checkbutton, Radiobutton)

# TODO: Shift some stuff to controllers

"""
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable

from gui.model.grid import TkGrid
from gui.model.gui_widget import GuiWidget


# Clickcommand is the method that is called when the button is pressed.
class GuiButton(ttk.Button, GuiWidget):
    def __init__(self, frame, grid: TkGrid, grid_tag: str, command: Callable, textvariable=None, command_kwargs: dict=(),
                 variable=None, event_bindings=(), width=None, **kwargs):
        """ Class for setting up tk Button widget.
        A tk.Button built on top of the given frame with frequently used attributes, used to define tk elements in a
        standardized fashion. Commonly tweaked parameters can be passed as well. All parameters with the exception of
        grid correspond to attributes used in original tk objects.

        Parameters
        ----------
        frame : ttk.Frame
            The ttk frame on which the Label will be placed.

        command : callable
            Callback for when the button is pressed.

        Attributes
        ----------
        frame : ttk.Frame
            Frame on which the object will be placed

        Methods
        -------
        set_text(string)
            Method for changing the text displayed by the label
        """
        
        self.command_kwargs = command_kwargs
        self.command = command

        super().__init__(command=self.execute_command, width=width, **kwargs)
        super(GuiWidget, self).__init__(frame, grid_tag, grid, event_bindings)
        self.add_tk_var(var=tk.BooleanVar() if variable is None else variable, key='bool')
        self.add_tk_var(var=tk.StringVar() if textvariable is None else textvariable, key='text')
        
    def set_text(self, text: str):
        self.set_value(var_key='text', value=text)
        
    def set_command(self, command: Callable = None, kwargs: dict = None):
        if command is not None:
            self.command = command
        if kwargs is not None:
            self.command_kwargs = kwargs
    
    def execute_command(self):
        self.command(**self.command_kwargs)



if __name__ == '__main__':
    b = GuiButton(tk.Frame(), lambda a, b: True)
    for k, v in b.__dict__.items():
        print(k, v)