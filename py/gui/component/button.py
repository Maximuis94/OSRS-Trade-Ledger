"""
Module with models for Gui Buttons (Button, Checkbutton, Radiobutton)

# TODO: Shift some stuff to controllers

"""
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable
from typing import Dict, get_type_hints
from warnings import warn_explicit, warn

from gui.base.widget import GuiWidget
from gui.base.frame import TkGrid, GuiFrame
from gui.util.colors import Color
from gui.util.font import Font


_kw_typeerr_help = (f"Consider setting a breakpoint here for manual verification. If you are sure that the "
                    f"command_kwargs configured are correct, you can also disable this check by passing "
                    f"enable_type_warnings=False to the GuiButton constructor.")


# Clickcommand is the method that is called when the button is pressed.
class GuiButton(ttk.Button, GuiWidget):
    frame: ttk.Frame
    tag: str
    
    _command: Callable = lambda: None
    _command_kwargs: Dict[str, any]
    
    _text: tk.StringVar
    _font: Font = Font(11, "Consolas", False, False, False, False, Color.BLACK)
    _enable_type_warnings: bool
    
    def __init__(self, frame: GuiFrame, tag: str, command: Callable, command_kwargs: dict = None,
                 variable=None, event_bindings=(), width=None, button_text: str = None, **kwargs):
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
        self._command_kwargs = {} if command_kwargs is None else command_kwargs
        self.frame = frame
        self.tag = tag
        self._text = tk.StringVar(self.frame, value=button_text)
        self._enable_type_warnings = kwargs.pop('enable_type_warnings', True)
        
        if button_text is not None:
            self.button_text = button_text
        
        kwargs = self._set_padding(**kwargs)
        self.set_button_command({"command": command, "command_kwargs": command_kwargs})
        # self.command = lambda name: command(column_name=name)
        # self.command_kwargs = command_kwargs if isinstance(command_kwargs, dict) else {}

        super().__init__(self.frame, command=self.button_command, textvariable=self._text, width=width)
        
        self._set_bindings(event_bindings)
        self.apply_grid(**kwargs)
    
    @property
    def button_text(self) -> str:
        """The text displayed on the button"""
        return self._text.get()
    
    @button_text.setter
    def button_text(self, text: str):
        self._text.set(text)
        
    def button_command(self):
        """ Sets the onclick command and/or kwargs passed to it `command` and `command_kwargs`, respectively """
        return self._command(**self._command_kwargs)
        
    def set_button_command(self, kwargs):
        """ Sets the onclick command and/or kwargs passed to it `command` and `command_kwargs`, respectively """
        if kwargs.get('command') is not None:
            self._command = kwargs.pop('command')
            if kwargs.get('command_kwargs') is not None:
                self._command_kwargs = kwargs.pop('command_kwargs')
            else:
                self._command_kwargs = {}
            
            # If enabled, run a diagnostic check on the configured keyword args
            if self._enable_type_warnings:
                msg = []
                for arg, arg_type in get_type_hints(self._command).items():
                    cur = self._command_kwargs.get(arg)
                    if cur is None:
                        if hasattr(self, arg):
                            cur = getattr(self, arg)
                        else:
                            _m = str(self._command.__module__)+"." if hasattr(self._command, '__module__') else ""
                            msg.append(f"In GuiButton.command_kwargs, arg = {arg} : {arg_type.__name__} was not given "
                                       f"a value, while this value is expected by {_m}{self._command.__name__}()")
                    
                    elif not isinstance(cur, arg_type):
                        msg.append(f"Mismatch between command_kwarg set for arg={arg}; Expected type= {arg_type}, "
                                   f"but given type={type(cur)}")
                
                if len(msg) > 0:
                    msg.append(_kw_typeerr_help)
                    warn("\n - ".join(msg))



if __name__ == '__main__':
    def my_method(text: str = 'hoi'):
        print(text)
    f = GuiFrame(ttk.Frame(), ['aaabbb'])
    b = GuiButton(f, command=my_method, command_kwargs={'text': 'Ja'}, tag='a')
    b.button_command
    for k, v in b.__dict__.items():
        print(k, v)