"""
Module: gui_button.py
======================
GuiButton class, used for creating a Button.


"""

import tkinter as tk
import tkinter.ttk as ttk
from typing import Callable, Dict, Any, get_type_hints
from warnings import warn

from gui.base.frame import GuiFrame
from gui.base.widget import GuiWidget
from gui.util.colors import Color

_kw_typeerr_help = (
    "Consider setting a breakpoint here for manual verification. If you are sure that the "
    "command_kwargs configured are correct, you can also disable this check by passing "
    "enable_type_warnings=False to the GuiButton constructor."
)

button_keywords = ("text", "image", "compound", "command", "width", "padding", "takefocus", "state", "cursor", "style")


class GuiButton(ttk.Button, GuiWidget):
    """
    A refactored GuiButton that extends ttk.Button and integrates GuiWidget's features.

    Responsibilities
    ------------
    - Manage text via a Tkinter StringVar.
    - Execute a command with validated keyword arguments.
    - Automate grid placement and event binding using GuiWidget helpers.
    - Optionally, configure a custom ttk style using font and color settings.

    Attributes
    ----------
    _command : Callable
        The callback function invoked on button press.
    _command_kwargs : dict
        Keyword arguments for the command.
    _enable_type_warnings : bool
        Flag controlling type-check warnings.
    """

    __slots__ = ("_command", "_command_kwargs", "_enable_type_warnings")

    def __init__(self,
                 gui_frame: GuiFrame,
                 tag: str,
                 command: Callable,
                 command_kwargs: Dict[str, Any] = None,
                 variable: Any = None,
                 event_bindings=(),
                 width: int = None,
                 button_text: str = None,
                 style: str = None,
                 **kwargs):
        """
        Initialize a GuiButton.

        Parameters
        ----------
        gui_frame : GuiFrame
            Parent frame containing the button.
        tag : str
            Tag for grid placement.
        command : Callable
            Callback function invoked on press.
        command_kwargs : dict, optional
            Keyword arguments for the command.
        variable : Any
            Not used in this refactoring; use button_text instead.
        event_bindings : iterable, optional
            Additional event bindings.
        width : int, optional
            Button width.
        button_text : str, optional
            Text to display on the button.
        style : str, optional
            ttk style to apply to the button. If not provided, a default style ("GuiButton.TButton")
            is configured using the default font and Color.BLACK.
        kwargs : dict
            Additional options (e.g., padding) passed for grid layout.
        """
        # Store command configuration.
        self._command_kwargs = command_kwargs or {}
        self._command = command
        self._enable_type_warnings = kwargs.pop('enable_type_warnings', True)

        # Initialize common widget properties.
        GuiWidget.__init__(self)
        self.frame = gui_frame  # Use GuiFrame's public 'frame' property.
        self.tag = tag

        # Initialize text variable via GuiFrame helper (also processes padding).
        text_var = gui_frame.init_widget_start(tag, text=button_text, **kwargs)
        self._text = text_var

        # Set up the command and validate keyword arguments.
        self._set_button_command(command, self._command_kwargs)

        # Determine style. If none is provided, create/use a default style.
        if style is None:
            style = "GuiButton.TButton"
            s = ttk.Style()
            # Configure default style using the widget's font and default foreground color.
            s.configure(style, font=self.font.tk, foreground=Color.BLACK.value.hexadecimal)
        kwargs.setdefault('style', style)

        # Create the underlying ttk.Button; assign it to tk_widget for GuiWidget management.
        self.tk_widget = ttk.Button(gui_frame.frame,
                                    command=self._button_command,
                                    textvariable=self._text,
                                    width=width,
                                    **{k: kwargs[k] for k in frozenset(kwargs.keys()).intersection(button_keywords)})
        # Finalize initialization: event bindings and grid placement.
        self.init_widget_end(event_bindings, **kwargs)

    @property
    def button_text(self) -> str:
        """
        Return the text displayed on the button.

        Returns
        -------
        str
            The current text of the button.
        """
        return self._text.get()

    @button_text.setter
    def button_text(self, text: str):
        """
        Set the text displayed on the button.

        Parameters
        ----------
        text : str
            The new text to display.
        """
        self._text.set(text)

    def _button_command(self):
        """
        Internal wrapper for button press.

        Executes the assigned command with the configured keyword arguments.

        Returns
        -------
        any
            The return value of the command.
        """
        return self._command(**self._command_kwargs)

    def _set_button_command(self, command: Callable, command_kwargs: Dict[str, Any]) -> None:
        """
        Configure and validate the button's command and its keyword arguments.

        If type warnings are enabled, this method checks the command's type hints and
        warns if expected arguments are missing or of an unexpected type.

        Parameters
        ----------
        command : Callable
            The callback function to be assigned.
        command_kwargs : dict
            Keyword arguments for the command.
        """
        self._command = command
        self._command_kwargs = command_kwargs or {}
        if self._enable_type_warnings:
            messages = []
            for arg, expected_type in get_type_hints(command).items():
                if arg not in self._command_kwargs:
                    messages.append(
                        f"Missing argument '{arg}' (expected type {expected_type.__name__}) in command {command.__name__}."
                    )
                elif not isinstance(self._command_kwargs[arg], expected_type):
                    messages.append(
                        f"Argument '{arg}' type mismatch: expected {expected_type.__name__}, got {type(self._command_kwargs[arg]).__name__}."
                    )
            if messages:
                messages.append(_kw_typeerr_help)
                warn("\n - ".join(messages))


if __name__ == '__main__':
    def my_method(text: str = 'hoi'):
        print(text)

    # Create a basic Tkinter application for testing.
    root = tk.Tk()
    root.geometry("300x200")

    # Create a GuiFrame with a simple grid layout.
    gui_frame = GuiFrame(ttk.Frame(root), ["aaabbb"])

    # Instantiate a GuiButton inside the frame.
    btn = GuiButton(gui_frame, tag='a', command=my_method,
                    command_kwargs={'text': 'Ja'}, button_text="Click Me")

    # Simulate a button press.
    btn._button_command()
    print("Button text:", btn.button_text)

    root.mainloop()
