"""
Module: event_bindings.py
=========================
This module contains a class representation for event bindings used throughout the GUI.
It provides a structured way to define an event (e.g., '<Button-1>') along with its associated callback,
and includes an optional description.

Classes
-------
EventBinding
    Represents an immutable event binding with an event string, a callback, and an optional description.
"""

from dataclasses import dataclass
from typing import Callable, Dict, Tuple, NamedTuple
from enum import Enum


class StandardEvent(Enum):
    """
    Enumeration of common Tkinter event binding strings.

    These events are typically used for structural or interaction events
    (e.g., mouse clicks, focus changes, window configuration) rather than individual key events.

    Attributes
    ----------
    BUTTON1 : str
        Single left mouse button press ("<Button-1>").
    BUTTON2 : str
        Single middle mouse button press ("<Button-2>").
    BUTTON3 : str
        Single right mouse button press ("<Button-3>").
    DOUBLE_BUTTON1 : str
        Double-click of the left mouse button ("<Double-Button-1>").
    DOUBLE_BUTTON3 : str
        Double-click of the right mouse button ("<Double-Button-3>").
    ENTER : str
        Mouse pointer enters a widget ("<Enter>").
    LEAVE : str
        Mouse pointer leaves a widget ("<Leave>").
    FOCUS_IN : str
        Widget gains focus ("<FocusIn>").
    FOCUS_OUT : str
        Widget loses focus ("<FocusOut>").
    KEY_PRESS : str
        Generic key press event ("<KeyPress>").
    KEY_RELEASE : str
        Generic key release event ("<KeyRelease>").
    MOTION : str
        Mouse motion within a widget ("<Motion>").
    MOUSE_WHEEL : str
        Mouse wheel event ("<MouseWheel>").
    CONFIGURE : str
        Widget is resized or configured ("<Configure>").
    DESTROY : str
        Widget is destroyed ("<Destroy>").
    MAP : str
        Widget is mapped (made visible) ("<Map>").
    UNMAP : str
        Widget is unmapped (hidden) ("<Unmap>").
    """
    BUTTON1 = "<Button-1>"
    BUTTON2 = "<Button-2>"
    BUTTON3 = "<Button-3>"
    DOUBLE_BUTTON1 = "<Double-Button-1>"
    DOUBLE_BUTTON3 = "<Double-Button-3>"
    ENTER = "<Enter>"
    LEAVE = "<Leave>"
    FOCUS_IN = "<FocusIn>"
    FOCUS_OUT = "<FocusOut>"
    KEY_PRESS = "<KeyPress>"
    KEY_RELEASE = "<KeyRelease>"
    MOTION = "<Motion>"
    MOUSE_WHEEL = "<MouseWheel>"
    CONFIGURE = "<Configure>"
    DESTROY = "<Destroy>"
    MAP = "<Map>"
    UNMAP = "<Unmap>"


class EventBinding(NamedTuple):
    """
    Represents an event binding for a GUI widget.

    Parameters
    ----------
    event : str
        The event string to bind (e.g., '<Button-1>', '<KeyPress>', etc.).
    callback : Callable
        The function to be invoked when the event is triggered.
    description : str, optional
        A brief description of the event binding. Default is an empty string.

    Examples
    --------
    >>> def on_click(event):
    ...     print("Widget clicked!")
    >>> binding = EventBinding(event="<Button-1>", callback=on_click, description="Left mouse click event")
    >>> binding.event
    '<Button-1>'
    """
    event: str
    callback: Callable
    description: str = ""
    
    def __eq__(self, other):
        return self.event == other.event and self.callback == other.callback
    
    def __ne__(self, other):
        return self.event != other.event or self.callback != other.callback
    
    @property
    def bind_args(self) -> Tuple[str, Callable]:
        """args that can be passed as positional args to Widget.bind()"""
        return self.event, self.callback


def event_binding(event_tag: str, action: Callable, kwargs: Dict[str, any] = None) -> Tuple[str, Callable]:
    """

    Parameters
    ----------
    event_tag : str
        A string that represents the event that triggers the action, e.g. clicking the left mouse button
    action : Callable
        The action to execute if the event occurs
    kwargs: Dict[str, any]
        The kwargs dict to pass to 'action' if the event occurs. Note that this is a static argument.

    Returns
    -------
    Tuple[str, Callable]
        A tuple with the event binding tag and the corresponding action

    """
    if kwargs is None:
        return event_tag, action
    else:
        return event_tag, lambda e: action(e, **kwargs)


def lmb(action: Callable, kwargs: Dict[str, any] = None) -> Tuple[str, Callable]:
    f"""Returns an EventBinding for a left mouse button click via a semi-hardcoded call to event_binding()"""
    return event_binding("<Button-1>", action, kwargs)


def rmb(action: Callable, kwargs: Dict[str, any] = None) -> Tuple[str, Callable]:
    f"""Returns an EventBinding for a right mouse button click via a semi-hardcoded call to event_binding()"""
    return event_binding("<Button-2>", action, kwargs)
