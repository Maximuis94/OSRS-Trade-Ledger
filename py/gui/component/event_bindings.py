"""
Class with a list of existing event bindings. Event bindings are used to bind specific actions to certain events. An
example of an event is pressing the left mouse button.
This module can be used as a reference, while it is designed to provide an interface for adding specific event bindings.

Due to the large amount of specific key/type/modifier combinations, as well as the fact that  using event bindings is
not necessarily intuitive, it is recommended to explore various possibilities, and to further consult the  references
listed below. Implementations contain explanations that are based on these references and various practical uses.
It is highly recommended to experiment with various bindings and see how they work in practice; e.g. via a callback
method that prints the event that is passed.

References
----------
https://www.pythontutorial.net/tkinter/tkinter-event-binding/
https://tk-tutorial.readthedocs.io/en/latest/event/event.html
https://tcl.tk/man/tcl8.6/TkCmd/bind.htm

"""
import datetime
import tkinter as tk
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from types import MethodType
from typing import NamedTuple, Dict, Tuple


def print_event(e: tk.Event, keys: Iterable[str] = None, *args, **kwargs):
    """ Example of a callback that will print non-null values of event `e`. If keys is defined, follow that order. """
    # if e is None:
    print(type(e))
    print('Current time:', datetime.datetime.now())
    
    if keys is None:
        print(e.__dir__())
        print(e)
        try:
            print(', '.join([f"{k}={v}" for k, v in e.__dict__.items() if v != '??']) + '\n')
        except AttributeError:
            print(', '.join([f"{el}={e.__getattribute__(el)}" for el in e.__dir__()]) + '\n')
    else:
        print(', '.join([f"{k}={e.__getattribute__(k)}" for k in keys if e.__getattribute__(k) != '??']))


class _EventModifier(Enum):
    """ Enum with various EventModifiers that can be used """
    alt: str = "Alt"
    alt_left: str = "Alt_L"
    alt_right: str = "Alt_R"
    ctrl: str = "Control"
    ctrl_left: str = "Control_L"
    ctrl_right: str = "Control_R"
    shift: str = "Shift"
    shift_left: str = "Shift_L"
    shift_right: str = "Shift_R"
    any: str = "Any"


class _EventType(NamedTuple):
    type: int
    name: str


class _EventType(Enum):
    """ Event Types that can be used. Specific event types are denoted in snake_case. """
    activate = _EventType(36, "Activate")
    button = _EventType(4, "Button")
    button_release = _EventType(5, "ButtonRelease")
    configure = _EventType(22, "Configure")
    deactivate = _EventType(37, "Deactivate")
    destroy = _EventType(17, "Destroy")
    enter = _EventType(7, "Enter")
    expose = _EventType(12, "Expose")
    focus_in = _EventType(9, "FocusIn")
    focus_out = _EventType(10, "FocusOut")
    key_press = _EventType(2, "KeyPress")
    key_release = _EventType(3, "KeyRelease")
    leave = _EventType(8, "Leave")
    map = _EventType(19, "Map")
    motion = _EventType(6, "Motion")
    mouse_wheel = _EventType(38, "MouseWheel")
    unmap = _EventType(18, "Unmap")
    visibility = _EventType(15, "Visibility")


class EventBindingTag(Enum):
    LMB = "<Button-1>"
    RMB = "<Button-2>"


@dataclass(frozen=True, slots=True)
class EventBinding:
    """ Immutable EventBinding Dataclass that can be passed to a bind() method of a tkinter Widget """
    tag: EventBindingTag
    callback: Callable
    
    
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
    f""" Returns an EventBinding for a left mouse button click via a semi-hardcoded call to event_binding() """
    return event_binding("<Button-1>", action, kwargs)
    

def rmb(action: Callable, kwargs: Dict[str, any] = None) -> Tuple[str, Callable]:
    f""" Returns an EventBinding for a right mouse button click via a semi-hardcoded call to event_binding() """
    return event_binding("<Button-2>", action, kwargs)


