"""
Various constants used by the GUI

"""
from collections.abc import Callable
from enum import Enum
import tkinter as tk
from typing import Dict, Tuple, Type

empty_tuple: Tuple = ((),)[0]
"""A tuple with no elements in it"""

letters: str = "abcdefghijklmnopqrstuwvxyzABCDEFGHIJKLMNOPQRSTUWVXYZ"
"""All lower- and upper-case letters sorted alphabetically. """


TK_VAR_MAPPING: Dict[Callable, Callable[[tk.Frame, any, str], tk.Variable]] = {
    int: tk.IntVar,
    float: tk.DoubleVar,
    str: tk.StringVar,
    bool: tk.BooleanVar
}
""" Basic python types mapped to their tkinter object counterpart """

DEFAULT_DTYPE_MAPPING: Dict[Type, str] = {
    int: "int64",
    float: "float64",
    bool: "bool",
    str: "string"
}
""" Basic Python types mapped to their pandas.DataFrame dtype counterpart. Use Int/Float/boolean for nullables; this
 does affect performance """


def tk_var(var_type: Callable[[], int | float | str | bool], *args, **kwargs) -> tk.Variable:
    """
    Construct a new tkinter Variable of the specified type `var_type`, or derive the type if passed as value. Subsequent
    args and kwargs will be passed to the resulting constructor.
    
    Parameters
    ----------
    var_type : Callable[[], int | float | str | bool]
        The type that dictates which TkVar should be returned. Choice is limited to int, float, bool or str.
        Alternatively, type(1) will be parsed as int, for instance.

    Returns
    -------
    tk.Variable
        A tkinter Variable is returned. Which variable depends on `var_type`; passing int will return a tkinter.IntVar.
        The tkinter Variable will be constructed with *args and **kwargs, if provided.

    """
    try:
        return TK_VAR_MAPPING[var_type](*args, **kwargs)
    except TypeError as e:
        try:
            if kwargs['_recursion']:
                note = "\nTypeError persists, even after a recursive call with type(var_type) was made. Consider " \
                       f"verifying the input that was provided;\n var_type={var_type}\n args={args}\n kwargs={kwargs}"
                e.add_note(note)
                raise TypeError(e)
        except KeyError:
            ...
        return tk_var(type(var_type), *args, _recursion=True, **kwargs)
    except KeyError:
        e = f"Variable type {var_type} does not have a tkinter variable mapped to it. The following var_type inputs " \
             f"are legal;\n\t"
        e += "\n\t".join([k.__name__ for k in TK_VAR_MAPPING.keys()])
        raise ValueError(e)
    

class Side(Enum):
    """ Fixed set of sides that can be passed as args to describe a side of a Widget """
    TOP: int = 0
    RIGHT: int = 1
    BOTTOM: int = 2
    LEFT: int = 3


class Alignment(Enum):
    """ Orientations for aligning series of 1d widgets """
    HORIZONTAL: int = 0
    VERTICAL: int = 1
