"""
Module with various basic GUI/tkinter-related methods

"""
import tkinter as tk
import tkinter.ttk as ttk


def tk_var(value, name: str = None, master: tk.Misc = None) -> tk.Variable:
    """ Create a new tk var based on the type of `value` with its value set to `value` and return it """
    if isinstance(value, int):
        return tk.IntVar(value=value, name=name, master=master)
    elif isinstance(value, str):
        return tk.StringVar(value=value, name=name, master=master)
    elif isinstance(value, float):
        return tk.DoubleVar(value=value, name=name, master=master)
    elif isinstance(value, bool):
        return tk.BooleanVar(value=value, name=name, master=master)
    else:
        return tk.Variable(value=value, name=name, master=master)


