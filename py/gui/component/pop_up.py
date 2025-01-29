from collections.abc import Callable
import tkinter as tk
import tkinter.ttk as ttk
import time
from overrides import override

import util.gui as ug
import util.str_formats as fmt
from global_variables.classes import SingletonMeta
from gui.component.button import GuiButton
from tasks.async_task import AsyncTask



class GuiCounter(SingletonMeta, AsyncTask):
    def __init__(cls, task, **kwargs):
        super().__init__(task=task, **kwargs)
    
    def set_task(self, task: Callable):
        self.task = task
    
    def set_oncomplete(self, oncomplete: Callable):
        self.on_complete = oncomplete


class PopUpWindow(tk.Toplevel):
    """
    Base class for pop-up windows.
    Pop-up windows are not directly linked to the main GUI; but more like a dialogue interface that can be used to
    provide a more detailed explanation without having to flood the GUI with text, or it can be used to get user input,
    for instance.

    A callback method can be passed to the constructor, this method will be called right before the pop-up window is
    destroyed, passing a data dict as arg

    # TODO move to separate module
    """
    def __init__(self, window_title: str = '', window_size: tuple = (300, 300), on_close_callback: Callable = None, **kwargs):
        
        super().__init__(**kwargs)
        self.title(window_title)
        self.geometry(f"{window_size[0]}x{window_size[1]}")
        self.on_close_callback = on_close_callback
        self.data = {}
        
        self.proceed = ug.tk_var(True, 'ThreadActive', master=self)
        self.text = 'Tekst'
        self.var = ug.tk_var(value=self.text, master=self)
        self.label = tk.Label(self, textvariable=self.var, font=('Helvetica', 20))
        self.label.grid(row=0, rowspan=1, columnspan=1, column=0)
        self.button = tk.Button(self, text='Start/stop', command=self.start_counter)
        self.button.grid(row=1, rowspan=1, columnspan=1, column=0)
        self.t0 = 0
        self.t = AsyncTask(self.start_counter, name='Counter', daemon=True)
        self.t.start()
        self.bind('-', self.start_counter)
    
    def start_counter(self, e=None, max_value: int = None):
        """ Start the counter and update the display """
        if self.t0 == 0:
            self.t0 = time.perf_counter()
            time.sleep(.1)
            while max_value is None or time.perf_counter() - self.t0 < max_value:
                t0 = self.t0
                self.text = fmt.delta_t(int(time.perf_counter( ) -t0))
                self.update_label()
                time.sleep(.5)
                
                # If the counter is reset, terminate the thread running the old counter
                if not self.proceed.get():
                    self.proceed.set(True)
                    exit(-1)
            self.t0 = 0
        else:
            self.t0 = 0
            self.proceed.set(False)
            self.t = AsyncTask(self.start_counter, name='Counter', daemon=True)
            self.t.start()
    
    def update_label(self):
        self.var.set(self.text)
        # self.label = tk.Label(self.frame, text=self.text, textvariable=self.var)
        # self.master.after(2000, self.update_label)
    
    @override
    def destroy(self):
        """ Method to execute when the pop-up windows task is fulfilled. Configured callback is invoked + destroy. """
        if self.on_close_callback is not None:
            self.on_close_callback(self.data)
        super().destroy()


class TestPopUp(tk.Toplevel):
    """
    Base class for pop-up windows.
    Pop-up windows are not directly linked to the main GUI; but more like a dialogue interface that can be used to
    provide a more detailed explanation without having to flood the GUI with text, or it can be used to get user input,
    for instance.

    A callback method can be passed to the constructor, this method will be called right before the pop-up window is
    destroyed, passing a data dict as arg

    # TODO move to separate module
    """
    
    def __init__(self, window_title: str = '', window_size: tuple = (300, 300), on_close_callback: Callable = None,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.title(window_title)
        self.frame = ttk.Frame()
        self.geometry(f"{window_size[0]}x{window_size[1]}")
        self.on_close_callback = on_close_callback
        self.data = {}
        self.button = GuiButton(self.frame, command=lambda e: True, width=10, tag='A' ,dims=(0, 0, 5, 3))
        # self.button.
    
    @override
    def destroy(self):
        """ Method to execute when the pop-up windows task is fulfilled. Configured callback is invoked + destroy. """
        if self.on_close_callback is not None:
            self.on_close_callback(self.data)
        super().destroy()


if __name__ == "__main__":
    TestPopUp()