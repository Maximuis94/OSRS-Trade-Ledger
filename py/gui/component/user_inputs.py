"""
This module contains various widgets related to textual user input; entry, combobox,

"""


# The text in the entry field can be modified or acquired with self.text.set(str) or self.text.get()
class GuiEntry(tk.Entry):
    def __init__(self, frame, text='', width=10, xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', textvariable=None,
                 event_bindings=None, grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, width=width)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)


# The text in the combobox can be modified or acquired with self.text.set(str) or self.text.get()
class GuiCombobox(ttk.Combobox):
    def __init__(self, frame, values, text='', width=10, xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N',
                 textvariable=None, event_bindings=None, grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, width=width)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self['values'] = list(values)
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)

    def update_values(self, values):
        self['values'] = values
