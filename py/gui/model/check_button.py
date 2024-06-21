import tkinter as tk


class GuiCheckbutton(tk.Checkbutton):
    def __init__(self, frame, variable=None, command=None, text='', initial_state=None,
                 xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N', textvariable=None, event_bindings=None,
                 grid: list or tuple = None):
        if grid is not None:
            xy, wh = grid
            assert len(xy) == 2 and len(wh) == 2
        self.frame = frame
        if variable is None:
            self.status = tk.BooleanVar()
            self.status.set(True)
        else:
            self.status = variable
        if initial_state is not None:
            self.status.set(initial_state)
        if not isinstance(event_bindings, list) and event_bindings is not None:
            event_bindings = [event_bindings]
        elif event_bindings is None:
            event_bindings = []
        if not isinstance(textvariable, tk.StringVar):
            textvariable = tk.StringVar(self.frame)
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, variable=self.status, onvalue=True, offvalue=False)
        if command is not None:
            self.bind('<Button-1>', command)
        for binding in event_bindings:
            # print(binding)
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)
    
    def get(self):
        return self.status.get()
    
    def set(self, new_status: bool):
        self.status.set(new_status)
