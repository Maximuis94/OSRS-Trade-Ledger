import tkinter as tk


class GuiRadiobutton(tk.Radiobutton):
    def __init__(self, frame, variable, value, command=None, text='', xy=(0, 0), wh=(1, 1), padxy=(0, 0), sticky='N',
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
        self.status = variable
        self.text = textvariable
        self.text.set(text)
        super().__init__(self.frame, textvariable=self.text, value=value, variable=self.status, command=command)
        for binding in event_bindings:
            self.bind(binding[0], binding[1])
        self.grid(row=xy[1], rowspan=wh[1], column=xy[0], columnspan=wh[0], padx=padxy[0], pady=padxy[1], sticky=sticky)
