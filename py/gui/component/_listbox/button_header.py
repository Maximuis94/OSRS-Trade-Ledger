"""
Module with ButtonHeader implementation

"""
from collections.abc import Iterable, Callable
from typing import Tuple, Dict, Any, Optional

from multipledispatch import dispatch

from gui.base.frame import GuiFrame
from gui.component.button import GuiButton
from gui.component.interface.button_header import IButtonHeader
from gui.component.interface.column import IListboxColumn
from gui.component.sort.sort import Sort
from gui.util.constants import letters
from gui.util.generic import Number

_ENTRY_SPACING: int = 1
"""Amount of spaces between two columns"""


_EMPTY_TUPLE: Tuple = tuple([])
"""An empty tuple"""


class ButtonHeader(IButtonHeader):
    """An array of GuiButtons"""
    _parent: GuiFrame
    tag: str
    _button_panel_frame: GuiFrame
    _buttons: Tuple[GuiButton, ...] = _EMPTY_TUPLE
    _n_entry_chars: int = None
    width_scaling: Number
    _on_click: Callable
    _kws: Iterable[Dict[str, Any]] = _EMPTY_TUPLE
    
    def __init__(self, frame: GuiFrame, tag: str, header_button_callback: Callable, columns: Tuple[IListboxColumn, ...]):
        columns = [c for c in columns if c.is_visible]
        if not all([isinstance(c, IListboxColumn) for c in columns]):
            raise TypeError("Error initializing button header; columns should be a list of properly initialized "
                            "ListboxColumns. ")
        self._parent = frame
        self._tag = tag
        
        # self._frame = GuiFrame(frame, [letters[:len(columns)]])
        self._on_click = header_button_callback
        
        self.width_scaling = 1.15  # self.entry_width / self.entry_chars
        self.entry_width = self._n_entry_chars
        
        x, self.header, self.button_ids = 0, " ", {}
        button_grid_ids, column_grid_ids = letters, {}
        self.column_indices = {}
        
        if len(columns) > 0:
            self.add(*columns)
        self.generate_buttons()
    
    def add(self, *columns: IListboxColumn):
        columns = [c for c in columns if c.is_visible]
        self.n_entry_chars(*columns)
        self.entry_width = self._n_entry_chars
        kws = list(self._kws)
        
        for idx, c in enumerate(columns):
            idx += len(self._buttons)
            # w = int(c.width / self._n_entry_chars * self.entry_width * self.width_scaling)
            w = int(c.width / self._n_entry_chars * self.entry_width * self.width_scaling)
            self.header += f"{c.header: ^{c.width}} "
            kws.append({
                "command": self.on_click,
                "column": c,
                "command_kwargs": {"sort": Sort(c.column)},
                "button_text": c.header,
                "width": w,
                "sticky": "WE",
                "tag": letters[idx]
            })
            self.column_indices[c.column] = columns.index(c)
        self._kws = tuple(kws)
    
    def on_click(self, **kwargs):
        self._on_click(**kwargs)
    
    def generate_buttons(self, *columns: IListboxColumn, button_command: Optional[Callable] = None):
        for b in self._buttons:
            b.destroy()
        
        if button_command is not None:
            self._on_click = button_command
        
        if len(columns) > 0:
            self.add(*columns)

        self._button_panel_frame = GuiFrame(self._parent, [letters[:len(tuple(self._kws))]])
        buttons = []
        for kw in self._kws:
            if button_command is not None:
                kw["command"] = self._on_click
                
            b = GuiButton(self._button_panel_frame, **kw)
            column = kw['column']
            key = str(b.info).split('!')[-1].replace('>', '')
            self.button_ids[key] = column.column
            buttons.append(b)
        self._buttons = tuple(buttons)
        self._button_panel_frame.grid(**self.grid_kwargs)
    
    def n_entry_chars(self, *columns: IListboxColumn) -> int:
        """Amount of chars estimated to be in the entry this header is placed above after adding `columns`"""
        self._n_entry_chars = sum([kw['column'].width + _ENTRY_SPACING for kw in self._kws]) - _ENTRY_SPACING
        if len(columns) > 0:
            self._n_entry_chars += sum([(c.width + _ENTRY_SPACING) for c in columns])
        return self._n_entry_chars
    
    def get_button(self, attribute_name: str, attribute_value: any) -> Optional[Tuple[GuiButton, Dict[str, any]]]:
        if len(self._buttons) == 0:
            raise RuntimeError("Unable to get a specific button if no buttons have been generated so far")
        
        for b, kw in zip(self._buttons, self._kws):
            try:
                column = kw['column']
                if column.__getattribute__(attribute_name) == attribute_value:
                    return b, kw
            except AttributeError:
                ...
            
            try:
                if kw[attribute_name] == attribute_value:
                    return b, kw
            except KeyError:
                ...
        return None
    
    def destroy(self):
        for b in self._buttons:
            b.destroy()
        if self._button_panel_frame is not None:
            self._button_panel_frame.destroy()
    
    @property
    def grid_kwargs(self) -> Dict[str, any]:
        """The keyword args that can be used for generating the GuiFrame the buttons will be placed on"""
        return self._parent.get_grid_kwargs(self._tag)
    
    def __iter__(self):
        return iter(self._buttons)
    
    @dispatch(int)
    def __getitem__(self, idx: int) -> GuiButton:
        return self._buttons[idx]
