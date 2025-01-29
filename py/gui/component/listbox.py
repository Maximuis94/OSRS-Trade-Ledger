"""
This module contains the implementation of the GuiListbox

"""
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Callable, Sequence, Iterable
from typing import List, Tuple, Optional

from gui.base.frame import GuiFrame
from gui.component._listbox.button_header import ButtonHeader
from gui.component._listbox.entry_manager import ListboxEntries
from gui.component._listbox.interfaces import SortLike
from gui.component._listbox.row import ListboxRow
from gui.component.interface.button_header import IButtonHeader
from gui.component.interface.column import IListboxColumn
from gui.component.interface.filter import IFilter, IFilterable
from gui.component.interface.listbox import IListbox
from gui.component.interface.row import IRow
from gui.component.interface.sort import ISortSequence, ISortable
from gui.component.label import GuiLabel
from gui.component.sort.sort import Sorts
from gui.util.colors import Rgba
from gui.util.decorators import DocstringInheritor
from gui.util.generic import SupportsGetItem


_STYLE_ID = "style.GuiListbox"


def _make_button_header(gui_listbox, tag: str):
    if gui_listbox._button_header is not None:
        gui_listbox._button_header.destroy()
    
    button_header = ButtonHeader(gui_listbox, tag, gui_listbox.header_button_sort, gui_listbox._columns)
    return button_header


def _remove_entry_slice(rows: Iterable[IRow], start: int = None, end: int = None):
    """Remove the slice of entries determined using `start` and `end`"""
    if start is None and end is None:
        return []
    else:
        rows = list(rows)
        if start is None:
            return rows[end:]
        elif end is None:
            return rows[:start]
        else:
            return rows[:start] + rows[end:]


@DocstringInheritor.inherit_docstrings
class GuiListbox(GuiFrame, IListbox, ISortable, IFilterable):
    """
    Basic Listbox class that is primarily designed to provide a Listbox with minimal extras. That is, its complexity is
    determined by an external controller class. It does not necessarily do much on its own.
    """
    
    _columns: Tuple[IListboxColumn, ...]
    _button_header: Optional[IButtonHeader] = None
    _listbox: tk.Listbox
    _scrollbar = ttk.Scrollbar
    _bottom_label: GuiLabel
    _entries: ListboxEntries
    _bottom_label_text: Optional[tk.StringVar] = None
    _active_filters: Optional[IFilter] = None
    _active_sorts: Optional[ISortSequence] = None
    _color_scheme: Callable[[IRow], Rgba] = None
    _set_row_bgc: bool = True
    _submitted_entries: List[IRow]
    _onclick_row: Callable
    _style: ttk.Style
    
    def __init__(self, frame: GuiFrame, tag: str, entries: List[dict or tuple], entry_width=20, listbox_height=10,
                 sticky='N', event_bindings=None, select_mode=tk.SINGLE, columns: Sequence[IListboxColumn] = None,
                 onclick_row: Callable[[SupportsGetItem], any] = None, **kwargs):
        """Basic Listbox class. It consists of a button header, a listbox, a scrollbar and a bottom label.
        
        
        Parameters
        ----------
        frame : ttk.Frame
            The ttk frame on which the Label will be placed.

        df : pandas.DataFrame
            DataFrame that will be used to draw entries from

        header_button_callback : callable
            Callback method to use if one of the header buttons is pressed

        top_label_text : str
            String to print above the listbox

        bottom_label_text : str
            String to print below the listbox

        columns : list
            A list of ListboxColumns that dictates how the rows should be formatted. The used columns should have a
            reference (`df_column`) to an existing pandas DataFrame column used as df.

        default_sort : tuple
            A tuple indicating the sorting method to use by default. The first element refers to the index of the
            ListboxColumn to use as sorting value, the second one is a flag indicating an ascending sorting order or not

        onclick_row : Callable[[ListboxEntry], any], optional
            A command to execute whenever a row is clicked. The clicked row will be passed as arg.



        Attributes
        ----------
        frame : ttk.Frame
            Frame on which the object will be placed

        Methods
        -------
        set_text(string)
            Method for changing the text displayed by the label
        """
        
        self.init_widget_start(frame=frame, tag=tag, **{k: v for k, v in kwargs.items() if k != 'text'})
        self._columns = tuple(columns)
        width = entry_width + 1
        
        # self._style = ttk.Style()
        # self._style.configure(_STYLE_ID, borderwidth=kwargs.pop('borderwidth', 5), relief=kwargs.pop('relief', "ridge"))
        
        super().__init__(frame, ["A"*entry_width+"*", "B"*entry_width+"C", "D"*width], relief="ridge")#, style=_STYLE_ID)
        
        if isinstance(entries[0], dict):
            self._entries = ListboxEntries(
                [ListboxRow({c.id: entry[c.column] for c in self._columns}) for entry in entries],
                listbox_columns=self._columns, insert_subset=self.fill_listbox, **kwargs)
        else:
            self._entries = ListboxEntries([ListboxRow(e) for e in entries], listbox_columns=columns,
                                           insert_subset=self.fill_listbox, **kwargs)
        
        self._make_button_header()
        
        self._scrollbar = ttk.Scrollbar(self.frame, orient="vertical")
        self._scrollbar.grid(row=1, column=entry_width, sticky="NS")
        
        self._listbox = tk.Listbox(self.frame, width=entry_width, height=listbox_height, selectmode=select_mode,
                                   yscrollcommand=self._scrollbar.set, font=self._entries.font.tk)
        self._listbox.grid(row=1, column=0, columnspan=entry_width, sticky="NWE")
        self._scrollbar.config(command=self._listbox.yview)
        
        self._bottom_label_text = tk.StringVar(self.frame)
        self._bottom_label = GuiLabel(self, text_variable=self._bottom_label_text, tag='D', padxy=(0, 0), sticky=sticky)
        self._bottom_label.grid(row=2, column=0, columnspan=width, sticky='WE')
        
        self._submitted_entries = []
        
        self.fill_listbox()
        
        if event_bindings is not None:
            for binding in event_bindings:
                self._listbox.bind(binding[0], binding[1])
        self.row_click = onclick_row
    
    def insert(self, *rows: IRow, index: Optional[int] = None):
        if index is None:
            index = len(self._submitted_entries)
        for idx, row in enumerate(rows):
            try:
                idx += index
                r = str(row)
                self._listbox.insert(idx, r)
                self._submitted_entries.append(row)
                if self._set_row_bgc:
                    self._listbox.itemconfig(idx, bg=self.get_bgc(row))
            except AttributeError as e:
                print('Attribute Error', idx, row)
                raise e
    
    def add(self, rows: Iterable[IRow], extend: bool = True):
        if extend:
            self._entries.all = list(self._entries.all) + list(rows)
        else:
            self._entries.all = list(rows)
    
    def fill_listbox(self, rows: Iterable[IRow] = None, filters: Optional[IFilter] = None, extend: bool = True,
                     sorts: Optional[Sorts] = None, is_header_button_callback: bool = False):
        if not extend:
            self.clear_listbox()
        if rows is not None:
            if not extend:
                self._entries.all = rows
            else:
                self._entries.all += list(rows)
        
        rows = self._entries.apply_configurations(sort_by=self._active_sorts if sorts is None else sorts,
                                                  filters=self._active_filters if filters is None else filters,
                                                  header_callback=is_header_button_callback)
        
        for r in rows:
            self.insert(r)
    
    def refresh_listbox(self):
        self.clear_listbox()
        
        for row in self._entries.apply_configurations():
            self.insert(row)
            
    def clear_listbox(self, start: Optional[int] = None, end: Optional[int] = None):
        """CLear the listbox. By default, it removes all entries. Alternatively, start and end row index can be given"""
        self._listbox.delete(0 if start is None else start, tk.END if end is None else end)
        self._entries.subset = _remove_entry_slice(self._entries.subset, start, end)
    
    def get_bgc(self, entry: IRow) -> Optional[str]:
        if self._color_scheme is None:
            return None
        return self.color_scheme(entry).hexadecimal
    
    @property
    def row_click(self) -> Callable[[IRow], ...]:
        """The action that is executed whenever a row is clicked."""
        return self._row_click
    
    @row_click.setter
    def row_click(self, onclick_row: Callable[[IRow], ...]):
        self._listbox.unbind("<<ListboxSelect>>")
        self._onclick_row = onclick_row
        self._listbox.bind("<<ListboxSelect>>", self._row_click)
    
    @property
    def frame(self) -> ttk.Frame:
        """The ttk.Frame that holds the widgets of this class"""
        return self._frame
    
    @property
    def color_scheme(self):
        """The color scheme that is applied to ListboxEntries"""
        return self._color_scheme
    
    @color_scheme.setter
    def color_scheme(self, color_scheme: Callable[[IRow], Rgba]):
        self._color_scheme = color_scheme
    
    @property
    def bottom_label(self) -> str:
        """Value of the tk.StringVar of the label at the bottom of the listbox"""
        return self._bottom_label_text.get()
    
    @bottom_label.setter
    def bottom_label(self, string):
        self._bottom_label_text.set(string)
    
    @property
    def index(self) -> int:
        """Index of the currently selected row"""
        idx = self._listbox.curselection()[0]
        return idx if isinstance(idx, int) else idx[0]

    def header_button_sort(self, sort: Sorts):
        """Apply the sort(s) in `sort` and fill the Listbox sorted as such"""
        self.fill_listbox(sorts=sort, is_header_button_callback=True, extend=False)
    
    def _make_button_header(self):
        """Create the button header. Each visible column is assigned a button"""
        self._button_header = _make_button_header(self, "A")
    
    def _row_click(self, event):
        """Executes whenever a row within the listbox is clicked. """
        if self._onclick_row is not None:
            idx = self.index
            entry = self._entries.subset[idx]
            self._onclick_row(**{"index": idx, "entry": entry, "event": event})
    
    @property
    def default_sort(self) -> Sorts:
        """The sorting sequence that is applied by default"""
        return self._entries.default_sort_sequence
    
    @default_sort.setter
    def default_sort(self, sort_by: Optional[SortLike] = None):
        self._entries.initial_sort(sort_by)
    
    @property
    def n_entries(self) -> int:
        """Return the amount of entries in this Listbox"""
        return len(self._entries)
    
    def sort(self, sort_by: ISortSequence) -> Tuple[IRow, ...]:
        return self._entries.sort(sort_by)
    
    def filter(self, *filters: IFilter) -> Tuple[IRow, ...]:
        return self._entries.filter(filters)
    
    def __getitem__(self, index: int) -> IRow:
        return self._submitted_entries[index]
    
    def __len__(self) -> int:
        return len(self._entries)


if __name__ == '__main__':
    row = list(range(20))
    print(row)
    row = _remove_entry_slice(row, 5, 15)
    print(row)
    
    # import global_variables.path as gp
    # db = Database(gp.f_db_local)
    # rows = db.execute("""SELECT * FROM "transaction" WHERE item_id < 100""", factory=dict).fetchall()
    # df = pd.DataFrame(rows)
    # df.to_pickle(os.path.join(gp.dir_data, "test_df.dat"))
