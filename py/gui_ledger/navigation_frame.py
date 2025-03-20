"""
This module contains the implementation of the NavigationFrame, which is the frame on the far left with various buttons.
The buttons on this frame can be used to switch between various frames, as well as initiate certain actions.

The buttons are placed in a column on the far left. The NavigationFrame consists of a label and a series of buttons.
The label indicates the current 'mode' the GUI is in

Since this is the realization of the actual GUI, classes are hard-coded.
"""
from collections.abc import Callable
from typing import Tuple, Optional

from gui.base.frame import GuiFrame, TkGrid
from gui.component.button import GuiButton
from gui.component.label import GuiLabel
from gui.frame.button_array import ButtonArray
from gui.util.constants import letters, Alignment
from gui.util.font import Font, FontSize, FontFamily

_LABEL_TAG: str = "L"
"""Tag assigned to the label widget"""


_BUTTON_TAG: str = "B"
"""Tag assigned to the ButtonArray"""


_CLOSE_BUTTON_TAG: str = "X"
"""Tag assigned to the Close Button"""


_LABEL_FONT: Font = Font(font_size=FontSize.H3, font_family=FontFamily.SEGOE_UI, is_bold=True)
"""The font used for the label at the top of the buttons"""


def _grid_layout(n_buttons: int) -> TkGrid:
    """Returns a TkGrid instance for `n_buttons`. Buttons are assigned letters A-Z, the label '_'"""
    return TkGrid([_LABEL_TAG, '*'] + [letters[:n_buttons]])
    

class NavigationFrame(GuiFrame):
    """
    The navigation Frame is the frame on the left with the label and buttons.
    It consists of one widget and multiple buttons. The buttons are primarily used to control what the other
    frames are displaying.
    
    TODO
        1. Implement GuiButton DONE
        2. Implement GuiLabel DONE
        3. Define label and buttons
        4. Add underlying behaviour
    """
    top_label: GuiLabel
    """Label at the top of the frame"""
    
    button_column: ButtonArray
    """Column of buttons used to navigate with"""
    
    close_button: GuiButton
    """Button used to close the interface at the very bottom of the frame."""
    
    button_names: Tuple[str, ...] = "Inventory", "Results/day", "Item prices", "Overall results", "Import data"
    
    width: int = 30
    """The width of the buttons in the NavigationFrame"""
    
    max_label_length: Optional[int] = 25
    """Maximum length of the label"""
    
    def __init__(self, parent: GuiFrame, grid_kwargs=None, button_callback: Callable = None, close_button_callback: Callable = None, **kwargs):
        kw = {'frame': self, 'width': self.width, 'padxy': (5, 7), "top_label_text": "TOP LABEL TEXT",
              "bottom_label_text": "BOTTOM LABEL TEXT",
              'font': ('Helvetica', '12'), 'sticky': 'WE'}
        
        _grid = ([_LABEL_TAG, '*'] + [_BUTTON_TAG for _ in range(len(self.button_names))] +
                 ["*"] + ["*" if close_button_callback is None else _CLOSE_BUTTON_TAG])
        self.max_label_length = kwargs.pop('max_text_length', self.max_label_length)
        super().__init__(parent, grid_layout=TkGrid(_grid), **kwargs)
        self.configure()

        self.top_label = GuiLabel(self, _LABEL_TAG, text="TEST", font=_LABEL_FONT,
                                  max_text_length=self.max_label_length, sticky="E")
        
        self.setup_close_button(close_button_callback)
        
        self.button_column = ButtonArray(self, tag=_BUTTON_TAG, width=self.width,
                                         commands={button_name: button_callback for button_name in self.button_names},
                                         alignment=Alignment.VERTICAL)
        
        if grid_kwargs is not None:
            self.grid(**grid_kwargs)
    
    @property
    def label_text(self):
        """Text displayed in the top label."""
        return self.top_label.text
    
    @label_text.setter
    def label_text(self, text: str):
        self.top_label.text = text
    
    def setup_close_button(self, callback: Optional[Callable]):
        """Sets up the close button, but only if `callback` is not None and the Frame is in initialization."""
        if callback is not None and not hasattr(self, "button_column"):
            self.close_button = GuiButton(self, _CLOSE_BUTTON_TAG, callback, button_text="Close GUI", width=self.width,
                                          padx=5, pady=10, sticky="SWE")
        ...
