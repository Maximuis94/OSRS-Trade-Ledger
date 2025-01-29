"""
Module with the implementation for a set of buttons

"""
from collections.abc import Sequence, Callable
from typing import Tuple, Dict, Optional, Literal

from gui.base.frame import GuiFrame, TkGrid
from gui.component.button import GuiButton
from gui.util.constants import letters, Alignment


def _get_grid(n: int, alignment: Literal[Alignment.HORIZONTAL, Alignment.VERTICAL]) -> TkGrid:
    """Generate a TkGrid for `n` widgets that is aligned horizontally or vertically, depending on `alignment`"""
    return TkGrid([letters[:n]]) if alignment == Alignment.HORIZONTAL else TkGrid([c for c in letters[:n]])


class ButtonArray:
    """A class for defining an n-sized button array"""
    _frame: GuiFrame
    buttons: Tuple[GuiButton, ...]
    
    def __init__(self, frame, alignment: Literal[Alignment.HORIZONTAL, Alignment.VERTICAL], tag: str,
                 commands: Dict[str, Callable],
                 button_specific_kwargs: Optional[Sequence[Dict[str, any]]] = None,
                 common_kwargs: Optional[Dict[str, any]] = None, **kwargs):
        """
        Initialize the button array. All buttons are initialized with `common_kwargs`.
        This can be specified per Button via `button_specific_kwargs`. Since buttons typically have to display text,
        a Sequence of strings, `button_texts`, is a required arg.
        Passing any of the following kwargs will have no effect; 'tag', 'button_text', 'frame', 'command', 'sticky'.
        
        Parameters
        ----------
        frame : GuiFrame
            The parent Frame on which the array should be placed
        alignment : Literal[Alignment.HORIZONTAL, Alignment.VERTICAL]
            Orientation of the array; horizontal will yield a row, whereas vertical will yield a column
        tag : str
            The tag to use when fetching grid() parameters.
        commands : Dict[str, Callable]
            A dict with key, command pairs. Each key will be set as button text and each command for that pair will be
            assigned to that button
        button_specific_kwargs : Optional[Sequence[Dict[str, any]]], None by default
            Keyword args that are passed to specific buttons
        common_kwargs : Optional[Dict[str, any]], None by default
            Keyword args that are passed to all buttons
        """
        self._frame = GuiFrame(frame, _get_grid(len(commands), alignment), tag=tag)
        
        if common_kwargs is not None:
            common_kwargs['width'] = kwargs.pop('width', 20)
            common_kwargs['sticky'] = "WE"
        else:
            common_kwargs = {"width": kwargs.pop('width', 20), "sticky": "WE"}
        
        kws = [
            {"tag": tag, "button_text": text, "command": command, "frame": self._frame, "command_kwargs": {"button_id": text}}
            for tag, text, command in zip([c for c in letters], [k for k in commands], [commands[k] for k in commands])
        ]
        self.buttons = tuple([GuiButton(**{**common_kwargs, **kw}) for kw in kws])
        self._frame.grid(**frame.get_grid_kwargs(self._frame.tag))
    
    
        