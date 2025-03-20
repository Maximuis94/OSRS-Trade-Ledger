"""
Module: gui_tooltip.py
======================
This module provides a mixin class for adding tooltip functionality to GUI widgets.

The GuiTooltip mixin can be integrated with any widget that exposes its underlying
Tkinter widget via the attribute `tk_widget`. It enables displaying a tooltip after
a specified delay when the mouse hovers over the widget and hides it upon movement
or when the mouse leaves the widget.

Classes
-------
GuiTooltip
    A mixin class that provides tooltip functionality to GUI widgets.
"""

import tkinter as tk
from typing import Optional, Tuple, List, Callable, Dict

from gui.base.frame import GuiFrame
from gui.base.widget import GuiWidget
from gui.util.font import Font
from gui.util.constants import tooltip_font


class GuiTooltip:
    """
    Mixin class that adds tooltip functionality to a widget.

    When the mouse hovers over a widget that uses this mixin, after a delay a tooltip
    window is displayed near the widget. If the mouse moves or leaves the widget, the
    tooltip is hidden. Additionally, the widget's appearance may change while the
    tooltip is active.

    Attributes
    ----------
    _tooltip_text : Optional[str]
        The text to display in the tooltip.
    _tooltip_delay : int
        The delay in milliseconds before showing the tooltip.
    _tooltip_highlight : str
        The background color to apply to the widget when the tooltip is active.
    _tooltip_window : Optional[tk.Toplevel]
        The Toplevel window used to display the tooltip.
    _tooltip_after_id : Optional[str]
        The identifier for the scheduled tooltip display callback.
    _original_bg : Optional[str]
        The original background color of the widget, used for restoring after tooltip hides.

    Methods
    -------
    set_tooltip(tooltip: str, delay: int = 500, highlight_color: str = "lightyellow")
        Configure and activate tooltip functionality for the widget.
    """
    
    __slots__ = ("_tooltip_text", "_tooltip_delay", "_tooltip_highlight",
                 "_tooltip_window", "_tooltip_after_id", "_original_bg")
    tk_widget: GuiWidget
    frame: Optional[GuiFrame] = None
    tag: str = ""
    event_bindings: List[Tuple[str, Callable]] = []
    padx: int = 0
    pady: int = 0
    sticky: str = 'N'
    _text: Optional[tk.StringVar] = None
    _tk_vars: Dict[str, tk.Variable] = {}
    font: Font = tooltip_font
    _max_length: Optional[int] = None
    _tooltip_text: str
    _tooltip_delay: int
    _tooltip_highlight: str
    _tooltip_window: tk.Toplevel
    _tooltip_after_id: str
    _original_bg: str
    
    def set_tooltip(self, tooltip: str, delay: int = 500, highlight_color: str = "lightyellow") -> None:
        """
        Configure the tooltip for the widget.

        When the mouse hovers over the widget and remains still for the specified delay,
        a tooltip will appear. The widget's background will change to the highlight color
        while the tooltip is visible.

        Parameters
        ----------
        tooltip : str
            The text to display in the tooltip.
        delay : int, optional
            The delay (in milliseconds) before showing the tooltip. Default is 500.
        highlight_color : str, optional
            The background color to apply to the widget when the tooltip is active.
            Default is "lightyellow".
        """
        self._tooltip_text = tooltip
        self._tooltip_delay = delay
        self._tooltip_highlight = highlight_color
        self._tooltip_window = None
        self._tooltip_after_id = None
        self._original_bg = None
        
        # Bind necessary events to the underlying widget.
        self.tk_widget.bind("<Enter>", self._on_enter)
        self.tk_widget.bind("<Leave>", self._on_leave)
        self.tk_widget.bind("<Motion>", self._on_motion)
    
    def _on_enter(self, event: tk.Event) -> None:
        """Schedule tooltip display when the mouse enters the widget."""
        # Save original background to restore later.
        self._original_bg = self.tk_widget.cget("background")
        # Schedule tooltip display after the specified delay.
        self._tooltip_after_id = self.tk_widget.after(self._tooltip_delay, self._show_tooltip)
    
    def _on_motion(self, event: tk.Event) -> None:
        """Cancel tooltip display if the mouse moves, or hide the tooltip if already shown."""
        if self._tooltip_after_id:
            self.tk_widget.after_cancel(self._tooltip_after_id)
            self._tooltip_after_id = None
        if self._tooltip_window:
            self._hide_tooltip()
    
    def _on_leave(self, event: tk.Event) -> None:
        """Cancel any scheduled tooltip display and hide the tooltip when the mouse leaves."""
        if self._tooltip_after_id:
            self.tk_widget.after_cancel(self._tooltip_after_id)
            self._tooltip_after_id = None
        self._hide_tooltip()
    
    def _show_tooltip(self) -> None:
        """Display the tooltip in a small Toplevel window and change the widget's appearance."""
        if not self._tooltip_text:
            return
        # Change the widget's background to the highlight color.
        self.tk_widget.configure(background=self._tooltip_highlight)
        # Create a borderless Toplevel window for the tooltip.
        self._tooltip_window = tw = tk.Toplevel(self.tk_widget)
        tw.wm_overrideredirect(True)
        # Position the tooltip near the widget.
        x = self.tk_widget.winfo_rootx() + 20
        y = self.tk_widget.winfo_rooty() + self.tk_widget.winfo_height() + 5
        tw.wm_geometry(f"+{x}+{y}")
        # Add a label inside the Toplevel to display the tooltip text.
        label = tk.Label(tw, text=self._tooltip_text, justify=tk.LEFT,
                         background=self._tooltip_highlight, relief=tk.SOLID, borderwidth=1,
                         font=("tahoma", 8, "normal"))
        label.pack(ipadx=1)
    
    def _hide_tooltip(self) -> None:
        """Destroy the tooltip window and restore the widget's original appearance."""
        if self._tooltip_window:
            self._tooltip_window.destroy()
            self._tooltip_window = None
        if self._original_bg is not None:
            self.tk_widget.configure(background=self._original_bg)
