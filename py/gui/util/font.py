"""
This module contains a class representation for Fonts, as well as fonts stylings used throughout the project.

"""
from dataclasses import dataclass
from enum import Enum
from tkinter import font
from typing import Tuple

from gui.util.colors import Rgba, Color


class FontSize(Enum):
    """ Pre-defined Font Sizes """
    H1: int = 36
    H2: int = 24
    H3: int = 18
    LARGE: int = 14
    NORMAL: int = 11
    SMALL: int = 9


class FontFamily(Enum):
    """ Various Font Families one can use """
    HELVETICA = "Helvetica"
    COURIER = "Courier"
    TIMES = "Times"
    CONSOLAS = "Consolas"
    SEGOE_UI = "Segoe UI"


class Font:
    """
    Intermediate class between Python and Tkinter Font. Translates given input into tk fonts, and presents various
    configurations differently. Also allows for coloring the font.
    
    tk.font docs:
        Represents a named font.
    
        Constructor options are:
    
        font -- font specifier (name, system font, or (family, size, style)-tuple)
        name -- name to use for this font configuration (defaults to a unique name)
        exists -- does a named font by this name already exist?
           Creates a new named font if False, points to the existing font if True.
           Raises _tkinter.TclError if the assertion is false.
    
           the following are ignored if font is specified:
    
        family -- font 'family', e.g. Courier, Times, Helvetica
        size -- font size in points
        weight -- font thickness: NORMAL, BOLD
        slant -- font slant: ROMAN, ITALIC
        underline -- font underlining: false (0), true (1)
        overstrike -- font strikeout: false (0), true (1)
    """
    font_size: int
    font_name: str
    is_bold: bool
    is_italic: bool
    
    is_underlined: bool
    is_overstruck: bool
    color: Rgba = Color.BLACK.value
    
    def __init__(self, font_size: int or FontSize = 9, font_family: str or FontFamily = "Consolas",
                 is_italic: bool = False, is_bold: bool = False, is_underlined: bool = False,
                 is_struck_over: bool = False, color: Color or Rgba or Tuple[int, int, int, int or float] = Color.BLACK):
        """
        Creates a Font configuration that is accepted by GuiWidget subclasses. Default args are derived from the font
        that tkinter uses by default. Use Font.tk to access the tk Font instance.
        
        Parameters
        ----------
        font_size : int, optional, 9 by default
            The font size in pixels
        font_family : str or FontFamily, optional, "Consolas" by default
            The font family that will be used. Default is Consolas because it is a monospaced font.
        is_italic : bool, optional, False by default
            If True, fonts are shown with italics
        is_bold : bool, optional, False by default
            If True, fonts are made bold
        is_underlined : bool, optional, False by default
            If True, fonts will be made underlined
        is_struck_over : bool, optional, False by default
            If True, fonts will be struck over
        color : Color or Rgba, optional, Color.BLACK by default
            The color to display the font with. Can be passed as a pre-defined Color, or as a RGBA tuple
        """
        self.font_size: int = font_size if isinstance(font_size, int) else font_size.value
        # """ Font size in pixels. For pre-defined sizes, see the FontSize Enum """
        
        self.font_family: str = font_family if isinstance(font_family, str) else font_family.value
        # """ Name of the font family. For pre-defined families, see the FontFamily Enum. """
        
        self.is_italic: bool = is_italic
        # """ Whether the font is italic """
        
        self.is_bold: bool = is_bold
        # """ Whether the font is bold """
        
        self.is_underlined: bool = is_underlined
        # """ Whether the font is underlined """
        
        self.is_overstruck: bool = is_struck_over
        # """ Boolean indicating whether the text will be  """
        
        if isinstance(color, Color):
            self.color = color.value
        elif isinstance(color, Rgba):
            self.color = color
        else:
            try:
                self.color = Rgba(*color)
            except TypeError:
                raise TypeError("Invalid type passed as color. See gui.util.colors module for Color/Rgba, or pass it as"
                                " a 3- or 4-digit tuple (r, g, b, (a))")
            
    @property
    def tk(self):
        """the tk Font"""
        return font.Font(
            family=self.font_family,
            size=self.font_size,
            slant="italic" if self.is_italic else "roman",
            weight="bold" if self.is_bold else "normal",
            underline=self.is_underlined,
            overstrike=self.is_overstruck
        )
