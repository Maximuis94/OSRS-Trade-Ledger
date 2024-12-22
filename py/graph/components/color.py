"""
This module contains a class representation of a color (in the context of the GUI), as well as various pre-defined
colors to use. Furthermore, this module has implementations for generating a set of colors programmatically.


"""
from dataclasses import dataclass
from enum import Enum
from typing import Tuple

from multipledispatch import dispatch

from gui.util.str_formats import strf_float


verify_colors: bool = True
_cur_idx: int = 0


@dataclass(frozen=True, order=True, eq=True, slots=True, match_args=True)
class Rgba:
    """
    Class representation of a Color. A Color has a red, green and blue value that ranges from 0-255, as well as alpha
    that ranges between 0 and 1.
    Colors are static; the values it is composed of cannot be altered once created.
    """
    r: int
    g: int
    b: int
    a: int | float = 1
    
    def __post_init__(self):
        if __debug__:
            t = self.r, self.g, self.b, self.a
            if min(t) < 0 or max(t) > 255 or self.a > 1:
                e = (f"Illegal values found in Rgba {str(self)}\n"
                     f"r, g, b should range from 0-255; a from 0-1")
                raise ValueError(e)
    
    @property
    def hexadecimal(self) -> str:
        """ The hexadecimal representation of this RGB tuple. This does not include alpha. """
        return "#%02x%02x%02x" % (self.r, self.g, self.b)
    
    @property
    def tuple(self) -> Tuple[int, int, int, int | float]:
        """ Return the rgba values as a tuple """
        return self.r, self.g, self.b, self.a
    
    @property
    def plt_tuple(self) -> Tuple[float, float, float, float]:
        """Rgba tuple that can be passed as color in a matplotlib-related context"""
        return self.r/255, self.g/255, self.b/255, float(self.a)
    
    @property
    def rgb(self) -> Tuple[int, int, int]:
        """ Return the rgba values as a tuple """
        return self.r, self.g, self.b
    
    def __getitem__(self, item):
        return self.__getattribute__(self.__match_args__[item])
    
    def __repr__(self):
        return f"(r={self.r}, g={self.g}, b={self.b}, a={strf_float(self.a)})"
    
    def __str__(self):
        return f"(r={self.r}, g={self.g}, b={self.b}, a={strf_float(self.a)})"


########################################################################################################################
#   Pre-defined colors
########################################################################################################################
@dataclass(match_args=True)
class Color(Enum):
    """ A list of pre-defined, commonly used colors. Alpha set to 1 by default; use get() for different alpha values """
    RED = Rgba(255, 0, 0)
    GREEN = Rgba(0, 255, 0)
    BLUE = Rgba(0, 0, 255)
    WHITE = Rgba(255, 255, 255)
    BLACK = Rgba(0, 0, 0)
    YELLOW = Rgba(255, 255, 0)
    CYAN = Rgba(0, 255, 255)
    MAGENTA = Rgba(255, 0, 255)
    BROWN = Rgba(165, 42, 42)
    ORANGE = Rgba(255, 165, 0)
    GRAY = Rgba(128, 128, 128)
    LIGHT_GRAY = Rgba(211, 211, 211)
    DARK_GRAY = Rgba(64, 64, 64)
    PURPLE = Rgba(128, 0, 128)
    VIOLET = Rgba(238, 130, 238)
    INDIGO = Rgba(75, 0, 130)
    GOLD = Rgba(255, 215, 0)
    SILVER = Rgba(192, 192, 192)
    NAVY = Rgba(0, 0, 128)
    TEAL = Rgba(0, 128, 128)
    OLIVE = Rgba(128, 128, 0)
    MAROON = Rgba(128, 0, 0)
    LIME = Rgba(0, 255, 0)
    TURQUOISE = Rgba(64, 224, 208)
    PINK = Rgba(255, 192, 203)
    LIGHT_BLUE = Rgba(173, 216, 230)
    DARK_BLUE = Rgba(0, 0, 139)
    LIGHT_GREEN = Rgba(144, 238, 144)
    DARK_GREEN = Rgba(0, 100, 0)
    LIGHT_YELLOW = Rgba(255, 255, 224)
    DARK_RED = Rgba(139, 0, 0)

    @staticmethod
    @dispatch(str)
    def get(color: str) -> Rgba:
        """ Fetch a Color by passing it as a str, along with a value for Alpha (default=1) """
        return Color.__getitem__(color.upper()).value

    @staticmethod
    @dispatch(Enum)
    def get(color: Enum) -> Rgba:
        """ Fetch a Color by passing it as a pre-defined Color, along with a value for Alpha (default=1) """
        return color.value

    @staticmethod
    @dispatch(str, float)
    def get(color: str, alpha: float) -> Rgba:
        """ Fetch a Color by passing it as a string, along with a value for Alpha (default=1) """
        c = Color.__getitem__(color.upper()).value
        return Rgba(c.r, c.g, c.b, alpha)
    
    @staticmethod
    @dispatch(Enum, float)
    def get(color: Enum, alpha: float) -> Rgba:
        """ Fetch a Color by passing it as a pre-defined Color, along with a value for Alpha (default=1) """
        c = color.value
        return Rgba(c.r, c.g, c.b, alpha)

    @staticmethod
    def get_next_color(auto_reset_order: bool = False) -> Rgba:
        """Returns the next color in line"""
        global _cur_idx
        try:
            c = Color.__getitem__(tuple(Color.__members__.keys())[_cur_idx])
            _cur_idx += 1
        except IndexError as e:
            if auto_reset_order:
                c = Color.__getitem__(tuple(Color.__members__.keys())[0])
                _cur_idx = 1
            else:
                raise e
        return c.value
        