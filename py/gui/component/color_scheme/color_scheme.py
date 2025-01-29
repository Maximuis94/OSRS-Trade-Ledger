"""
Class representation of a ColorScheme

"""
from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional, final, get_type_hints

from graph.components.color import Rgba
from gui.component.interface.row import IRow


@dataclass(slots=True, frozen=True)
class ColorScheme:
    """
    A ColorScheme class that can help to determine which color to assign to a particular row.
    Although a sequence of ColorSchemes can be applied, which would theoretically modify the color several times, its
    design w.r.t. the priority mechanism is built on the notion that, once a color is determined by a particular color
    scheme, further processing stops and this color is assigned.
    
    A ColorScheme instance can be applied via ColorSchemeInstance.apply(row) or ColorScheme(row)
    
    Examples
    --------
    # Create some function that accepts a Row and derives Rgba values from this Row
    def assign_color(row: IRow) -> Optional[Rgba]:
        return Rgba(max(240, abs(row.price)/196078), 240, 85) if row.price < 0 else
                Rgba(240, max(240, row.price/196078), 85)
    
    color_scheme = ColorScheme(assign_color, 50)
    
    my_row = Row(price=30000000, quantity=120, item=3)
    
    rgba = color_scheme(my_row)
    """
    evaluate_row: Callable[[IRow], Rgba]
    """Method used to evaluate the Rgba score of the Row"""
    
    priority: Optional[int]
    """Priority of the ColorScheme. Higher means it will get processed earlier; None means last."""
    
    @final
    def apply(self, row: IRow) -> Optional[Rgba]:
        """Applies this ColorScheme to the given row. If no color can be determined, it returns None."""
        return self.evaluate_row(row)
    
    @final
    def __call__(self, row: IRow) -> Optional[Rgba]:
        """Applies this ColorScheme to the given row. If no color can be determined, it returns None."""
        return self.evaluate_row(row)


class DefaultColorScheme:
    """
    A DefaultColorScheme is a ColorScheme with a priority score of None. In practice, this translates to the ColorScheme
    being processed as last. Note that this is a skeleton class; calling it instantiates a ColorScheme instance.
    """
    def __new__(cls, *args, **kwargs):
        try:
            if len(args) > 0 and isinstance(args[0], Callable):
                if get_type_hints(args[0])["return"] == Rgba or Optional[Rgba]:
                    return ColorScheme(args[0], None)
                elif kwargs.get("evaluate_row") is None:
                    raise RuntimeError("The first positional arg passed to the DefaultColorScheme constructor is a "
                                       "Callable, but its returned type does not match signature Rgba or "
                                       "Optional[Rgba].")
            return ColorScheme(kwargs["evaluate_row"], None)
        except KeyError:
            raise RuntimeError("The constructor of DefaultColorScheme should be passed a Callable keyword arg "
                               "'evaluate_row' that returns Rgba or Optional[Rgba].")
