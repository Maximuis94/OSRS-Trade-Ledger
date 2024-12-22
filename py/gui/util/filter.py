"""
Module with Filter logic

"""
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class ArithmeticFilters(Enum):
    """Arithmetic filter operations; correspond with dunder methods"""
    GT = lambda threshold_value, value: value > threshold_value
    LT = lambda threshold_value, value: value < threshold_value
    GE = lambda threshold_value, value: value >= threshold_value
    LE = lambda threshold_value, value: value <= threshold_value
    EQ = lambda threshold_value, value: value == threshold_value
    NE = lambda threshold_value, value: value != threshold_value


class StringFilters(Enum):
    """String filter operations; correspond with builtin/dunder methods"""
    HAS = lambda threshold_value, value: threshold_value in value
    HASNOT = lambda threshold_value, value: threshold_value not in value
    EQ = lambda threshold_value, value: value == threshold_value
    NE = lambda threshold_value, value: value != threshold_value
    STARTSWITH = lambda threshold_value, value: value.startswith(threshold_value)
    ENDSWITH = lambda threshold_value, value: value.endswith(threshold_value)


@dataclass(slots=True, match_args=True, frozen=True, eq=True, order=True)
class Filter:
    """Filter dataclass used for filtering Entries"""
    
    attribute_name: str
    """The name of the attribute that is to be filtered"""
    
    function: Callable[[int or float or str, int or float or str], bool] or ArithmeticFilters or StringFilters
    """Filter function to apply; its args should be attribute_name, threshold_value, value"""
    
    threshold_value: int or float or str
    """Threshold value applied to the filter"""
    
    def __call__(self, entry) -> bool:
        if isinstance(self.function, (StringFilters, ArithmeticFilters)):
            return self.function.value(self.threshold_value, entry.values.get(self.attribute_name))
        else:
            return self.function(self.threshold_value, entry.values.get(self.attribute_name))
    