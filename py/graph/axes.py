
import time

from math import floor
from matplotlib.axis import XAxis, YAxis
from dataclasses import dataclass
from overrides import override
from collections.abc import Iterable, Callable

from graph.components.axis import IAxis
from graph.components.line import Line

########################################################################################################################
#   x-axes
########################################################################################################################

dp_kwargs = {
    'slots': True,
    'frozen': True,
    'order': True,
    'match_args': True
}


# @dataclass(frozen=True, slots=True, order=True, match_args=True)
class XUnixTimestamp(XAxis, IAxis):
    """x-axis with unix timestamps"""
    timestamp: int or float
    
    def __init__(self, **kwargs):
        
        super().__init__(**kwargs)
        self.set_major_formatter(self._major_format)
    
    def set_axis_span(self, values: Iterable):
        self.axis_span = min(values), max(values)
    
    def set_gridlines(self, small: Line = None, large: Line = None):
        if small is not None:
            self.grid(True, "minor", **small._asdict())
        if large is not None:
            self.grid(True, "major", **large._asdict())
    
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        pass
    
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        pass
    
    def _major_format(self, value: int or float) -> str:
        """ Floor timestamp to hour, then format it """
        return time.strftime('%d-%m-%Y %H:00', time.localtime(int(value - value % 3600)))
    
    def _minor_format(self, value: int or float):
        """ Print timestamp in dd-mm-yy hh:mm:ss format """
        return time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(value))
    
    # def __new__(cls, value):
    #     return IPlotValue(int(value), cls.__str__, cls.__repr__)


@dataclass(frozen=True, slots=True, order=True, match_args=True)
class XAxisDayOfWeek(IAxis):
    """ Day-of-week datapoint value. Typically used on the x-axis, after being derived from a UNIX timestamp """
    timestamp: int or float
    
    @override
    def __post_init__(self):
        """ Sets the underlying value to a UNIX timestamp as an integer. Dow value range will be limited to 0-7. """
        object.__setattr__(self, "_value", int(self.timestamp) % 604800 / 86400)
    
    def __str__(self) -> str:
        """ Return the day-of-week as a floored integer within a string """
        return f"{floor(self.value):.0f}"
    
    def __repr__(self):
        return self.__str__()
    
    def major_format(self) -> str:
        return self.__str__()
    
    def set_axis_span(self, values: Iterable):
        pass
    
    def set_gridlines(self, small: Iterable[int or float, ...] = None, large: Iterable[int or float, ...] = None):
        pass
    
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        pass
    
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        pass


@dataclass(frozen=True, slots=True, order=True, match_args=True)
class XAxisHourOfDay(IAxis):
    """ Hour-of-day datapoint value. Typically used on the x-axis, after being derived from a UNIX timestamp """
    timestamp: int or float
    
    @override
    def __post_init__(self):
        """ Sets the underlying value to a UNIX timestamp as an integer. Dow value range will be limited to 0-24. """
        object.__setattr__(self, "_value", int(self.timestamp) % 86400 / 3600)
    
    def __str__(self) -> str:
        """ Return the hour-of-day as a floored integer within a string """
        return f"{floor(self.value):.0f}"
    
    def __repr__(self):
        return self.__str__()
    
    def major_format(self) -> str:
        return self.__str__()
    
    def set_axis_span(self, values: Iterable):
        pass
    
    def set_gridlines(self, small: Iterable[int or float, ...] = None, large: Iterable[int or float, ...] = None):
        pass
    
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        pass
    
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        pass


########################################################################################################################
#   y-axes
########################################################################################################################
@dataclass(frozen=True, slots=True, order=True, match_args=True)
class YAxisPrice(IAxis):
    """ Price datapoint value. Typically used on the y-axis. Beyond a certain threshold, values are abbreviated using a
    'K', 'M', or 'B' suffix that substitutes the trailing 3, 6 or 9 digits, respectively. """
    price: int
    
    @override
    def __post_init__(self):
        """ Sets the underlying value to a UNIX timestamp as an integer. Dow value range will be limited to 0-7. """
        object.__setattr__(self, "_value", self.price)
    
    def __str__(self) -> str:
        """ Return the day-of-week as a floored integer within a string """
        return f"{floor(self.value):.0f}"
    
    def __repr__(self):
        return self.__str__()
    
    def major_format(self) -> str:
        """ Major price format. Allows for up to two decimals, with a maximum length of 8 """
        return fmt.number(self.price, 2, 8)
    
    def set_axis_span(self, values: Iterable):
        pass
    
    def set_gridlines(self, small: Iterable[int or float, ...] = None, large: Iterable[int or float, ...] = None):
        pass
    
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        pass
    
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        pass


@dataclass(frozen=True, slots=True, order=True, match_args=True)
class YAxisVolume(IAxis):
    """ Volume datapoint value. Typically used on the y-axis. Beyond a certain threshold, values are typically
    abbreviated using a 'K', 'M', or 'B' suffix that subsitutes the trailing 3, 6 or 9 digits, respectively. """
    volume: int
    
    @override
    def __post_init__(self):
        """ Sets the underlying value to a UNIX timestamp as an integer. Dow value range will be limited to 0-7. """
        object.__setattr__(self, "_value", int(self.volume))
    
    def __str__(self) -> str:
        """ Return the day-of-week as a floored integer within a string """
        return f"{floor(self.value):.0f}"
    
    def __repr__(self):
        return self.__str__()
    
    def major_format(self) -> str:
        """ Major price format. Allows for up to two decimals, with a maximum length of 8 """
        return fmt.number(self.volume, 2, 8)
    
    def set_axis_span(self, values: Iterable):
        pass
    
    def set_gridlines(self, small: Iterable[int or float, ...] = None, large: Iterable[int or float, ...] = None):
        pass
    
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        pass
    
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        pass


@dataclass(frozen=True, slots=True, order=True, match_args=True)
class YAxisValue(IAxis):
    """ Value datapoint 'value' that is typically used on the y-axis. It is defined as the product of price and
    quantity and it behaves more or less the same as its counterparts. The expected value range is slightly bigger. """
    price: int
    quantity: int
    
    @override
    def __post_init__(self):
        """ Sets the underlying value to a UNIX timestamp as an integer. Dow value range will be limited to 0-7. """
        object.__setattr__(self, "_value", self.price * self.quantity)
    
    def __str__(self) -> str:
        """ Return the day-of-week as a floored integer within a string """
        return f"{floor(self.value):.0f}"
    
    def __repr__(self):
        return self.__str__()
    
    def major_format(self) -> str:
        """ Major price format. Allows for up to two decimals, with a maximum length of 8 """
        return fmt.number(self._value, 2, 8)
    
    def set_axis_span(self, values: Iterable):
        pass
    
    def set_gridlines(self, small: Iterable[int or float, ...] = None, large: Iterable[int or float, ...] = None):
        pass
    
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        pass
    
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        pass