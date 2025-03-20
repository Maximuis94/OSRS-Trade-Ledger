"""
Module with enums used throughout the project

"""
from enum import Enum


class Src(Enum):
    """Encoding of the src column values used in the timeseries database"""
    WIKI = 0
    AVG5M_BUY = 1
    AVG5M_SELL = 2
    REALTIME_BUY = 3
    REALTIME_SELL = 4
