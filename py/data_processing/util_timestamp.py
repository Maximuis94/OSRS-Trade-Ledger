"""
Module with utility functions that are timestamp-related
"""
import datetime
from enum import Enum


class WINDOW_SIZE(Enum):
    DAY = 86400
    WEEK = 604800
    HOUR = 3600
    
    def __int__(self) -> int:
        return self.value
    
    
def dt_utc_ts(timestamp: int) -> datetime.datetime:
    """Converts `timestamp` into a datetime.datetime object that aligns with the UTC timezone and returns it"""
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC)


def floor_ts_day(timestamp: int) -> int:
    """Floor `timestamp` to 12am UTC time and return it"""
    return timestamp - timestamp % WINDOW_SIZE.DAY.value


def floor_ts_week(timestamp: int) -> int:
    """Floor `timestamp` to monday 12am UTC time and return it"""
    return timestamp - timestamp % WINDOW_SIZE.WEEK.value - 259200
