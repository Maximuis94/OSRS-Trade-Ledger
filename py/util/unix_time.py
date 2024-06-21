"""
Import as 'import util.unix as ut'
Module with time-related utility methods like UNIX-datetime conversions.

UNIX timestamp is the amount of seconds passed since 1-1-1970 00:00 (UTC), it is typically represented as a float.

Methods in this module have been tested, compared and optimized.

Due to the small difference between UTC/local time and the OSRS server being synced with UTC, UTC time is the preferred
datetime representation.
"""
import datetime
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Tuple

# datetime.datetime objects at UNIX=0.0
utc_0 = datetime.datetime.utcnow()-datetime.timedelta(seconds=time.time())
loc_0 = datetime.datetime.now()-datetime.timedelta(seconds=time.time())

# Difference of local time relative to utc time in seconds and hours as integers
delta_s_loc_utc = (loc_0-utc_0).seconds
delta_h_loc_utc = round(delta_s_loc_utc / 3600)


def loc_dtn() -> datetime.datetime:
    """ Return the current datetime.datetime of the local timezone """
    return datetime.datetime.now()


def loc_unix_dt(timestamp: (int or float)) -> datetime.datetime:
    """ Convert the given UNIX timestamp to a local timezone datetime.datetime """
    return datetime.datetime.fromtimestamp(timestamp)


def loc_dt_unix(dt: datetime.datetime) -> float:
    """ Convert a local timezone datetime.datetime to a UNIX timestamp """
    return (dt-loc_0).total_seconds()


def utc_dtn() -> datetime.datetime:
    """ Return the current utc datetime.datetime """
    return datetime.datetime.utcnow()


def utc_unix_dt(timestamp: (int or float)) -> datetime.datetime:
    """ Convert the given UNIX timestamp to a UTC datetime.datetime """
    return datetime.datetime.utcfromtimestamp(timestamp)


def utc_dt_unix(dt: datetime.datetime) -> float:
    """ Convert a UTC datetime.datetime to a UNIX timestamp """
    return (dt-utc_0).total_seconds()


def extract_day_utc(ts: (int, float) = time.time()) -> Tuple[int, int]:
    """ Given unix timestamp `ts`, extract the surrounding day in UTC time """
    t0 = int(ts - ts % 86400)
    return t0, t0 + 86399


def extract_day_loc(ts: (int, float) = time.time()) -> Tuple[int, int]:
    """ Given unix timestamp `ts`, extract the surrounding day in UTC time """
    return extract_day_utc(ts+delta_s_loc_utc)


@dataclass(order=False, frozen=False, match_args=True)
class Timestamp:
    """
    Immutable class representation of a unix timestamp with both local and utc times.
    Allows for easy access to both local and utc datetime, derived from `ts`.
    
    Instances of Timestamp should be created via unix_time.timestamp(ts), as the class itself is
    immutable.
    
    The idea behind this class is to facilitate utc / local timezone conversions and to minimize the risk on confusing
    one with another.
    
    Note that the attributes of this class, or mostly the hour attribute, is expressed as UTC time. This is also the
    default timezone that is used. The attributes are frequently used attributes throughout the project.
    
    Methods
    -------
    utc() -> datetime.datetime:
        Convert this timestamp to a utc datetime.datetime object and return it.
        
    local() -> datetime.datetime:
        Convert this timestamp to a local datetime.datetime object and return it.
    """
    unix: int
    s: int
    m: int
    h: int
    day: int
    month: int
    year: int
    dow: int
    
    def utc(self) -> datetime.datetime:
        """ Return the utc datetime.datetime for this timestamp """
        return utc_unix_dt(self.unix)
    
    def local(self) -> datetime.datetime:
        """ Return the local datetime.datetime for this timestamp """
        return loc_unix_dt(self.unix)
    
    def __repr__(self):
        return f'{self.day:0>2}-{self.month:0>2}-{str(self.year)[-2:]} {self.h:0>2}:{self.m:0>2}:{self.s:0>2}'


def timestamp(ts: int) -> Timestamp:
    """ Generate a Timestamp from unix timestamp `timestamp` """
    dt = utc_unix_dt(ts)
    return Timestamp(ts, dt.second, dt.minute, dt.hour, dt.day, dt.month, dt.year, dt.weekday())


if __name__ == '__main__':
    n = pow(10, 9)
    _ts = int(time.time())
    t_ = time.perf_counter_ns()
    for _ in range(1000000):
        timestamp(_ts)
    print((time.perf_counter_ns()-t_)/n)
    print(timestamp(_ts))
    
    t_ = time.perf_counter_ns()
    for _ in range(1000000):
        datetime.datetime.utcfromtimestamp(_ts)
    print((time.perf_counter_ns()-t_)/n)
    # for a in Timestamp.__match_args__[1:]:
    #     arg = dt.__getattribute__(a)
    #     if not isinstance(arg, Callable):
    #         print(a, arg)
    #     else:
    #         print(a, arg())
    # print(dt.timestamp())

    # ts = timestamp(int(time.time()))
    # print(ts)
    