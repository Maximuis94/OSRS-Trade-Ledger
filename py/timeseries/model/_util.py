"""
Atomic classes used by any form of Timeseries data.
All of the classes listed here are NamedTuple classes;
- immutable
- memory-efficient


"""
from dataclasses import dataclass

import time
from typing import List, Tuple, NamedTuple, Any

import datetime


delta_local = time.localtime(time.time())[3] - time.gmtime(time.time())[3]
print(delta_local)
print(datetime.datetime.fromtimestamp(time.time()))

_offset = int(round(time.localtime()[3] - time.gmtime()[3], 0))


class StructTime(NamedTuple):
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int


class _TimeStamp(NamedTuple):
    unix: int
    """UNIX timestamp"""
    
    utc: Any
    """UTC time struct"""
    
    loc: Any
    """Local time struct"""
    
    @property
    def local_utc_offset(self):
        """Offset in seconds between local and UTC time"""
        return _offset


@dataclass
class Timestamp:
    def __new__(cls, timestamp: int) -> _TimeStamp:
        ts = int(timestamp)
        return _TimeStamp(ts, time.gmtime(ts), time.localtime(ts))
    
    @property
    def unix(self) -> int:
        """Local time struct"""
        ...
    
    @property
    def loc(self) -> StructTime:
        """Local time struct"""
        ...

    @property
    def utc(self) -> StructTime:
        """UTC time struct"""
        ...
    
    @property
    def local_utc_offset(self) -> int:
        """Offset in seconds between local and UTC time"""
        return _offset
        

timestamp = Timestamp(time.time())

print(timestamp)

for el in timestamp.utc:
    print(el)
    
print(timestamp.local_utc_offset)

class Datapoint(NamedTuple):
    x: int
    """Timestamp of this datapoint"""
    
    y: int | float
    """y-value of this datapoint"""
    
    


class Timespan(NamedTuple):
    ...