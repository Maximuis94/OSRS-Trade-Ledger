"""
This module contains various string formatting methods.
All methods return a specifically formatted string.

Module is to be imported as "import util.str_formats as fmt", so method calls can be read as fmt.seconds() for instance.
In order to prevent naming conflicts, some method names have a trailing underscore (e.g. dt_(), int_())

"""
import datetime
import math
import os
import time
from collections import namedtuple
from collections.abc import Callable

from import_parent_folder import recursive_import
import global_variables.values as gv
del recursive_import

def del_chars(input_string, to_remove: str) -> str:
    """ Iterate over chars in `to_remove` and remove each char from `input_string` """
    for _c in to_remove:
        input_string = input_string.replace(_c, "")
    return input_string


def path_str(path: str = None, add_slash: bool = True) -> str:
    """ Convert path to a string without backslashes  """
    if path is None:
        path = str(os.getcwd())
    add_slash = '/' if add_slash and os.path.isdir(path) and path[-1] != '/' else ''
    return path.replace('\\', '/').rstrip('/') + add_slash
    


##########################################################################
# Time related string formats
##########################################################################


def dt_(dt: datetime.datetime = datetime.datetime.now(), fmt_str: str = '%d-%m-%y %H:%M:%S') -> str:
    """
    Format datetime.datetime `dt` according to `format_string`. By default, return as 01-12-23 01:23:45 (= d-m-y h:m:s)
    
    Parameters
    ----------
    dt : datetime.datetime, optional, datetime.datetime.now() by default
        The datetime that is to be formatted. If not specified, the current time is used
    fmt_str : str, optional, %d-%m-%y %H:%M:%S by default
        String that specifies how the datetime.datetime should be formatted. See Notes section.

    Returns
    -------
    str
        The datetime.datetime formatted using `fmt_str`
    
    Notes
    -----
    Various format values that can be used;
    %d: day as a double-digit integer
    %m: month as a double-digit integer
    %y: last 2 digits of the year
    %H: hour as a double-digit integer
    %M: minute as a double-digit integer
    %S: second as a double-digit integer
    
    See also https://docs.python.org/3.10/library/datetime.html#strftime-strptime-behavior
    """
    return dt.strftime(fmt_str)


def y_m(yyyymm: int):
    """ Convert a YYYYMM integer to a readable date MMM YY format (E.g. 202310 -> Oct 23) """
    return f"{gv.months_short[yyyymm % 100]} {yyyymm // 100 % 100}"


# See also https://docs.python.org/3.10/library/time.html#time.strftime
def unix_(unix_ts: (int or float) = time.time(), fmt_str: str = '%d-%m-%y %H:%M:%S', utc: bool = False) -> str:
    """ Format unix timestamp `unix_ts` using `fmt_str`. Default format: 01/12/23 01:23:45 (= d-m-y h:m:s) """
    return time.strftime(fmt_str, time.gmtime(unix_ts) if utc else time.localtime(unix_ts))


def delta_t(n: (int, float), days: bool = True, allow_ms: bool = True, t_getter: Callable = time.perf_counter) -> str:
    """ Convert an amount of seconds `n` into days, hours, minutes, seconds. Return as 'Dd HH:MM:SS' or 'HH:MM:SS'. """
    if n >= 864000:
        n = t_getter()-n
    
    if isinstance(n, float):
        ms, n = math.modf(n)
        
        n = int(n)
        if not allow_ms or n > 100:
            ms = 0
        elif n < 10:
            return f'{(n+ms)*1000:.0f}ms'
    else:
        ms = 0
    return f"{f'{n // 86400}d ' if n >= 86400 else ''}{n % 86400 // 3600:0>2}:{n % 3600 // 60:0>2}:{n % 60:0>2}" \
           if days else \
        f"{n // 3600:0>2}:{n % 3600 // 60:0>2}:{max(1, n % 60):0>2}{f'.{ms*1000:.0f}' if ms > 0 else ''}"


def passed_time(n: int or float, days: bool = True, t_getter: Callable = time.time) -> str:
    """ Convert the amount of passed time relative to unix timestamp `n` and format it with fmt.delta_t() """
    return delta_t(n=int(t_getter()-n), days=days)


def passed_pc(n: int or float, days: bool = True) -> str:
    """ Convert the amount of passed time relative to unix timestamp `n` and format it with fmt.delta_t() """
    d = time.perf_counter()-n
    return delta_t(n=d if d < 10 else int(d), days=days)


def dow(unix_ts: int or float, utc: bool = False, shortened: bool = False) -> str:
    """ Return full/shortened the day-of-week of the given unix timestamp `unix_ts`. Use local/utc time, given `utc` """
    return time.strftime('%a' if shortened else '%A', time.localtime(unix_ts) if utc else time.gmtime(unix_ts))


def month(unix_ts: int or float, utc: bool = False, shortened: bool = False) -> str:
    """ Return full/shortened the month of the given unix timestamp `unix_ts`. Use local/utc time, given `utc` """
    return time.strftime('%b' if shortened else '%B', time.localtime(unix_ts) if utc else time.gmtime(unix_ts))



##########################################################################
# Numerical string formats
##########################################################################

# Rules for abbreviating integer strings
Multiplier = namedtuple('Multiplier', ['multiplier', 'abbreviation', 'threshold'])

# Threshold values to determine when each Multiplier is applied
_multiplier_thresholds = [10000, 10000000, 10000000000]

# Various abbreviations used to shorten integer strings.
_int_abbreviations = [
    Multiplier(multiplier=pow(10, 9), abbreviation='B', threshold=10000000000),
    Multiplier(multiplier=pow(10, 6), abbreviation='M', threshold=10000000),
    Multiplier(multiplier=pow(10, 3), abbreviation='K', threshold=10000),
    Multiplier(multiplier=pow(10, 0), abbreviation=' ', threshold=0)
]


def number(n: (int or float), max_decimals: int = 3, max_length: int = 8) -> str:
    """ Return a shortened version of number `n` by substituting trailing 0s with K, M or B """
    try:
        m = 1
        for next_multiplier in _int_abbreviations:
            if next_multiplier.threshold < n.__abs__():
                m = next_multiplier
                break
    except AttributeError:
        return '-1'
    # Do not abbreviate integers that consist of less than 6 digits
    if abs(n) < 100000:
        return str(round(n, 0))
    if max_decimals == 0:
        return f"{round(n / m.multiplier, 0)}{m.abbreviation}"
    n = n / m.multiplier
    try:
        try:
            n_d = min(max_decimals, max(0, max_length - len(f"{int(n)}") - 2))
        except TypeError:
            n_d = 1
        if -.05 < n - int(n) < .05:
            return f"{int(n)}{m.abbreviation}"
        return f'{n:.{n_d}f}{m.abbreviation}'
    except ValueError:
        return f'-1'
    

def int_(n: int) -> str:
    """ Format number, but pre-defined args for integers """
    return number(n=int(round(n, 0)), max_decimals=0)


def float_(n: float, max_str_len: int = 6) -> str:
    """ Format a float while setting a number of rounding to a number of decimals compliant with `max_str_len` """
    return f"{n:.{max(max_str_len - len(str(math.modf(n)[1])[1:]), 0)}f}".strip('0')


def percentage(p: (int or float), n_decimals: int = 1) -> str:
    """ Format percentage `p` as a readable percentage, i.e. multiply by 100 and append '%' """
    return f'{p*100:.{n_decimals}f}'


_delta_t_ns_abbreviations = [
    Multiplier(multiplier=pow(10, n), abbreviation='s', threshold=pow(10, n+1)-1)
        for a, n in zip(['s', 'ms', 'μs', 'ns'], [9, 6, 3, 0])
    ]


def delta_t_ns(ns: int, n_decimals: int = 1):
    for m in _delta_t_ns_abbreviations:
        if ns > m.threshold:
            t = round(ns/m.multiplier, n_decimals)
            d = math.modf(ns)[0]
            return f'{t}{0 if d == 0 else {len(str(d))-1}}{m.abbreviation}'
        

# file_size_abbreviations = [(pow(10, 9 - n * 3), ['gb', 'mb', 'kb'][n]) for n in range(3)]
file_size_abbreviations = [
    Multiplier(multiplier=pow(10, n), abbreviation=a, threshold=pow(10, n+1)-1)
    for a, n in zip(['gb', 'mb', 'kb'], [9, 6, 3])
]


def fsize(arg: int or str, n_decimals: int = 1) -> str:
    """ Return `file_size` or the file size of the file at `path` as formatted string, dividing the the file size by
    10^9, 10^6 or 10^3 for gb, mb or kb, respectively.
    
    Parameters
    ----------
    arg: int or str
        Path to a file. If passed, it will override `file_size` with the size of the file at `path`
    n_decimals : int, optional, 1 by default
        Amount of decimals to use

    Returns
    -------
    str
        file size as a formatted string

    """
    file_size = os.path.getsize(arg) if isinstance(arg, str) else arg
    _size = abs(file_size)
    for m in file_size_abbreviations:
        if _size >= m.threshold:
            file_size = round(file_size / m.multiplier, n_decimals)
            d = abs(math.modf(file_size)[0])
            return f"{file_size:.{min(n_decimals, len(str(d))-1) if d != 0 else 0}f}{m.abbreviation}"
    return str(file_size)


def fsize2(arg: int or str, n_decimals: int = 1) -> str:
    """ Return `file_size` or the file size of the file at `path` as formatted string, dividing the the file size by
    10^9, 10^6 or 10^3 for gb, mb or kb, respectively.
    
    Parameters
    ----------
    arg: int or str
        Path to a file. If passed, it will override `file_size` with the size of the file at `path`
    n_decimals : int, optional, 1 by default
        Amount of decimals to use

    Returns
    -------
    str
        file size as a formatted string

    """
    # file_size = os.path.getsize(arg) if isinstance(arg, str) else arg
    try:
    # arg=1
        _size = abs(arg)
    except TypeError:
        return fsize(os.path.getsize(arg), n_decimals)
    
    for m in file_size_abbreviations:
        if _size >= m.threshold:
            arg = round(arg / m.multiplier, n_decimals)
            d = abs(math.modf(arg)[0])
            return f"{arg:.{min(n_decimals, len(str(d))-1) if d != 0 else 0}f}{m.abbreviation}"
    return str(arg)


##########################################################################
# Textual string formats
##########################################################################


# Abbreviations applied when abbreviating a string
_str_abbreviations = {
    'timestamp': 'ts',
    'is_buy': 'b',
    'buy': 'bp',
    'sell': 's',
    # 'value': 'val',
    'volume': 'v',
    'price': 'p',
    'day_of_week': 'dow',
    'avg5m': '5m',
    'realtime': 'rt',
    'wiki': 'w',
    '_': ''
}


def shorten_string(string: str, max_length: int = 20):
    """ Shorten the input string to `max_length` if its length exceeds `max_length`, replace chars with '.' """
    if len(string) <= max_length:
        return string
    else:
        n = max_length // 2
        return f"{string[:n - 1]}{'.' * (max_length-2*n+2)}{string[-n + 1:]}"


def abbreviate(string: str):
    """ Abbreviate `string` by replacing certain words with an abbreviated version of that word """
    for _from, _to in _str_abbreviations.items():
        string = string.replace(_from, _to)
    return string


def as_osrs_item_name(item_name: str) -> str:
    """ Format `item_name` according to OSRS item naming guidelines (first char uppercase, other lower) """
    return item_name[0].upper() + item_name[1:].lower()


##########################################################################
# Miscellaneous string formats
##########################################################################


def is_buy(b: bool) -> str:
    """Format is_buy boolean `b` into Buy if True or Sell if False """
    return "Buy" if b else "Sell"

if __name__ == '__main__':
    ...



