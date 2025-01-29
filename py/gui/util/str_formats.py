"""
String-related helper functions

"""
import math
import time
from collections import namedtuple


def strf_float(f: float) -> str:
    """ Return float `f` as a formatted str, removing trailing 0s and possibly the . """
    return str(f).rstrip('0.')


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


def strf_number(n: (int or float), max_decimals: int = 3, max_length: int = 8) -> str:
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
            n_d = min(max_decimals, max_length - len(str(int(n))) - 3)
        except TypeError:
            n_d = 1
        if n_d < 0 and abs(n) > 1:
            return str(int(round(n, 0))) + m.abbreviation
        if -.05 < n - int(n) < .05:
            return f"{int(n)}{m.abbreviation}"
        return f'{n:.{n_d}f}{m.abbreviation}'
    except ValueError:
        return f'-1'


def strf_int(n: int) -> str:
    """ Format number, but pre-defined args for integers """
    return strf_number(n=int(round(n, 0)), max_decimals=0)


def strf_float(n: float, max_str_len: int = 6) -> str:
    """ Format a float while setting a number of rounding to a number of decimals compliant with `max_str_len` """
    return f"{n:.{max(max_str_len - len(str(math.modf(n)[1])[1:]), 0)}f}".strip('0')


# See also https://docs.python.org/3.10/library/time.html#time.strftime
def strf_unix(unix_ts: (int or float) = time.time(), fmt_str: str = '%d-%m-%y %H:%M:%S', utc: bool = False) -> str:
    """ Format unix timestamp `unix_ts` using `fmt_str`. Default format: 01/12/23 01:23:45 (= d-m-y h:m:s) """
    return time.strftime(fmt_str, time.gmtime(unix_ts) if utc else time.localtime(unix_ts))


def shorten_string(string: str, max_length: int = 20):
    """ Shorten the input string to `max_length` if its length exceeds `max_length`, replace chars with '.' """
    if len(string) <= max_length:
        return string
    else:
        n = (max_length - 1) // 2
        return f"{string[:n - 1]}...{string[-n + 1:]}"
    