"""
Parser for **kwargs passed in methods.
Methods can be used to verify types of keyword args.
Additionally, a handler can be passed to further specify how the
value should be returned.

"""
from collections import namedtuple
from collections.abc import Callable
from typing import Iterable


KwParser = namedtuple('KwParser', ('k', 'h', 't'))


def parse_exception(value: str, expected_type: str = None):
    pass


def parse_dicts(dicts: Iterable[dict]):
    """ Apply parse() to each element from `dicts` and return the result """
    return [parse(kw=d.get('k'), return_handler=d.get('h'), return_type=d.get('t')) for d in dicts]


def parse_kwps(kwps: Iterable[KwParser]):
    """ Apply parse to each KwParser element from `kwps` and return the result """
    return [parse(kw=kwp.k, return_handler=kwp.h, return_type=kwp.t) for kwp in kwps]


def parse_kws(kws: Iterable[tuple], **kwargs):
    """ Apply parse() to each kwparser element from `kws` and return the result """
    output = []
    for _tuple in kws:
        if len(_tuple) == 1:
            output.append(parse_args(_tuple[0], **kwargs))
        elif len(_tuple) == 2:
            output.append(parse_args(_tuple[0], None, _tuple[1], **kwargs))
        else:
            output.append(parse_args(_tuple[0], _tuple[1], _tuple[2], **kwargs))
    return output


def parse_args(*args, **kwargs):
    if len(args) == 1:
        return parse(kw=args[0], **kwargs)
    elif len(args) == 2:
        return parse(kw=args[0], return_type=args[1], **kwargs)
    else:
        return parse(kw=args[0], return_handler=args[1], return_type=args[2], **kwargs)


def parse(kw: str, return_handler: Callable = None, return_type=None, **kwargs):
    """
    Check if keyword `kw` is present in `kwargs`. If so, call this value with `return_handler` (if given). Before
    returning its value, check if the typing matches with `return_type`, if given.
    
    Parameters
    ----------
    kw : str
        Keyword to search for in kwargs dict
    return_handler : collections.abc.Callable, optional, None by default
        If specified, pass the fetched value as arg to `return_handler` before returning it (and checking type)
    return_type
        If specified, check if the to-be-returned value type matches `return_type`
    kwargs : dict
        Keyword args dict passed to a method

    Returns
    -------
    arg
        If the kwargs dict has a value at `kw` of type `return_type` (if given), after having passed it to
        `return_handler` (if given), return it.
    
    Raises
    ------
    TypeError
        If any of the checks mentioned in the 'Returns' section fails, a TypeError is raised. If a check was not
        defined, it cannot raise a TypeError.
    
    Examples
    --------
    Suppose you want to extract integer n=120 from **kwargs;
    n = parse('n', None, int, n=120)

    """
    arg = kwargs.get(kw)
    
    # keyword kw is not in the kwargs dict;
    if arg is None:
        raise ValueError(f'keyword args dict does not contain a value for key {kw}')
    
    else:
        # arg from kwargs.get(kw) is passed to return_handler, if it was specified.
        arg = return_handler(arg) if isinstance(return_handler, Callable) else arg
        
        # Check whether the to-be returned value matches the specified type.
        if return_type is None or (return_type is not None and isinstance(arg, return_type)):
            return arg
        else:
            raise TypeError(f'kwargs dict does not have appropriate typing {return_type} for key {kw} (type={type(arg)}')


if __name__ == '__main__':
    my_args = [('n', int), ('b', float)]
    print(parse_kws(my_args, n=150, b=120.0))
    