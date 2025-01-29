"""
Module with various decorators

"""
import inspect
from functools import wraps


def inherit_docstring(parent_method):
    """
    A decorator to inherit docstrings from parent methods.
    """
    def decorator(child_method):
        child_method.__doc__ = parent_method.__doc__
        return child_method
    return decorator


class DocstringInheritor:
    """Decorator class for allowing docstring inheritance"""
    @staticmethod
    def inherit_docstrings(cls):
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            for base in cls.__bases__:
                parent_method = getattr(base, name, None)
                if parent_method and not method.__doc__:
                    method.__doc__ = parent_method.__doc__
        return cls
