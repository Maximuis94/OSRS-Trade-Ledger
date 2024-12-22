"""
This module contains a baseclass used as template for gui objects.

GuiWidgets are placed on a GuiFrame,

"""
from dataclasses import dataclass


@dataclass
class A:
    a: int = 1
    b: int = 2
    c: int = 4

if __name__ == '__main__':
    aaa = A(2, 3, 4)
    
    aaa.__dict__.update({'a': 2, 'd': 5})
    print(aaa.__dict__)
