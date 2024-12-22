"""
Module for the TkGrid, which is a grid that accepts a list of strings and assigns widgets listed in these strings via a
tag the appropriate space in the GUI.

The idea behind the grid is to allow one to easily alter the way the gui is organized, without having to alter variables
across multiple instances of widgets. Instead, alter the list of strings. The list of strings is converted into a grid
representation, where each Widget is encoded as a char





"""

from gui.base.frame import TkGrid

# X, Y, W, H

if __name__ == '__main__':
    tkg = TkGrid(['AAAAAaaaaabbbbbbb',
            'AAAAAaaaaabbbbbbb',
            'ccccccccccccccccc'])
    print(tkg.get_dims('A'))
    