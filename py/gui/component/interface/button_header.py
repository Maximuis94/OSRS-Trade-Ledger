"""
Module with ButtonHeader interface

"""
from abc import ABC, abstractmethod, ABCMeta
from collections.abc import Callable
from typing import Optional, Tuple, Dict

from gui.component.button import GuiButton
from gui.component.interface.column import IListboxColumn


class ButtonHeaderMeta(ABCMeta):
    """Metaclass for the ButtonHeader"""
    def __instancecheck__(self, instance):
        """Method invoked when calling isinstance(any, CLASS), for any CLASS with ButtonHeaderMeta as metaclass"""
        try:
            return isinstance(instance.add, Callable) and \
                isinstance(instance.destroy, Callable) and \
                isinstance(instance.on_click, Callable) and \
                isinstance(instance.generate_buttons, Callable) and \
                isinstance(instance.get_button, Callable)
        except AttributeError:
            return False
        finally:
            return super().__instancecheck__(instance)


class IButtonHeader(ABC, metaclass=ButtonHeaderMeta):
    """A header composed of various buttons"""
    @abstractmethod
    def add(self, *columns: IListboxColumn):
        """Extend this header by one or more buttons derived from `columns`"""
        raise NotImplementedError
    
    @abstractmethod
    def destroy(self):
        """Destroy all widgets related to this ButtonHeader"""
        raise NotImplementedError
    
    @abstractmethod
    def on_click(self, e, idx: int):
        """Method that is executed if any of the buttons is clicked. """
        raise NotImplementedError
    
    @abstractmethod
    def generate_buttons(self, *columns: IListboxColumn, button_command: Optional[Callable] = None):
        """
        Destroy existing Buttons in the grid, then generate all buttons as described by the generated list of kwargs.
        
        Parameters
        ----------
        columns : Optional[IListboxColumn]
            If passed, add additional columns to the list of buttons to generate
        button_command : Optional[Callable]
            If given, set the onclick command of all the buttons to `button_command`
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_button(self, attribute_name: str, attribute_value: any) -> Optional[Tuple[GuiButton, Dict[str, any]]]:
        """
        Get a specific button from the generated of buttons
        
        Parameters
        ----------
        attribute_name : str
            Name of the attribute. Can also refer to the column attribute
        attribute_value : any
            Value of the attribute represented by `attribute_name`

        Returns
        -------
        GuiButton
            The corresponding button
        Dict[str, any]
            The keyword args used to generate the button
        None
            If no matches are found, None is returned
        
        Raises
        ------
        RuntimeError
            If no buttons have been generated, a RuntimeError is raised.
        """
        raise NotImplementedError
        
