"""
Realization of the Inventory Listbox Frame.

It displays the current inventory per item, as well as statistics related to past trades.


"""
from gui.component.listbox import GuiListbox
from gui.frame.listbox_frame import GuiListboxFrame


class InventoryListboxFrame(GuiListboxFrame):
    def setup_primary_listbox(self, **kwargs) -> GuiListbox:
        """
        Initializes the primary Listbox. It displays the inventory of a particular item per row.
        Color scheme?
        By default, transactions with status=0 are omitted.
        
        default active filters;
            TODO think about default active filters, e.g. abs(stock) < 50
        
        default active sorts;
            1. Chronological sort (new-old)
            2. Alphabetical sort (A-Z)
        
        onclick_row_primary();
            Display all Transactions made with the itemthat was selected in the second listbox.
                ... but exclude transactions with status=0
            Display statistics for this particular item below secondary listbox
            Generate price graph for this particular item in other frame
            
        onclick_row_secondary();
            ...
        
        quick_sorts();
            1. Chronological sort (new-old) + Alphabetical sort (A-Z)
            2. Profit sort (high-low)
        
        quick_filters();
            1. abs(value) > 5000000
            2. balance < 0 | price < 0
            3. Traded in past 3 months
        
        color_schemes;
            1. ...
            
        
        
        
        
        
        
        Parameters
        ----------
        kwargs

        Returns
        -------

        """
        pass
    
    def setup_secondary_listbox(self, **kwargs) -> GuiListbox:
        pass
    
    def implement_configurations(self):
        pass