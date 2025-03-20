"""
This module contains an attempts to reduce the impact of unregistered GE trade completions. The idea is simple;
Each account has 8 GE slots at its disposal. If all slots are tracked using exported data, at some point an inexplicable
transition should occur.
That is, the offer placed on a particular slot on a particular account is replaced by another offer, without the
previous offer actually completing.

Though still not 100% accurate, this would at least offer the benefit of being able to notify the user as soon as it
happens, which is likely to imply that the completed offer is still present in the exchange history.


Example current offers flipping utilities entry;
{
  "accumulatedSessionTimeMillis": 7544998,
  "lastModifiedAt": 1738462400410,
  "lastOffers": {
    "0": {
      "b": false,
      "beforeLogin": false,
      "cQIT": 3919, # currently traded / traded so far
      "id": 22603, # item_id
      "p": 767, # price
      "s": 0, # slot_id
      "st": "SELLING", # STATE
      "t": 1742142178000, # timestamp (ms)
      "tAA": 12359,  # ?
      "tQIT": 15804,  # total quantity
      "tSFO": 5588,
      "tradeStartedAt": 1742138825131,
      "uuid": "fb3de9d8-8b11-48e8-9d41-dc3d08a758c9"
    },
    "1": {
      "b": false,
      "beforeLogin": false,
      "cQIT": 0,
      "id": 11260,
      "p": 0,
      "s": 1,
      "st": "SELLING",
      "t": 1742138825000,
      "tAA": 6771,
      "tQIT": 17250,
      "tSFO": 0,
      "tradeStartedAt": 1742138825132,
      "uuid": "5281db08-a637-4f33-93a6-c0a65cdc6886"
    },
    "2": {
      "b": false,
      "beforeLogin": false,
      "cQIT": 0,
      "id": 9381,
      "p": 0,
      "s": 2,
      "st": "SELLING",
      "t": 1742138825000,
      "tAA": 6771,
      "tQIT": 125860,
      "tSFO": 0,
      "tradeStartedAt": 1742138825132,
      "uuid": "85afdc0c-498a-4e6c-89ec-db2478c94e7f"
    }
  },
  "lastStoredAt": 1742142201600,
  "recipeFlipGroups": [],
  "sessionStartTime": 1742133956463,
  "slotTimers": [
    {
      "currentOffer": {
        "b": false,
        "beforeLogin": false,
        "cQIT": 3919,
        "id": 22603,
        "p": 767,
        "s": 0,
        "st": "SELLING",
        "t": 1742142178000,
        "tAA": 12359,
        "tQIT": 15804,
        "tSFO": 5588,
        "tradeStartedAt": 1742138825131,
        "uuid": "fb3de9d8-8b11-48e8-9d41-dc3d08a758c9"
      },
      "lastUpdate": 1742142178682,
      "offerOccurredAtUnknownTime": false,
      "slotIndex": 0,
      "tradeStartTime": 1742138825131
    },
    {
      "currentOffer": {
        "b": false,
        "beforeLogin": false,
        "cQIT": 0,
        "id": 11260,
        "p": 0,
        "s": 1,
        "st": "SELLING",
        "t": 1742138825000,
        "tAA": 6771,
        "tQIT": 17250,
        "tSFO": 0,
        "tradeStartedAt": 1742138825132,
        "uuid": "5281db08-a637-4f33-93a6-c0a65cdc6886"
      },
      "lastUpdate": 1742138825132,
      "offerOccurredAtUnknownTime": false,
      "slotIndex": 1,
      "tradeStartTime": 1742138825132
    },
    {
      "currentOffer": {
        "b": false,
        "beforeLogin": false,
        "cQIT": 0,
        "id": 9381,
        "p": 0,
        "s": 2,
        "st": "SELLING",
        "t": 1742138825000,
        "tAA": 6771,
        "tQIT": 125860,
        "tSFO": 0,
        "tradeStartedAt": 1742138825132,
        "uuid": "85afdc0c-498a-4e6c-89ec-db2478c94e7f"
      },
      "lastUpdate": 1742138825132,
      "offerOccurredAtUnknownTime": false,
      "slotIndex": 2,
      "tradeStartTime": 1742138825132
    },
    {
      "offerOccurredAtUnknownTime": true,
      "slotIndex": 3
    },
    {
      "offerOccurredAtUnknownTime": true,
      "slotIndex": 4
    },
    {
      "offerOccurredAtUnknownTime": true,
      "slotIndex": 5
    },
    {
      "offerOccurredAtUnknownTime": true,
      "slotIndex": 6
    },
    {
      "offerOccurredAtUnknownTime": true,
      "slotIndex": 7
    }
  ],
  "trades": [ ... ]
}

"""
from collections import namedtuple

import json
import os.path

from typing import List, Optional, Tuple

import global_variables.path as gp
from transaction.raw import ExchangeLoggerEntry, FlippingUtilitiesEntry
from transaction.transaction_model.account import OSRSAccount

# "b": false,
# "beforeLogin": false,
# "cQIT": 3919,  # currently traded / traded so far
# "id": 22603,  # item_id
# "p": 767,  # price
# "s": 0,  # slot_id
# "st": "SELLING",  # STATE
# "t": 1742142178000,  # timestamp (ms)
# "tAA": 12359,  # ?
# "tQIT": 15804,  # total quantity
# "tSFO": 5588,
# "tradeStartedAt": 1742138825131,
# "uuid": "fb3de9d8-8b11-48e8-9d41-dc3d08a758c9"
flipping_utilities_key_mapping = {
    "is_buy": "b",
    "quantity": "cQIT",  # Note that this is the current quantity and that the transaction is ongoing
    "item_id": "id",
    "price": "p",
    "ge_slot": "s",
    # "?": "st",    # STATE
    "timestamp": "t",
    # "?": "tAA",
    "max_quantity": "tQIT",  # total Quantity In Trade (?)
    # "?": "tSFO",  # ???, though t might stand for total
    "timestamp_created": "tradeStartedAt",
    "uuid": "uuid"
    
}

EntryFU = namedtuple("FlippingUtilitiesEntryOngoing", ("is_buy", "current_quantity", "item_id", "ge_slot", ""))


class GESlots:
    """8 GE slots of an OSRS Account"""
    
    slot_0: Optional[ExchangeLoggerEntry]
    slot_1: Optional[ExchangeLoggerEntry]
    slot_2: Optional[ExchangeLoggerEntry]
    slot_3: Optional[ExchangeLoggerEntry]
    slot_4: Optional[ExchangeLoggerEntry]
    slot_5: Optional[ExchangeLoggerEntry]
    slot_6: Optional[ExchangeLoggerEntry]
    slot_7: Optional[ExchangeLoggerEntry]


class GESlotTracker:
    account: OSRSAccount
    """The character that owns the GE slots"""
    
    slots: GESlots
    """The 3 or 8 GE slots that the account has at its disposal"""
    
    @staticmethod
    def last_known_config_flipping_utilities(account: str | OSRSAccount):
        """
        Parse the flipping utilities JSON file of `account` and extract the current status of its 8 GE slots. If the
        name is parsed as a string, make sure the name is passed properly.
        
        
        Parameters
        ----------
        account : str | OSRSAccount
            Account used to search for a particular file in the Flipping Utilities data folder. Alternatively, an
            absolute path to the appropriate json file can be passed as account as well. Though possible, it is
            recommended to stick with default settings.
            
        Returns
        -------
        GESlotTracker
            An with 8 GE slots, as defined by the FlippingUtilities plugin
            
        Raises
        ------
        ValueError
            A ValueError is raised if the account name does not have a json file in the Flipping Utilities data folder.
        
        
        """
        root = gp.dir_flipping_utilities_src
        
        if os.path.exists(account):
            json_file = account
            
        else:
            json_file = None
            
            for _file in os.listdir(root):
                f, e = os.path.splitext(_file)
                if account.name == f:
                    json_file = os.path.join(root, _file)
            if json_file is None:
                raise FileNotFoundError
        
        try:
            json_loaded = json.load(open(json_file, 'r'))
            entry = json_loaded["lastOffers"]
            last_offers = entry.values()
            
            output = []
            for o in last_offers:
                output.append(FlippingUtilitiesEntry(
                ))
        
        except FileNotFoundError:
            ...
        
        except KeyError:
            ...
        
        except AttributeError:
            ...
        
        
        
