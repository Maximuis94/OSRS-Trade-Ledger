from typing import Callable, Optional
import os
import json
import pickle

class IOProtocol:
    """Base class for handling different file protocols"""
    def load(self, path: str) -> any:
        raise NotImplementedError
        
    def save(self, data: any, path: str) -> None:
        raise NotImplementedError

class JsonProtocol(IOProtocol):
    def load(self, path: str) -> any:
        with open(path, 'r') as f:
            return json.load(f)
            
    def save(self, data: any, path: str) -> None:
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)

class PickleProtocol(IOProtocol):
    def load(self, path: str) -> any:
        with open(path, 'rb') as f:
            return pickle.load(f)
            
    def save(self, data: any, path: str) -> None:
        with open(path, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL) 