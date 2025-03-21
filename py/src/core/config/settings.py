import json
from pathlib import Path
from typing import Dict, Any

class Settings:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._config_file = Path('config/settings.json')
        self.load_config()
    
    def load_config(self):
        if self._config_file.exists():
            with open(self._config_file) as f:
                self._config = json.load(f)
        else:
            self._config = self._default_config()
            self.save_config()
    
    def save_config(self):
        with open(self._config_file, 'w') as f:
            json.dump(self._config, f, indent=4)
    
    @staticmethod
    def _default_config() -> Dict[str, Any]:
        return {
            'paths': {
                'data_dir': 'data',
                'resources_dir': 'data/resources',
                'output_dir': 'data/output',
            },
            'database': {
                'local_path': 'data/local.db',
                'timeseries_path': 'data/timeseries.db'
            }
        }

settings = Settings()  # Singleton instance 