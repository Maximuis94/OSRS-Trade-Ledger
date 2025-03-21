import os

# Verplaats roots.json naar config/
CONFIG_FILE = 'config/roots.json'

# Centraliseer pad-gerelateerde configuratie hier
class Paths:
    def __init__(self):
        self.config_file = CONFIG_FILE
        self.cwd = str(os.getcwd()).replace('\\', '/')
        # ... andere pad configuraties ...

# Singleton instance
paths = Paths() 