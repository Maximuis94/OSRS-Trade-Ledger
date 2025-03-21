from pathlib import Path
import sqlite3
import logging
import shutil
from typing import List, Dict
from dataclasses import dataclass
import json
import os

@dataclass
class ProjectConfig:
    """Project configuration settings"""
    root_dir: Path
    data_dir: Path
    database_dir: Path
    resource_dir: Path
    export_dir: Path
    log_dir: Path
    temp_dir: Path
    
    required_dirs: List[Path] = None
    
    def __post_init__(self):
        self.required_dirs = [
            self.data_dir,
            self.database_dir,
            self.resource_dir,
            self.export_dir,
            self.log_dir,
            self.temp_dir,
            self.data_dir / 'npy_arrays',
            self.data_dir / 'resources',
            self.export_dir / 'flipping_utilities',
            self.export_dir / 'exchange_logger',
            self.export_dir / 'runelite'
        ]

class ProjectSetup:
    """Handles project setup and initialization"""
    
    def __init__(self, config: ProjectConfig):
        self.config = config
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Initialize logging"""
        logger = logging.getLogger('project_setup')
        logger.setLevel(logging.INFO)
        
        # Create handlers
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(self.config.log_dir / 'setup.log')
        
        # Create formatters
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(formatter)
        f_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
        
        return logger

    def setup_project_structure(self):
        """Create necessary project directories"""
        self.logger.info("Setting up project directory structure...")
        
        for directory in self.config.required_dirs:
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created directory: {directory}")

    def setup_databases(self):
        """Initialize all required databases"""
        self.logger.info("Setting up databases...")
        
        databases = {
            'items': self._create_items_db,
            'transactions': self._create_transactions_db,
            'timeseries': self._create_timeseries_db,
            'npy': self._create_npy_db
        }
        
        for db_name, setup_func in databases.items():
            try:
                setup_func()
                self.logger.info(f"Successfully set up {db_name} database")
            except Exception as e:
                self.logger.error(f"Failed to set up {db_name} database: {str(e)}")
                raise

    def _create_items_db(self):
        """Create and initialize the items database"""
        db_path = self.config.database_dir / 'items.db'
        
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS item (
                    id INTEGER PRIMARY KEY,
                    item_id INTEGER NOT NULL UNIQUE,
                    item_name TEXT NOT NULL,
                    members BOOLEAN NOT NULL,
                    alch_value INTEGER NOT NULL,
                    buy_limit INTEGER NOT NULL,
                    stackable BOOLEAN NOT NULL,
                    release_date INTEGER NOT NULL,
                    equipable BOOLEAN NOT NULL,
                    weight REAL NOT NULL,
                    update_ts INTEGER NOT NULL,
                    augment_data INTEGER DEFAULT 0,
                    remap_to INTEGER DEFAULT 0,
                    remap_price REAL DEFAULT 0,
                    remap_quantity REAL DEFAULT 0,
                    target_buy INTEGER DEFAULT 0,
                    target_sell INTEGER DEFAULT 0,
                    item_group TEXT DEFAULT '',
                    count_item INTEGER DEFAULT 1
                );
                
                CREATE INDEX IF NOT EXISTS idx_item_id ON item(item_id);
                CREATE INDEX IF NOT EXISTS idx_item_name ON item(item_name);
            """)

    def _create_transactions_db(self):
        """Create and initialize the transactions database"""
        db_path = self.config.database_dir / 'transactions.db'
        
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS raw_transaction (
                    transaction_id INTEGER PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    is_buy BOOLEAN NOT NULL,
                    quantity INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    account_name TEXT,
                    update_timestamp INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    FOREIGN KEY (item_id) REFERENCES item(item_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_transaction_item 
                    ON raw_transaction(item_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_transaction_account 
                    ON raw_transaction(account_name, timestamp);
            """)

    def _create_timeseries_db(self):
        """Create and initialize the timeseries database"""
        db_path = self.config.database_dir / 'timeseries.db'
        
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    volume INTEGER NOT NULL,
                    source INTEGER NOT NULL,
                    UNIQUE (item_id, timestamp, source)
                );
                
                CREATE INDEX IF NOT EXISTS idx_price_item_time 
                    ON price_data(item_id, timestamp);
            """)

    def _create_npy_db(self):
        """Create and initialize the NPY database"""
        db_path = self.config.database_dir / 'npy.db'
        
        # Read the NPY schema SQL
        schema_path = Path(__file__).parent / 'sql' / 'npy_schema.sql'
        with open(schema_path) as f:
            schema_sql = f.read()
        
        with sqlite3.connect(db_path) as conn:
            conn.executescript(schema_sql)

    def create_config_files(self):
        """Create necessary configuration files"""
        self.logger.info("Creating configuration files...")
        
        config = {
            'paths': {
                'data_dir': str(self.config.data_dir),
                'database_dir': str(self.config.database_dir),
                'resource_dir': str(self.config.resource_dir),
                'export_dir': str(self.config.export_dir),
                'log_dir': str(self.config.log_dir),
                'temp_dir': str(self.config.temp_dir)
            },
            'database': {
                'items_db': str(self.config.database_dir / 'items.db'),
                'transactions_db': str(self.config.database_dir / 'transactions.db'),
                'timeseries_db': str(self.config.database_dir / 'timeseries.db'),
                'npy_db': str(self.config.database_dir / 'npy.db')
            },
            'settings': {
                'debug': False,
                'auto_vacuum': True,
                'backup_interval': 86400,  # 1 day in seconds
                'max_concurrent_connections': 5
            }
        }
        
        config_path = self.config.resource_dir / 'config.json'
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        
        self.logger.info(f"Created configuration file at {config_path}")

    def setup_all(self):
        """Run complete setup process"""
        try:
            self.logger.info("Starting project setup...")
            
            self.setup_project_structure()
            self.setup_databases()
            self.create_config_files()
            
            self.logger.info("Project setup completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Project setup failed: {str(e)}")
            return False

def main():
    """Main setup function"""
    # Get the project root directory (assuming this script is in src/setup)
    root_dir = Path(__file__).parent.parent.parent
    
    config = ProjectConfig(
        root_dir=root_dir,
        data_dir=root_dir / 'data',
        database_dir=root_dir / 'data' / 'db',
        resource_dir=root_dir / 'data' / 'resources',
        export_dir=root_dir / 'data' / 'export',
        log_dir=root_dir / 'data' / 'logs',
        temp_dir=root_dir / 'data' / 'temp'
    )
    
    setup = ProjectSetup(config)
    
    if setup.setup_all():
        print("Project setup completed successfully!")
        print(f"Project root: {root_dir}")
        print("\nCreated directories:")
        for directory in config.required_dirs:
            print(f"  - {directory}")
    else:
        print("Project setup failed. Check the logs for details.")

if __name__ == "__main__":
    main() 