from dataclasses import dataclass, field
from typing import Optional, Tuple, Callable
import sqlite3
import global_variables.path as gp
from interfaces.db_entity import DbEntity

@dataclass(slots=True, match_args=False)
class ItemProductionRule(DbEntity):
    """
    Representeert een productieregel voor items.
    Definieert hoe items geproduceerd kunnen worden uit andere items.
    """
    id: int = field(compare=True)
    rule_id: int = field(compare=False)
    output_item_id: int = field(compare=False)
    output_quantity: int = field(compare=False)
    input_item_id: int = field(compare=False)
    input_quantity: int = field(compare=False)
    skill_name: str = field(compare=False)
    skill_level: int = field(compare=False)
    update_ts: int = field(compare=False)

    @property
    def sqlite_path(self) -> str:
        return str(gp.f_db_local)
    
    @property
    def sqlite_table(self) -> str:
        return "item_production_rule"
    
    @property
    def sqlite_row_factory(self) -> Optional[Callable[[sqlite3.Cursor, tuple], any]]:
        return lambda c, row: ItemProductionRule(*row)
    
    @property
    def sqlite_attributes(self) -> Tuple[str, ...]:
        return ("id", "rule_id", "output_item_id", "output_quantity", 
                "input_item_id", "input_quantity", "skill_name", 
                "skill_level", "update_ts")
    
    @property
    def sqlite_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS "{self.sqlite_table}" (
            id INTEGER PRIMARY KEY,
            rule_id INTEGER NOT NULL UNIQUE,
            output_item_id INTEGER NOT NULL,
            output_quantity INTEGER NOT NULL,
            input_item_id INTEGER NOT NULL,
            input_quantity INTEGER NOT NULL,
            skill_name TEXT NOT NULL,
            skill_level INTEGER NOT NULL,
            update_ts INTEGER NOT NULL,
            FOREIGN KEY (output_item_id) REFERENCES item(item_id),
            FOREIGN KEY (input_item_id) REFERENCES item(item_id)
        )"""

    @property
    def sqlite_trigger(self) -> str:
        return f"""
        CREATE TRIGGER IF NOT EXISTS update_ts_{self.sqlite_table}
        AFTER UPDATE ON "{self.sqlite_table}"
        BEGIN
            UPDATE "{self.sqlite_table}" 
            SET update_ts = unixepoch()
            WHERE rule_id = NEW.rule_id;
        END
        """

    @property
    def sqlite_index(self) -> Tuple[str, ...]:
        return (
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_rule_id 
                ON "{self.sqlite_table}"(rule_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_output_item 
                ON "{self.sqlite_table}"(output_item_id)""",
            f"""CREATE INDEX IF NOT EXISTS idx_{self.sqlite_table}_input_item 
                ON "{self.sqlite_table}"(input_item_id)"""
        ) 