from typing import Optional, List, Tuple, Dict
import sqlite3
from .sql_manager import sql
from .base_raw_transaction import BaseRawTransaction

class TransactionMerger:
    """
    Handles merging of transactions from different sources,
    maintaining data integrity and handling conflicts.
    """
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.cursor = connection.cursor()

    def merge_transactions(self, transaction_id_a: int, transaction_id_b: int) -> Optional[int]:
        """
        Merges two transactions, preserving the most reliable data
        and maintaining referential integrity.
        """
        try:
            # Get merge statement and execute
            stmt = sql.get_statement('update', 'merge_transactions')
            params = {
                'transaction_id_a': transaction_id_a,
                'transaction_id_b': transaction_id_b
            }
            
            # Start transaction
            self.conn.execute('BEGIN TRANSACTION')
            
            # Execute merge
            result = self.cursor.execute(stmt, params)
            
            # Update related records
            self._update_related_records(transaction_id_a, transaction_id_b)
            
            # Commit transaction
            self.conn.commit()
            
            return result.lastrowid
            
        except Exception as e:
            self.conn.rollback()
            raise e

    def _update_related_records(self, kept_id: int, merged_id: int):
        """Update all related records to point to the kept transaction"""
        updates = [
            ('update_inventory_entries', {'old_id': merged_id, 'new_id': kept_id}),
            ('update_stock_counts', {'old_id': merged_id, 'new_id': kept_id}),
            ('update_transaction_tags', {'old_id': merged_id, 'new_id': kept_id})
        ]
        
        for statement_name, params in updates:
            stmt = sql.get_statement('update', statement_name)
            self.cursor.execute(stmt, params)

    def find_potential_duplicates(self, transaction: BaseRawTransaction) -> List[int]:
        """Find potential duplicate transactions based on matching criteria"""
        stmt = sql.get_statement('select', 'find_potential_duplicates')
        params = transaction.to_dict()
        
        return [row[0] for row in self.cursor.execute(stmt, params).fetchall()] 