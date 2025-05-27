"""Base classes and utilities for database table operations."""

import uuid
from contextlib import contextmanager
from typing import Set, Optional, List, Dict, Any
import teradatasql

from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class TableManager:
    """Manages volatile table lifecycle."""
    
    def __init__(self, connection: teradatasql.TeradataConnection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.session_id = str(uuid.uuid4()).replace('-', '')[:8]
        self.created_tables: Set[str] = set()
    
    def create_volatile_table(
        self, 
        table_suffix: str, 
        columns: List[Dict[str, str]],
        primary_index: Optional[str] = None
    ) -> str:
        """
        Create a volatile table with specified schema.
        
        Args:
            table_suffix: Suffix for the table name
            columns: List of column definitions [{'name': 'col1', 'type': 'VARCHAR(10)'}, ...]
            primary_index: Primary index column(s)
            
        Returns:
            Created table name
        """
        table_name = f"{table_suffix}_{self.session_id}"
        
        # Build column definitions
        col_defs = [f"{col['name']} {col['type']}" for col in columns]
        columns_sql = ", ".join(col_defs)
        
        # Build CREATE TABLE statement
        create_sql = f"CREATE VOLATILE TABLE {table_name} ({columns_sql})"
        
        if primary_index:
            create_sql += f" PRIMARY INDEX ({primary_index})"
        
        create_sql += " ON COMMIT PRESERVE ROWS"
        
        try:
            self.cursor.execute(create_sql)
            self.created_tables.add(table_name)
            logger.debug(f"Created volatile table: {table_name}")
            return table_name
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {str(e)}")
            raise
    
    def drop_table(self, table_name: str) -> None:
        """Drop a single table."""
        try:
            self.cursor.execute(f"DROP TABLE {table_name}")
            self.created_tables.discard(table_name)
            logger.debug(f"Dropped table: {table_name}")
        except Exception as e:
            logger.warning(f"Failed to drop table {table_name}: {str(e)}")
    
    def cleanup_all_tables(self) -> None:
        """Drop all created tables."""
        for table_name in list(self.created_tables):
            self.drop_table(table_name)
    
    @contextmanager
    def temporary_table(self, table_suffix: str, columns: List[Dict[str, str]], **kwargs):
        """Context manager for temporary table creation and cleanup."""
        table_name = self.create_volatile_table(table_suffix, columns, **kwargs)
        try:
            yield table_name
        finally:
            self.drop_table(table_name)


class BatchProcessor:
    """Utilities for batch processing database operations."""
    
    @staticmethod
    def batch_insert(
        cursor: Any,  # Teradata cursor object
        table_name: str,
        columns: List[str],
        data: List[tuple],
        batch_size: int = 10000
    ) -> int:
        """
        Insert data in batches with progress tracking.
        
        Args:
            cursor: Database cursor
            table_name: Target table name
            columns: List of column names
            data: List of tuples to insert
            batch_size: Size of each batch
            
        Returns:
            Number of rows inserted
        """
        if not data:
            return 0
        
        placeholders = ", ".join(["?"] * len(columns))
        columns_str = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        total_inserted = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            try:
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
                
                if total_inserted % 50000 == 0:
                    logger.info(f"Progress: Inserted {total_inserted:,}/{len(data):,} rows")
                    
            except Exception as e:
                logger.error(f"Error inserting batch at index {i}: {str(e)}")
                raise
        
        logger.info(f"Batch insert completed: {total_inserted:,} rows inserted")
        return total_inserted