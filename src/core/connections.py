"""Database connection management with persistent connection for volatile tables."""

import time
from typing import Optional
import teradatasql
from dotenv import load_dotenv
import os

from .config import DatabaseConfig
from .constants import CONNECTION_RETRY_ATTEMPTS, CONNECTION_TIMEOUT
from .exceptions import DatabaseConnectionError
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class PersistentConnectionManager:
    """
    Manages a single persistent database connection for the entire pipeline.
    
    This ensures volatile tables remain accessible throughout processing.
    """
    
    _instance: Optional['PersistentConnectionManager'] = None
    _connection: Optional[teradatasql.TeradataConnection] = None
    
    def __new__(cls):
        """Singleton pattern to ensure only one connection manager exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize connection manager."""
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self.config = DatabaseConfig.from_env()
            
    def get_connection(self) -> teradatasql.TeradataConnection:
        """
        Get the persistent database connection.
        
        Creates a new connection if none exists or if the existing one is closed.
        """
        if self._connection is None or self._connection.closed:
            self._connection = self._create_connection()
        return self._connection
    
    def _create_connection(self) -> teradatasql.TeradataConnection:
        """Create a new database connection with retry logic."""
        params = self.config.to_connection_params()
        last_error = None
        
        for attempt in range(1, CONNECTION_RETRY_ATTEMPTS + 1):
            try:
                logger.info(f"Connecting to database (attempt {attempt}/{CONNECTION_RETRY_ATTEMPTS})")
                connection = teradatasql.connect(**params)
                
                # Test connection
                cursor = connection.cursor()
                cursor.execute("SELECT CURRENT_TIMESTAMP")
                timestamp = cursor.fetchone()[0]
                cursor.close()
                
                logger.info(f"Database connection established at {timestamp}")
                return connection
                
            except Exception as e:
                last_error = e
                logger.warning(f"Connection attempt {attempt} failed: {str(e)}")
                
                if attempt < CONNECTION_RETRY_ATTEMPTS:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        raise DatabaseConnectionError(
            f"Failed to connect after {CONNECTION_RETRY_ATTEMPTS} attempts: {str(last_error)}"
        )
    
    def close(self):
        """Close the database connection."""
        if self._connection and not self._connection.closed:
            try:
                self._connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
            finally:
                self._connection = None
    
    @classmethod
    def reset(cls):
        """Reset the singleton instance (mainly for testing)."""
        if cls._instance:
            cls._instance.close()
        cls._instance = None
        cls._connection = None