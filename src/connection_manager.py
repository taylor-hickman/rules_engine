"""
Shared Database Connection Manager

Manages a single database connection shared across multiple components to ensure
volatile tables remain accessible throughout the processing lifecycle.
"""

import logging
from typing import Dict, Optional, Set
from contextlib import contextmanager
import teradatasql

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection operations fail."""
    pass


class SharedConnectionManager:
    """
    Manages shared database connection for multiple processing components.
    
    Ensures volatile tables created by one component remain accessible to other
    components using the same database session. Tracks component usage and
    provides safe connection lifecycle management.
    """
    
    def __init__(self, connection_parameters: Dict[str, str]):
        """
        Initialize connection manager with database parameters.
        
        Args:
            connection_parameters: Database connection configuration
                Required keys: host, user, password
                Optional keys: logmech, TMODE
                
        Raises:
            ValueError: If required connection parameters are missing
        """
        self._validate_connection_parameters(connection_parameters)
        
        self.connection_params = connection_parameters.copy()
        self.connection: Optional[teradatasql.TeradataConnection] = None
        self.active_components: Set[str] = set()
        self.connection_attempts = 0
        self.max_connection_attempts = 3
        
        logger.debug("Shared connection manager initialized")
    
    def get_connection(self, component_name: str) -> teradatasql.TeradataConnection:
        """
        Provides shared database connection to requesting component.
        
        Creates connection on first request and reuses for subsequent requests.
        Tracks which components are using the connection for safe cleanup.
        
        Args:
            component_name: Identifier for component requesting connection
            
        Returns:
            Active database connection
            
        Raises:
            DatabaseConnectionError: If connection cannot be established
            ValueError: If component_name is empty or None
        """
        if not component_name or not component_name.strip():
            raise ValueError("Component name cannot be empty")
        
        component_name = component_name.strip()
        
        if self.connection is None:
            self._establish_connection()
        
        self.active_components.add(component_name)
        logger.debug(f"Connection provided to component: {component_name}")
        logger.debug(f"Active components: {sorted(self.active_components)}")
        
        return self.connection
    
    def release_component(self, component_name: str) -> None:
        """
        Releases component's hold on shared connection.
        
        Removes component from active tracking. Connection remains open
        until all components have released it or cleanup is called.
        
        Args:
            component_name: Identifier for component releasing connection
        """
        if not component_name:
            logger.warning("Attempted to release connection with empty component name")
            return
        
        component_name = component_name.strip()
        
        if component_name in self.active_components:
            self.active_components.remove(component_name)
            logger.debug(f"Component released connection: {component_name}")
            logger.debug(f"Remaining active components: {sorted(self.active_components)}")
        else:
            logger.warning(f"Attempted to release connection for unregistered component: {component_name}")
    
    def is_connection_active(self) -> bool:
        """
        Checks if database connection is currently active.
        
        Returns:
            True if connection exists and is usable, False otherwise
        """
        if self.connection is None:
            return False
        
        try:
            # Test connection with simple query
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            logger.warning(f"Connection health check failed: {str(e)}")
            return False
    
    def get_active_components(self) -> Set[str]:
        """
        Returns set of components currently using the connection.
        
        Returns:
            Copy of active components set
        """
        return self.active_components.copy()
    
    def get_connection_info(self) -> Dict[str, any]:
        """
        Returns information about current connection state.
        
        Returns:
            Dictionary containing connection status and component information
        """
        return {
            'is_active': self.is_connection_active(),
            'active_components': list(self.active_components),
            'component_count': len(self.active_components),
            'connection_attempts': self.connection_attempts,
            'host': self.connection_params.get('host', 'unknown')
        }
    
    @contextmanager
    def get_temporary_connection(self, component_name: str):
        """
        Context manager for temporary connection access.
        
        Automatically registers and releases component connection.
        Useful for short-lived operations that need database access.
        
        Args:
            component_name: Identifier for temporary component
            
        Yields:
            Active database connection
            
        Example:
            with connection_manager.get_temporary_connection('temp_validator') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM table")
                result = cursor.fetchone()
        """
        try:
            connection = self.get_connection(component_name)
            yield connection
        finally:
            self.release_component(component_name)
    
    def cleanup(self) -> None:
        """
        Closes shared connection and cleans up all resources.
        
        Should be called when all processing is complete. Logs warning
        if components are still active when cleanup is called.
        """
        if self.active_components:
            logger.warning(f"Cleanup called with active components: {sorted(self.active_components)}")
            logger.warning("Components should release connections before cleanup")
        
        if self.connection:
            try:
                logger.info("Closing shared database connection")
                self.connection.close()
                logger.info("Shared database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
            finally:
                self.connection = None
                self.active_components.clear()
        else:
            logger.debug("Cleanup called but no active connection found")
    
    def force_reconnect(self) -> None:
        """
        Forces reconnection by closing current connection.
        
        Next call to get_connection will establish new connection.
        Use with caution as this will invalidate any volatile tables.
        """
        if self.connection:
            logger.warning("Forcing database reconnection - volatile tables will be lost")
            try:
                self.connection.close()
            except Exception as e:
                logger.error(f"Error during forced reconnection: {str(e)}")
            finally:
                self.connection = None
        
        self.connection_attempts = 0
    
    def _establish_connection(self) -> None:
        """
        Establishes database connection with retry logic.
        
        Raises:
            DatabaseConnectionError: If connection cannot be established after retries
        """
        self.connection_attempts += 1
        
        if self.connection_attempts > self.max_connection_attempts:
            raise DatabaseConnectionError(
                f"Failed to establish connection after {self.max_connection_attempts} attempts"
            )
        
        try:
            host = self.connection_params.get('host', 'unknown')
            logger.info(f"Establishing database connection to {host} (attempt {self.connection_attempts})")
            
            self.connection = teradatasql.connect(**self.connection_params)
            
            # Verify connection with test query
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP")
            timestamp = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Database connection established successfully at {timestamp}")
            
        except Exception as e:
            error_msg = f"Failed to establish database connection: {str(e)}"
            logger.error(error_msg)
            
            self.connection = None
            
            if self.connection_attempts < self.max_connection_attempts:
                logger.info(f"Retrying connection (attempt {self.connection_attempts + 1}/{self.max_connection_attempts})")
                self._establish_connection()
            else:
                raise DatabaseConnectionError(error_msg) from e
    
    def _validate_connection_parameters(self, params: Dict[str, str]) -> None:
        """
        Validates required connection parameters are present.
        
        Args:
            params: Connection parameters to validate
            
        Raises:
            ValueError: If required parameters are missing
        """
        required_params = ['host', 'user', 'password']
        missing_params = [param for param in required_params if not params.get(param)]
        
        if missing_params:
            raise ValueError(f"Missing required connection parameters: {missing_params}")
        
        # Log connection parameters (excluding sensitive information)
        safe_params = {k: v for k, v in params.items() if k not in ['password']}
        logger.debug(f"Connection parameters validated: {safe_params}")
    
    def __enter__(self):
        """Context manager entry - returns self for connection access."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup is called."""
        self.cleanup()
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        status = "active" if self.is_connection_active() else "inactive"
        return (f"SharedConnectionManager(host={self.connection_params.get('host')}, "
                f"status={status}, components={len(self.active_components)})")


class ConnectionPool:
    """
    Simple connection pool for multiple independent connections.
    
    Use when components need separate connections rather than shared volatile tables.
    """
    
    def __init__(self, connection_parameters: Dict[str, str], pool_size: int = 5):
        """
        Initialize connection pool.
        
        Args:
            connection_parameters: Database connection configuration
            pool_size: Maximum number of connections in pool
        """
        self.connection_params = connection_parameters
        self.pool_size = pool_size
        self.available_connections: list = []
        self.used_connections: Set[teradatasql.TeradataConnection] = set()
        
        logger.debug(f"Connection pool initialized with size {pool_size}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting pooled connection.
        
        Yields:
            Database connection from pool
        """
        connection = self._acquire_connection()
        try:
            yield connection
        finally:
            self._release_connection(connection)
    
    def _acquire_connection(self) -> teradatasql.TeradataConnection:
        """Gets connection from pool or creates new one."""
        if self.available_connections:
            connection = self.available_connections.pop()
            logger.debug("Reusing pooled connection")
        else:
            connection = teradatasql.connect(**self.connection_params)
            logger.debug("Created new pooled connection")
        
        self.used_connections.add(connection)
        return connection
    
    def _release_connection(self, connection: teradatasql.TeradataConnection) -> None:
        """Returns connection to pool."""
        if connection in self.used_connections:
            self.used_connections.remove(connection)
            
            if len(self.available_connections) < self.pool_size:
                self.available_connections.append(connection)
                logger.debug("Connection returned to pool")
            else:
                connection.close()
                logger.debug("Pool full - connection closed")
    
    def cleanup(self) -> None:
        """Closes all pooled connections."""
        for connection in self.available_connections:
            try:
                connection.close()
            except Exception as e:
                logger.warning(f"Error closing pooled connection: {str(e)}")
        
        for connection in self.used_connections:
            try:
                connection.close()
            except Exception as e:
                logger.warning(f"Error closing used connection: {str(e)}")
        
        self.available_connections.clear()
        self.used_connections.clear()
        logger.info("Connection pool cleanup completed")