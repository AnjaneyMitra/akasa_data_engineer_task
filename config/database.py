"""
Database configuration for the Akasa Data Engineering Pipeline.
Provides secure database connection management with environment variable support.
"""

import os
from typing import Optional
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from common.logger import setup_logger

# Load environment variables
load_dotenv()

logger = setup_logger(__name__)


class DatabaseConfig:
    """Database configuration and connection management."""
    
    def __init__(self):
        """Initialize database configuration from environment variables."""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '3306')
        self.username = os.getenv('DB_USERNAME')
        self.password = os.getenv('DB_PASSWORD')
        self.database = os.getenv('DB_DATABASE', 'akasa_pipeline')
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    def _validate_config(self) -> bool:
        """Validate database configuration."""
        if not self.username or not self.password:
            logger.error("Database username and password must be set in environment variables")
            return False
        
        return True
    
    def get_connection_string(self, mask_password: bool = True) -> str:
        """
        Get database connection string.
        
        Args:
            mask_password: Whether to mask password for logging
            
        Returns:
            Database connection string
        """
        if mask_password:
            masked_password = '*' * len(self.password) if self.password else 'None'
            return f"mysql+pymysql://{self.username}:{masked_password}@{self.host}:{self.port}/{self.database}"
        
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def create_engine(self, **kwargs) -> Optional[Engine]:
        """
        Create database engine with proper configuration.
        
        Args:
            **kwargs: Additional engine parameters
            
        Returns:
            SQLAlchemy Engine or None if creation fails
        """
        if not self._validate_config():
            return None
        
        try:
            # Default engine parameters for production use
            default_params = {
                'pool_size': 10,
                'max_overflow': 20,
                'pool_pre_ping': True,
                'pool_recycle': 3600,
                'echo': False  # Set to True for SQL debugging
            }
            
            # Override with user parameters
            default_params.update(kwargs)
            
            connection_string = self.get_connection_string(mask_password=False)
            self._engine = create_engine(connection_string, **default_params)
            
            # Test connection
            with self._engine.connect() as conn:
                conn.execute("SELECT 1")
            
            logger.info(f"Database engine created successfully: {self.get_connection_string()}")
            return self._engine
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating database engine: {str(e)}")
            return None
    
    def get_engine(self) -> Optional[Engine]:
        """Get existing engine or create new one."""
        if self._engine is None:
            return self.create_engine()
        return self._engine
    
    def create_session_factory(self) -> Optional[sessionmaker]:
        """Create session factory for database operations."""
        engine = self.get_engine()
        if engine is None:
            return None
        
        self._session_factory = sessionmaker(bind=engine)
        logger.info("Database session factory created successfully")
        return self._session_factory
    
    def get_session(self) -> Optional[Session]:
        """
        Get database session.
        
        Returns:
            SQLAlchemy Session or None if creation fails
        """
        try:
            if self._session_factory is None:
                self.create_session_factory()
            
            if self._session_factory is None:
                logger.error("Failed to create session factory")
                return None
            
            return self._session_factory()
            
        except Exception as e:
            logger.error(f"Failed to create database session: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = self.get_engine()
            if engine is None:
                return False
            
            with engine.connect() as conn:
                result = conn.execute("SELECT 1 as test").fetchone()
                if result and result.test == 1:
                    logger.info("Database connection test successful")
                    return True
                else:
                    logger.error("Database connection test failed: unexpected result")
                    return False
                    
        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    def close_connections(self):
        """Close all database connections."""
        try:
            if self._engine:
                self._engine.dispose()
                logger.info("Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")


# Global database configuration instance
db_config = DatabaseConfig()
