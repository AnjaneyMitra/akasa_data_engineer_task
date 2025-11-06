"""
Database configuration for the Akasa Data Engineering Pipeline.
Simple, robust database connection management.
"""

import os
from typing import Optional
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup simple logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Simple database configuration and connection management."""
    
    def __init__(self):
        """Initialize database configuration from environment variables."""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = int(os.getenv('DB_PORT', 3306))
        self.username = os.getenv('DB_USERNAME', 'root')
        self.password = os.getenv('DB_PASSWORD', '')
        self.database = os.getenv('DB_DATABASE', 'akasa_pipeline')
        
        self._engine = None
        self._session_factory = None
        
        logger.info(f"Database config initialized: {self.username}@{self.host}:{self.port}/{self.database}")
    
    def get_connection_string(self, include_password: bool = False) -> str:
        """Get MySQL connection string."""
        if include_password:
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            # Masked for logging
            masked_pwd = '*' * len(self.password) if self.password else '[empty]'
            return f"mysql+pymysql://{self.username}:{masked_pwd}@{self.host}:{self.port}/{self.database}"
    
    def create_engine(self) -> Optional[Engine]:
        """Create database engine."""
        try:
            connection_string = self.get_connection_string(include_password=True)
            
            self._engine = create_engine(
                connection_string,
                echo=False,  # Set to True for SQL debugging
                pool_recycle=3600,
                pool_pre_ping=True
            )
            
            # Test the connection
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                test_result = result.fetchone()
                if test_result and test_result[0] == 1:
                    logger.info("Database connection successful!")
                    return self._engine
                else:
                    logger.error("Database connection test failed")
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            return None
    
    def get_engine(self) -> Optional[Engine]:
        """Get database engine, create if doesn't exist."""
        if self._engine is None:
            self._engine = self.create_engine()
        return self._engine
    
    def get_session(self) -> Optional[Session]:
        """Get database session."""
        try:
            engine = self.get_engine()
            if engine is None:
                return None
                
            if self._session_factory is None:
                self._session_factory = sessionmaker(bind=engine)
            
            return self._session_factory()
            
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            session = self.get_session()
            if session is None:
                return False
            
            # Test query
            result = session.execute(text("SELECT 1 as test"))
            test_result = result.fetchone()
            session.close()
            
            if test_result and test_result[0] == 1:
                logger.info("Database connection test: PASSED")
                return True
            else:
                logger.error("Database connection test: FAILED")
                return False
                
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False


# Global database configuration instance
db_config = DatabaseConfig()


def get_db_session():
    """Helper function to get database session."""
    return db_config.get_session()


def test_db_connection():
    """Helper function to test database connection."""
    return db_config.test_connection()
