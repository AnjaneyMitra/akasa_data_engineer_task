"""
SQLAlchemy models for customers and orders data.
Defines database schema with proper indexes and constraints for optimal performance.
"""

from datetime import datetime
from typing import List, Optional
from sqlalchemy import (
    Column, String, Integer, DateTime, Float, 
    Index, ForeignKey, create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))

from common.logger import setup_logger

logger = setup_logger(__name__)

# Create declarative base
Base = declarative_base()


class Customer(Base):
    """Customer model with regional information."""
    
    __tablename__ = 'customers'
    
    # Primary key
    customer_id = Column(String(20), primary_key=True, index=True)
    
    # Customer information
    customer_name = Column(String(100), nullable=False, index=True)
    mobile_number = Column(String(15), nullable=False, unique=True, index=True)
    region = Column(String(50), nullable=False, index=True)
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    orders = relationship("Order", back_populates="customer", cascade="all, delete-orphan")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_customer_region_name', 'region', 'customer_name'),
        Index('idx_customer_mobile_region', 'mobile_number', 'region'),
    )
    
    def __repr__(self):
        return f"<Customer(id='{self.customer_id}', name='{self.customer_name}', region='{self.region}')>"
    
    def to_dict(self) -> dict:
        """Convert customer to dictionary."""
        return {
            'customer_id': self.customer_id,
            'customer_name': self.customer_name,
            'mobile_number': self.mobile_number,
            'region': self.region,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


class Order(Base):
    """Order model with SKU and amount information."""
    
    __tablename__ = 'orders'
    
    # Primary key
    order_id = Column(String(20), primary_key=True, index=True)
    
    # Foreign key to customer (via mobile_number for flexibility)
    mobile_number = Column(String(15), ForeignKey('customers.mobile_number'), nullable=False, index=True)
    
    # Order information
    order_date_time = Column(DateTime, nullable=False, index=True)
    sku_id = Column(String(20), nullable=False, index=True)
    sku_count = Column(Integer, nullable=False)
    total_amount = Column(Float, nullable=False, index=True)
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    
    # Indexes for KPI performance
    __table_args__ = (
        Index('idx_order_date_amount', 'order_date_time', 'total_amount'),
        Index('idx_order_mobile_date', 'mobile_number', 'order_date_time'),
        Index('idx_order_sku_date', 'sku_id', 'order_date_time'),
        Index('idx_order_date_desc', 'order_date_time DESC'),  # For recent orders
    )
    
    def __repr__(self):
        return f"<Order(id='{self.order_id}', mobile='{self.mobile_number}', amount={self.total_amount})>"
    
    def to_dict(self) -> dict:
        """Convert order to dictionary."""
        return {
            'order_id': self.order_id,
            'mobile_number': self.mobile_number,
            'order_date_time': self.order_date_time.isoformat() if self.order_date_time else None,
            'sku_id': self.sku_id,
            'sku_count': self.sku_count,
            'total_amount': self.total_amount,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


class KPISummary(Base):
    """Model to store precomputed KPI results for caching."""
    
    __tablename__ = 'kpi_summary'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # KPI identification
    kpi_name = Column(String(100), nullable=False, index=True)
    calculation_date = Column(DateTime, nullable=False, index=True)
    parameters = Column(String(500))  # JSON string of calculation parameters
    
    # Results
    result_count = Column(Integer)
    result_value = Column(Float)
    result_json = Column(String(2000))  # JSON string for complex results
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Indexes
    __table_args__ = (
        Index('idx_kpi_name_date', 'kpi_name', 'calculation_date'),
    )
    
    def __repr__(self):
        return f"<KPISummary(name='{self.kpi_name}', date='{self.calculation_date}')>"
    
    def to_dict(self) -> dict:
        """Convert KPI summary to dictionary."""
        return {
            'id': self.id,
            'kpi_name': self.kpi_name,
            'calculation_date': self.calculation_date.isoformat() if self.calculation_date else None,
            'parameters': self.parameters,
            'result_count': self.result_count,
            'result_value': self.result_value,
            'result_json': self.result_json,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


def create_tables(engine, drop_existing: bool = False):
    """
    Create all database tables.
    
    Args:
        engine: SQLAlchemy engine
        drop_existing: Whether to drop existing tables first
    """
    try:
        if drop_existing:
            logger.warning("Dropping existing tables...")
            Base.metadata.drop_all(engine)
        
        logger.info("Creating database tables...")
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
        
        # Log table information
        logger.info("Created tables:")
        for table_name in Base.metadata.tables.keys():
            logger.info(f"  - {table_name}")
            
    except Exception as e:
        logger.error(f"Failed to create database tables: {str(e)}")
        raise


def get_table_stats(session: Session) -> dict:
    """
    Get statistics about tables.
    
    Args:
        session: Database session
        
    Returns:
        Dictionary with table statistics
    """
    try:
        stats = {}
        
        # Customer statistics
        customer_count = session.query(Customer).count()
        region_counts = session.query(Customer.region).distinct().count()
        
        stats['customers'] = {
            'total_count': customer_count,
            'unique_regions': region_counts
        }
        
        # Order statistics
        order_count = session.query(Order).count()
        unique_customers_with_orders = session.query(Order.mobile_number).distinct().count()
        
        stats['orders'] = {
            'total_count': order_count,
            'unique_customers': unique_customers_with_orders
        }
        
        # KPI summary statistics
        kpi_count = session.query(KPISummary).count()
        stats['kpi_summary'] = {
            'total_count': kpi_count
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get table statistics: {str(e)}")
        return {}


def validate_schema(engine) -> bool:
    """
    Validate database schema matches expected structure.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        True if schema is valid, False otherwise
    """
    try:
        from sqlalchemy import inspect
        
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        expected_tables = ['customers', 'orders', 'kpi_summary']
        
        for table in expected_tables:
            if table not in existing_tables:
                logger.error(f"Missing table: {table}")
                return False
        
        logger.info("Database schema validation successful")
        return True
        
    except Exception as e:
        logger.error(f"Schema validation failed: {str(e)}")
        return False
