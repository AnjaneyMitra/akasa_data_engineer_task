"""
Database setup script for the Akasa Data Engineering Pipeline.
Creates database, tables, and loads initial data.
"""

import os
import sys
from pathlib import Path

# Add paths
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / 'src'))
sys.path.append(str(project_root))

from config.database import db_config
from src.database.models import create_tables, validate_schema, get_table_stats
from src.database.operations import DatabaseOperations
from src.data_processing.data_cleaner import DataCleaner
from src.common.logger import setup_logger

logger = setup_logger(__name__)


def create_database_if_not_exists():
    """Create database if it doesn't exist."""
    try:
        import pymysql
        
        # Connect without specifying database
        connection = pymysql.connect(
            host=db_config.host,
            port=int(db_config.port),
            user=db_config.username,
            password=db_config.password
        )
        
        with connection.cursor() as cursor:
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_config.database}`")
            cursor.execute(f"USE `{db_config.database}`")
            logger.info(f"Database '{db_config.database}' created/verified successfully")
        
        connection.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {str(e)}")
        return False


def setup_database(load_data: bool = True, use_generated_data: bool = True):
    """
    Complete database setup process.
    
    Args:
        load_data: Whether to load initial data
        use_generated_data: Whether to use generated data (True) or original data (False)
    """
    logger.info("Starting database setup process...")
    
    # Step 1: Create database
    if not create_database_if_not_exists():
        logger.error("Database creation failed")
        return False
    
    # Step 2: Test connection
    if not db_config.test_connection():
        logger.error("Database connection test failed")
        return False
    
    # Step 3: Create engine and tables
    try:
        engine = db_config.create_engine()
        if engine is None:
            logger.error("Failed to create database engine")
            return False
        
        # Create tables with indexes
        create_tables(engine, drop_existing=True)
        
        # Validate schema
        if not validate_schema(engine):
            logger.error("Schema validation failed")
            return False
        
        logger.info("Database tables created successfully")
        
    except Exception as e:
        logger.error(f"Table creation failed: {str(e)}")
        return False
    
    # Step 4: Load initial data
    if load_data:
        success = load_initial_data(use_generated_data)
        if not success:
            logger.warning("Data loading failed, but database setup completed")
    
    logger.info("Database setup completed successfully")
    return True


def load_initial_data(use_generated_data: bool = True) -> bool:
    """
    Load initial customer and order data.
    
    Args:
        use_generated_data: Whether to use generated data or original sample data
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("Loading initial data...")
        
        # Initialize data processor
        data_cleaner = DataCleaner()
        
        # Determine data files to use
        if use_generated_data:
            customer_file = "data/raw/generated_customers.csv"
            orders_file = "data/raw/generated_orders.xml"
            logger.info("Using generated comprehensive data")
        else:
            customer_file = "data/raw/task_DE_new_customers.csv"
            orders_file = "data/raw/task_DE_new_orders.xml"
            logger.info("Using original sample data")
        
        # Process data
        if not data_cleaner.process_customer_data(customer_file):
            logger.error("Failed to process customer data")
            return False
        
        if not data_cleaner.process_order_data(orders_file):
            logger.error("Failed to process order data")
            return False
        
        # Get processed data
        customers_df = data_cleaner.get_customers_dataframe()
        orders_df = data_cleaner.get_orders_dataframe()
        
        if customers_df is None or orders_df is None:
            logger.error("Processed data is None")
            return False
        
        # Load into database
        session = db_config.get_session()
        if session is None:
            logger.error("Failed to create database session")
            return False
        
        try:
            db_ops = DatabaseOperations(session)
            
            # Insert customers
            success, errors = db_ops.bulk_insert_customers(customers_df)
            if not success:
                logger.error(f"Customer insert failed: {errors}")
                return False
            
            # Insert orders
            success, errors = db_ops.bulk_insert_orders(orders_df)
            if not success:
                logger.error(f"Order insert failed: {errors}")
                return False
            
            # Get and log statistics
            stats = get_table_stats(session)
            logger.info("Data loading statistics:")
            logger.info(f"  Customers: {stats.get('customers', {}).get('total_count', 0)}")
            logger.info(f"  Orders: {stats.get('orders', {}).get('total_count', 0)}")
            
            return True
            
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        return False


def verify_setup():
    """Verify database setup is working correctly."""
    logger.info("Verifying database setup...")
    
    try:
        session = db_config.get_session()
        if session is None:
            logger.error("Failed to create session for verification")
            return False
        
        try:
            db_ops = DatabaseOperations(session)
            
            # Test basic queries
            repeat_customers = db_ops.get_repeat_customers()
            monthly_trends = db_ops.get_monthly_order_trends()
            regional_revenue = db_ops.get_regional_revenue()
            top_customers = db_ops.get_top_customers_last_n_days(30)
            
            logger.info("Verification results:")
            logger.info(f"  Repeat customers found: {len(repeat_customers)}")
            logger.info(f"  Monthly trend periods: {len(monthly_trends)}")
            logger.info(f"  Regions with revenue: {len(regional_revenue)}")
            logger.info(f"  Top customers (30 days): {len(top_customers)}")
            
            # Get database summary
            summary = db_ops.get_database_summary()
            logger.info("Database summary:")
            for key, value in summary.items():
                logger.info(f"  {key}: {value}")
            
            return True
            
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Verification failed: {str(e)}")
        return False


def main():
    """Main setup function."""
    logger.info("=== Akasa Data Engineering Pipeline - Database Setup ===")
    
    # Check environment variables
    if not db_config.username:
        logger.error("Database username not found in environment variables")
        logger.info("Please ensure DB_USERNAME is set in .env file")
        return False
    
    if db_config.password is None:
        logger.warning("No password set for database user (using empty password)")
    
    # Setup database
    success = setup_database(load_data=True, use_generated_data=True)
    if not success:
        logger.error("Database setup failed")
        return False
    
    # Verify setup
    success = verify_setup()
    if not success:
        logger.error("Database verification failed")
        return False
    
    logger.info("=== Database setup completed successfully! ===")
    return True


if __name__ == "__main__":
    main()
