"""
Database operations for loading and managing customer and order data.
Provides secure, parameterized queries and bulk operations for performance.
"""

from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, func
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))

from common.logger import setup_logger
from database.models import Customer, Order, KPISummary

logger = setup_logger(__name__)


class DatabaseOperations:
    """Handle all database operations with security and performance considerations."""
    
    def __init__(self, session: Session):
        """
        Initialize database operations.
        
        Args:
            session: SQLAlchemy session
        """
        self.session = session
    
    # Customer Operations
    def bulk_insert_customers(self, customers_df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Bulk insert customers with conflict handling.
        
        Args:
            customers_df: DataFrame with customer data
            
        Returns:
            Tuple of (success, error_messages)
        """
        errors = []
        success_count = 0
        
        try:
            for _, row in customers_df.iterrows():
                try:
                    # Check if customer already exists
                    existing = self.session.query(Customer).filter(
                        Customer.customer_id == row['customer_id']
                    ).first()
                    
                    if existing:
                        # Update existing customer
                        existing.customer_name = row['customer_name']
                        existing.mobile_number = row['mobile_number']
                        existing.region = row['region']
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new customer
                        customer = Customer(
                            customer_id=row['customer_id'],
                            customer_name=row['customer_name'],
                            mobile_number=row['mobile_number'],
                            region=row['region']
                        )
                        self.session.add(customer)
                    
                    success_count += 1
                    
                except IntegrityError as e:
                    error_msg = f"Integrity error for customer {row.get('customer_id', 'unknown')}: {str(e)}"
                    errors.append(error_msg)
                    logger.warning(error_msg)
                    self.session.rollback()
                    
                except Exception as e:
                    error_msg = f"Error inserting customer {row.get('customer_id', 'unknown')}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    self.session.rollback()
            
            # Commit all changes
            if success_count > 0:
                self.session.commit()
                logger.info(f"Successfully inserted/updated {success_count} customers")
                return True, errors
            else:
                return False, errors if errors else ["No customers to insert"]
                
        except Exception as e:
            self.session.rollback()
            error_msg = f"Bulk customer insert failed: {str(e)}"
            logger.error(error_msg)
            return False, [error_msg]
    
    def bulk_insert_orders(self, orders_df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Bulk insert orders with foreign key validation.
        
        Args:
            orders_df: DataFrame with order data
            
        Returns:
            Tuple of (success, error_messages)
        """
        errors = []
        success_count = 0
        
        try:
            for _, row in orders_df.iterrows():
                try:
                    # Validate customer exists
                    customer_exists = self.session.query(Customer).filter(
                        Customer.mobile_number == row['mobile_number']
                    ).first()
                    
                    if not customer_exists:
                        error_msg = f"Customer with mobile {row['mobile_number']} not found for order {row['order_id']}"
                        errors.append(error_msg)
                        logger.warning(error_msg)
                        continue
                    
                    # Check if order already exists
                    existing = self.session.query(Order).filter(
                        Order.order_id == row['order_id']
                    ).first()
                    
                    if existing:
                        # Update existing order
                        existing.mobile_number = row['mobile_number']
                        existing.order_date_time = row['order_date_time']
                        existing.sku_id = row['sku_id']
                        existing.sku_count = row['sku_count']
                        existing.total_amount = row['total_amount']
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new order
                        order = Order(
                            order_id=row['order_id'],
                            mobile_number=row['mobile_number'],
                            order_date_time=row['order_date_time'],
                            sku_id=row['sku_id'],
                            sku_count=row['sku_count'],
                            total_amount=row['total_amount']
                        )
                        self.session.add(order)
                    
                    success_count += 1
                    
                except IntegrityError as e:
                    error_msg = f"Integrity error for order {row.get('order_id', 'unknown')}: {str(e)}"
                    errors.append(error_msg)
                    logger.warning(error_msg)
                    self.session.rollback()
                    
                except Exception as e:
                    error_msg = f"Error inserting order {row.get('order_id', 'unknown')}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    self.session.rollback()
            
            # Commit all changes
            if success_count > 0:
                self.session.commit()
                logger.info(f"Successfully inserted/updated {success_count} orders")
                return True, errors
            else:
                return False, errors if errors else ["No orders to insert"]
                
        except Exception as e:
            self.session.rollback()
            error_msg = f"Bulk order insert failed: {str(e)}"
            logger.error(error_msg)
            return False, [error_msg]
    
    # KPI Query Methods (using parameterized queries for security)
    def get_repeat_customers(self) -> List[Dict[str, Any]]:
        """
        Get customers who have placed more than one order.
        Uses parameterized query for security.
        
        Returns:
            List of customer dictionaries with order counts
        """
        try:
            # Using parameterized query with SQLAlchemy ORM
            query = self.session.query(
                Customer.customer_id,
                Customer.customer_name,
                Customer.mobile_number,
                Customer.region,
                func.count(Order.order_id).label('order_count'),
                func.sum(Order.total_amount).label('total_spent')
            ).join(
                Order, Customer.mobile_number == Order.mobile_number
            ).group_by(
                Customer.customer_id,
                Customer.customer_name,
                Customer.mobile_number,
                Customer.region
            ).having(
                func.count(Order.order_id) > 1
            ).order_by(
                func.count(Order.order_id).desc()
            )
            
            results = []
            for row in query.all():
                results.append({
                    'customer_id': row.customer_id,
                    'customer_name': row.customer_name,
                    'mobile_number': row.mobile_number,
                    'region': row.region,
                    'order_count': row.order_count,
                    'total_spent': float(row.total_spent)
                })
            
            logger.info(f"Found {len(results)} repeat customers")
            return results
            
        except Exception as e:
            logger.error(f"Error getting repeat customers: {str(e)}")
            return []
    
    def get_monthly_order_trends(self) -> List[Dict[str, Any]]:
        """
        Get order trends aggregated by month.
        
        Returns:
            List of monthly trend data
        """
        try:
            # Using SQLAlchemy functions for date manipulation
            query = self.session.query(
                func.year(Order.order_date_time).label('year'),
                func.month(Order.order_date_time).label('month'),
                func.count(Order.order_id).label('order_count'),
                func.sum(Order.total_amount).label('total_revenue'),
                func.count(func.distinct(Order.mobile_number)).label('unique_customers')
            ).group_by(
                func.year(Order.order_date_time),
                func.month(Order.order_date_time)
            ).order_by(
                func.year(Order.order_date_time),
                func.month(Order.order_date_time)
            )
            
            results = []
            for row in query.all():
                month_name = datetime(row.year, row.month, 1).strftime('%B %Y')
                results.append({
                    'year': row.year,
                    'month': row.month,
                    'month_name': month_name,
                    'order_count': row.order_count,
                    'total_revenue': float(row.total_revenue),
                    'unique_customers': row.unique_customers,
                    'avg_order_value': float(row.total_revenue / row.order_count) if row.order_count > 0 else 0
                })
            
            logger.info(f"Retrieved monthly trends for {len(results)} months")
            return results
            
        except Exception as e:
            logger.error(f"Error getting monthly order trends: {str(e)}")
            return []
    
    def get_regional_revenue(self) -> List[Dict[str, Any]]:
        """
        Get revenue breakdown by region.
        
        Returns:
            List of regional revenue data
        """
        try:
            query = self.session.query(
                Customer.region,
                func.count(func.distinct(Customer.customer_id)).label('customer_count'),
                func.count(Order.order_id).label('order_count'),
                func.sum(Order.total_amount).label('total_revenue'),
                func.avg(Order.total_amount).label('avg_order_value')
            ).join(
                Order, Customer.mobile_number == Order.mobile_number
            ).group_by(
                Customer.region
            ).order_by(
                func.sum(Order.total_amount).desc()
            )
            
            results = []
            total_revenue = 0
            
            for row in query.all():
                revenue = float(row.total_revenue)
                total_revenue += revenue
                
                results.append({
                    'region': row.region,
                    'customer_count': row.customer_count,
                    'order_count': row.order_count,
                    'total_revenue': revenue,
                    'avg_order_value': float(row.avg_order_value)
                })
            
            # Add revenue percentage
            for result in results:
                result['revenue_percentage'] = (result['total_revenue'] / total_revenue * 100) if total_revenue > 0 else 0
            
            logger.info(f"Retrieved regional revenue data for {len(results)} regions")
            return results
            
        except Exception as e:
            logger.error(f"Error getting regional revenue: {str(e)}")
            return []
    
    def get_top_customers_last_n_days(self, n_days: int = 30, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top customers by spend in the last N days.
        Uses parameterized query for date filtering.
        
        Args:
            n_days: Number of days to look back
            limit: Maximum number of customers to return
            
        Returns:
            List of top customer data
        """
        try:
            # Calculate date threshold using parameterized approach
            cutoff_date = datetime.now() - timedelta(days=n_days)
            
            query = self.session.query(
                Customer.customer_id,
                Customer.customer_name,
                Customer.mobile_number,
                Customer.region,
                func.count(Order.order_id).label('recent_order_count'),
                func.sum(Order.total_amount).label('recent_total_spent'),
                func.max(Order.order_date_time).label('last_order_date')
            ).join(
                Order, Customer.mobile_number == Order.mobile_number
            ).filter(
                Order.order_date_time >= cutoff_date
            ).group_by(
                Customer.customer_id,
                Customer.customer_name,
                Customer.mobile_number,
                Customer.region
            ).order_by(
                func.sum(Order.total_amount).desc()
            ).limit(limit)
            
            results = []
            for row in query.all():
                results.append({
                    'customer_id': row.customer_id,
                    'customer_name': row.customer_name,
                    'mobile_number': row.mobile_number,
                    'region': row.region,
                    'recent_order_count': row.recent_order_count,
                    'recent_total_spent': float(row.recent_total_spent),
                    'last_order_date': row.last_order_date.isoformat(),
                    'avg_order_value': float(row.recent_total_spent / row.recent_order_count)
                })
            
            logger.info(f"Retrieved top {len(results)} customers for last {n_days} days")
            return results
            
        except Exception as e:
            logger.error(f"Error getting top customers: {str(e)}")
            return []
    
    # Utility Methods
    def get_database_summary(self) -> Dict[str, Any]:
        """Get comprehensive database summary statistics."""
        try:
            summary = {}
            
            # Customer statistics
            customer_count = self.session.query(Customer).count()
            region_stats = self.session.query(
                Customer.region,
                func.count(Customer.customer_id).label('count')
            ).group_by(Customer.region).all()
            
            summary['customers'] = {
                'total_count': customer_count,
                'by_region': {row.region: row.count for row in region_stats}
            }
            
            # Order statistics
            order_count = self.session.query(Order).count()
            total_revenue = self.session.query(func.sum(Order.total_amount)).scalar() or 0
            
            summary['orders'] = {
                'total_count': order_count,
                'total_revenue': float(total_revenue),
                'avg_order_value': float(total_revenue / order_count) if order_count > 0 else 0
            }
            
            # Date range
            date_range = self.session.query(
                func.min(Order.order_date_time).label('earliest'),
                func.max(Order.order_date_time).label('latest')
            ).first()
            
            if date_range and date_range.earliest:
                summary['date_range'] = {
                    'earliest_order': date_range.earliest.isoformat(),
                    'latest_order': date_range.latest.isoformat()
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting database summary: {str(e)}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """
        Clean up old data beyond specified days.
        
        Args:
            days_to_keep: Number of days of data to retain
            
        Returns:
            Number of records deleted
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Delete old orders
            deleted_orders = self.session.query(Order).filter(
                Order.order_date_time < cutoff_date
            ).delete()
            
            # Delete customers with no orders (optional - uncomment if needed)
            # orphaned_customers = self.session.query(Customer).filter(
            #     ~Customer.mobile_number.in_(
            #         self.session.query(Order.mobile_number).distinct()
            #     )
            # ).delete(synchronize_session=False)
            
            self.session.commit()
            logger.info(f"Cleaned up {deleted_orders} old orders")
            
            return deleted_orders
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error during data cleanup: {str(e)}")
            return 0
