"""
Base KPI Calculator class providing common functionality.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from abc import ABC, abstractmethod
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from common.logger import setup_logger
from common.utils import get_date_range_last_n_days, safe_numeric_conversion

logger = setup_logger(__name__)


class BaseKPICalculator(ABC):
    """
    Base class for all KPI calculators providing common functionality.
    """
    
    def __init__(self, customers_df: pd.DataFrame, orders_df: pd.DataFrame):
        """
        Initialize KPI calculator with data.
        
        Args:
            customers_df: DataFrame containing customer data
            orders_df: DataFrame containing order data
        """
        self.customers_df = customers_df.copy() if customers_df is not None else pd.DataFrame()
        self.orders_df = orders_df.copy() if orders_df is not None else pd.DataFrame()
        self._enriched_df: Optional[pd.DataFrame] = None
        
        # Validate data
        self._validate_data()
        
        logger.info(f"Initialized {self.__class__.__name__} with {len(self.customers_df)} customers and {len(self.orders_df)} orders")
    
    def _validate_data(self) -> bool:
        """Validate input data structure."""
        try:
            # Check if DataFrames are empty
            if self.customers_df.empty:
                logger.warning("Customer DataFrame is empty")
                return False
            
            if self.orders_df.empty:
                logger.warning("Orders DataFrame is empty")
                return False
            
            # Check required columns for customers
            required_customer_cols = ['customer_id', 'customer_name', 'mobile_number', 'region']
            missing_customer_cols = [col for col in required_customer_cols if col not in self.customers_df.columns]
            if missing_customer_cols:
                logger.error(f"Missing customer columns: {missing_customer_cols}")
                return False
            
            # Check required columns for orders
            required_order_cols = ['order_id', 'mobile_number', 'order_date_time', 'total_amount']
            missing_order_cols = [col for col in required_order_cols if col not in self.orders_df.columns]
            if missing_order_cols:
                logger.error(f"Missing order columns: {missing_order_cols}")
                return False
            
            # Validate data types and convert if necessary
            self._normalize_data_types()
            
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            return False
    
    def _normalize_data_types(self):
        """Normalize data types for consistent processing."""
        try:
            # Ensure order_date_time is datetime
            if 'order_date_time' in self.orders_df.columns:
                if not pd.api.types.is_datetime64_any_dtype(self.orders_df['order_date_time']):
                    self.orders_df['order_date_time'] = pd.to_datetime(
                        self.orders_df['order_date_time'], 
                        errors='coerce'
                    )
            
            # Ensure total_amount is numeric
            if 'total_amount' in self.orders_df.columns:
                self.orders_df['total_amount'] = self.orders_df['total_amount'].apply(
                    lambda x: safe_numeric_conversion(x, 0.0)
                )
            
            # Ensure sku_count is numeric if present
            if 'sku_count' in self.orders_df.columns:
                self.orders_df['sku_count'] = self.orders_df['sku_count'].apply(
                    lambda x: safe_numeric_conversion(x, 1)
                )
            
            logger.debug("Data types normalized successfully")
            
        except Exception as e:
            logger.error(f"Data type normalization failed: {str(e)}")
    
    def get_enriched_dataframe(self) -> pd.DataFrame:
        """
        Get enriched dataframe with joined customer and order data.
        
        Returns:
            DataFrame with combined customer and order information
        """
        if self._enriched_df is None:
            try:
                # Merge customers and orders on mobile_number
                self._enriched_df = pd.merge(
                    self.orders_df,
                    self.customers_df,
                    on='mobile_number',
                    how='left',
                    suffixes=('_order', '_customer')
                )
                
                # Add derived columns
                current_time = pd.Timestamp.now()
                self._enriched_df['days_since_order'] = (
                    current_time - self._enriched_df['order_date_time']
                ).dt.days
                
                # Add month-year for trend analysis
                self._enriched_df['order_month'] = self._enriched_df['order_date_time'].dt.to_period('M')
                self._enriched_df['order_year'] = self._enriched_df['order_date_time'].dt.year
                
                logger.info(f"Created enriched dataframe with {len(self._enriched_df)} records")
                
            except Exception as e:
                logger.error(f"Failed to create enriched dataframe: {str(e)}")
                self._enriched_df = pd.DataFrame()
        
        return self._enriched_df
    
    def filter_orders_by_date_range(self, days_back: int) -> pd.DataFrame:
        """
        Filter orders within the last N days.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Filtered DataFrame with recent orders
        """
        try:
            start_date, end_date = get_date_range_last_n_days(days_back)
            
            # Convert to pandas datetime for comparison
            start_date_pd = pd.Timestamp(start_date)
            end_date_pd = pd.Timestamp(end_date)
            
            filtered_df = self.orders_df[
                (self.orders_df['order_date_time'] >= start_date_pd) &
                (self.orders_df['order_date_time'] <= end_date_pd)
            ].copy()
            
            logger.debug(f"Filtered {len(filtered_df)} orders from last {days_back} days")
            return filtered_df
            
        except Exception as e:
            logger.error(f"Date range filtering failed: {str(e)}")
            return pd.DataFrame()
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get basic summary statistics of the data."""
        try:
            enriched_df = self.get_enriched_dataframe()
            
            if enriched_df.empty:
                return {}
            
            stats = {
                'total_customers': len(self.customers_df),
                'total_orders': len(self.orders_df),
                'total_revenue': enriched_df['total_amount'].sum(),
                'avg_order_value': enriched_df['total_amount'].mean(),
                'date_range': {
                    'start': enriched_df['order_date_time'].min().isoformat() if not enriched_df['order_date_time'].isna().all() else None,
                    'end': enriched_df['order_date_time'].max().isoformat() if not enriched_df['order_date_time'].isna().all() else None
                },
                'regions': enriched_df['region'].nunique() if 'region' in enriched_df.columns else 0
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to generate summary stats: {str(e)}")
            return {}
    
    @abstractmethod
    def calculate(self) -> Dict[str, Any]:
        """
        Abstract method to calculate specific KPI.
        Must be implemented by subclasses.
        
        Returns:
            Dictionary containing KPI results
        """
        pass
    
    def validate_results(self, results: Dict[str, Any]) -> bool:
        """
        Validate KPI calculation results.
        
        Args:
            results: Dictionary containing KPI results
            
        Returns:
            True if results are valid, False otherwise
        """
        try:
            if not isinstance(results, dict):
                logger.error("Results must be a dictionary")
                return False
            
            if not results:
                logger.warning("Results dictionary is empty")
                return True  # Empty results might be valid in some cases
            
            return True
            
        except Exception as e:
            logger.error(f"Result validation failed: {str(e)}")
            return False
