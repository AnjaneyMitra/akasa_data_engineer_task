"""
Top Customers KPI Calculator.
Identifies and analyzes top spending customers in the last N days.
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from .base_calculator import BaseKPICalculator

from src.common.logger import setup_logger
logger = setup_logger(__name__)


class TopCustomersCalculator(BaseKPICalculator):
    """Calculate top customers by spend KPI using pandas operations."""
    
    def __init__(self, customers_df: pd.DataFrame, orders_df: pd.DataFrame, days: int = 30):
        """
        Initialize calculator with specific time period.
        
        Args:
            customers_df: Customer data
            orders_df: Order data
            days: Number of days to look back (default: 30)
        """
        super().__init__(customers_df, orders_df)
        self.days = days
        self.cutoff_date = datetime.now() - timedelta(days=days)
    
    def calculate(self, top_n: int = 10) -> Dict[str, Any]:
        """
        Calculate top customers by spending in the last N days.
        
        Args:
            top_n: Number of top customers to return (default: 10)
            
        Returns:
            Dictionary containing:
            - top_customers: List of top customers with metrics
            - spending_summary: Overall spending analysis
            - customer_segments: Customer segmentation analysis
            - time_period_info: Information about the analysis period
        """
        try:
            if self.customers_df.empty or self.orders_df.empty:
                logger.warning("Empty data provided for top customers calculation")
                return self._empty_result(top_n)
            
            # Filter orders for the specified time period
            orders_df = self.orders_df.copy()
            
            # Ensure order_date_time is datetime
            if not pd.api.types.is_datetime64_any_dtype(orders_df['order_date_time']):
                orders_df['order_date_time'] = pd.to_datetime(orders_df['order_date_time'])
            
            # Filter by date range
            recent_orders = orders_df[orders_df['order_date_time'] >= self.cutoff_date]
            
            if recent_orders.empty:
                logger.warning(f"No orders found in the last {self.days} days")
                return self._empty_result(top_n)
            
            # Group by customer (mobile_number) and calculate spending metrics
            customer_spending = recent_orders.groupby('mobile_number').agg({
                'total_amount': ['sum', 'mean', 'count', 'std'],
                'order_date_time': ['min', 'max'],
                'sku_count': 'sum',
                'order_id': 'nunique'
            }).reset_index()
            
            # Flatten column names
            customer_spending.columns = [
                'mobile_number', 'total_spent', 'avg_order_value', 'total_orders', 
                'spending_std', 'first_order_date', 'last_order_date', 
                'total_items', 'unique_orders'
            ]
            
            # Merge with customer information
            customer_spending = pd.merge(
                customer_spending,
                self.customers_df[['mobile_number', 'customer_name', 'region', 'customer_id']],
                on='mobile_number',
                how='left'
            )
            
            # Convert categorical columns to string to avoid category issues
            if pd.api.types.is_categorical_dtype(customer_spending['region']):
                customer_spending['region'] = customer_spending['region'].astype(str)
            if pd.api.types.is_categorical_dtype(customer_spending['customer_name']):
                customer_spending['customer_name'] = customer_spending['customer_name'].astype(str)
            if pd.api.types.is_categorical_dtype(customer_spending['customer_id']):
                customer_spending['customer_id'] = customer_spending['customer_id'].astype(str)
            
            # Calculate additional metrics
            customer_spending['days_active'] = (
                customer_spending['last_order_date'] - customer_spending['first_order_date']
            ).dt.days + 1
            
            customer_spending['orders_per_day'] = (
                customer_spending['total_orders'] / customer_spending['days_active']
            )
            
            customer_spending['spending_per_day'] = (
                customer_spending['total_spent'] / customer_spending['days_active']
            )
            
            # Fill NaN values
            customer_spending = customer_spending.fillna({
                'spending_std': 0,
                'orders_per_day': 0,
                'spending_per_day': 0,
                'customer_name': 'Unknown',
                'region': 'Unknown',
                'customer_id': 'Unknown'
            })
            
            # Sort by total spent (descending) and get top N
            top_customers_df = customer_spending.nlargest(top_n, 'total_spent')
            
            # Convert to list of dictionaries
            top_customers_list = []
            for rank, (_, row) in enumerate(top_customers_df.iterrows(), 1):
                top_customers_list.append({
                    'rank': rank,
                    'customer_id': row['customer_id'],
                    'customer_name': row['customer_name'],
                    'mobile_number': row['mobile_number'],
                    'region': row['region'],
                    'total_spent': float(row['total_spent']),
                    'total_orders': int(row['total_orders']),
                    'avg_order_value': float(row['avg_order_value']),
                    'spending_std': float(row['spending_std']),
                    'total_items': int(row['total_items']),
                    'unique_orders': int(row['unique_orders']),
                    'days_active': int(row['days_active']),
                    'orders_per_day': float(row['orders_per_day']),
                    'spending_per_day': float(row['spending_per_day']),
                    'first_order_date': row['first_order_date'].isoformat(),
                    'last_order_date': row['last_order_date'].isoformat()
                })
            
            # Calculate spending summary
            spending_summary = self._calculate_spending_summary(customer_spending, recent_orders)
            
            # Calculate customer segments
            customer_segments = self._calculate_customer_segments(customer_spending)
            
            # Time period information
            time_period_info = {
                'days_analyzed': self.days,
                'cutoff_date': self.cutoff_date.isoformat(),
                'analysis_date': datetime.now().isoformat(),
                'total_customers_in_period': len(customer_spending),
                'total_orders_in_period': len(recent_orders),
                'date_range': {
                    'start': recent_orders['order_date_time'].min().isoformat(),
                    'end': recent_orders['order_date_time'].max().isoformat()
                }
            }
            
            results = {
                'top_customers': top_customers_list,
                'spending_summary': spending_summary,
                'customer_segments': customer_segments,
                'time_period_info': time_period_info,
                'calculation_date': pd.Timestamp.now().isoformat()
            }
            
            # Validate results
            if self.validate_results(results):
                logger.info(f"Top customers calculation completed: {len(top_customers_list)} customers analyzed")
                return results
            else:
                logger.error("Top customers calculation validation failed")
                return self._empty_result(top_n)
                
        except Exception as e:
            logger.error(f"Top customers calculation failed: {str(e)}")
            return self._empty_result(top_n)
    
    def _calculate_spending_summary(self, customer_spending: pd.DataFrame, recent_orders: pd.DataFrame) -> Dict[str, Any]:
        """Calculate overall spending summary statistics."""
        try:
            if customer_spending.empty:
                return {}
            
            total_revenue = customer_spending['total_spent'].sum()
            total_customers = len(customer_spending)
            total_orders = customer_spending['total_orders'].sum()
            
            # Top customer metrics
            top_customer = customer_spending.iloc[0] if not customer_spending.empty else None
            
            # Spending distribution
            spending_percentiles = customer_spending['total_spent'].quantile([0.25, 0.5, 0.75, 0.9, 0.95])
            
            # Revenue concentration (what % of revenue comes from top customers)
            top_10_pct_customers = max(1, int(len(customer_spending) * 0.1))
            top_customers_revenue = customer_spending.nlargest(top_10_pct_customers, 'total_spent')['total_spent'].sum()
            revenue_concentration = (top_customers_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            return {
                'total_revenue': float(total_revenue),
                'total_customers': int(total_customers),
                'total_orders': int(total_orders),
                'avg_revenue_per_customer': float(total_revenue / total_customers) if total_customers > 0 else 0,
                'avg_orders_per_customer': float(total_orders / total_customers) if total_customers > 0 else 0,
                'top_customer': {
                    'customer_name': top_customer['customer_name'] if top_customer is not None else '',
                    'total_spent': float(top_customer['total_spent']) if top_customer is not None else 0,
                    'total_orders': int(top_customer['total_orders']) if top_customer is not None else 0
                } if top_customer is not None else {},
                'spending_distribution': {
                    'min_spending': float(customer_spending['total_spent'].min()),
                    'max_spending': float(customer_spending['total_spent'].max()),
                    'median_spending': float(spending_percentiles[0.5]),
                    'p75_spending': float(spending_percentiles[0.75]),
                    'p90_spending': float(spending_percentiles[0.9]),
                    'p95_spending': float(spending_percentiles[0.95]),
                    'std_spending': float(customer_spending['total_spent'].std())
                },
                'revenue_concentration': {
                    'top_10_pct_customers_revenue_share': float(revenue_concentration),
                    'customers_in_top_10_pct': int(top_10_pct_customers)
                }
            }
            
        except Exception as e:
            logger.error(f"Spending summary calculation failed: {str(e)}")
            return {}
    
    def _calculate_customer_segments(self, customer_spending: pd.DataFrame) -> Dict[str, Any]:
        """Segment customers based on spending patterns."""
        try:
            if customer_spending.empty:
                return {}
            
            # Define spending segments based on percentiles
            spending_percentiles = customer_spending['total_spent'].quantile([0.8, 0.6, 0.4, 0.2])
            
            def classify_customer(spending):
                if spending >= spending_percentiles[0.8]:
                    return 'VIP'
                elif spending >= spending_percentiles[0.6]:
                    return 'High Value'
                elif spending >= spending_percentiles[0.4]:
                    return 'Medium Value'
                elif spending >= spending_percentiles[0.2]:
                    return 'Low Value'
                else:
                    return 'Minimal'
            
            customer_spending['segment'] = customer_spending['total_spent'].apply(classify_customer)
            
            # Calculate segment statistics
            segment_stats = customer_spending.groupby('segment').agg({
                'total_spent': ['count', 'sum', 'mean'],
                'total_orders': 'mean',
                'avg_order_value': 'mean'
            }).reset_index()
            
            # Flatten column names
            segment_stats.columns = [
                'segment', 'customer_count', 'total_revenue', 'avg_revenue_per_customer',
                'avg_orders_per_customer', 'avg_order_value'
            ]
            
            # Calculate segment shares
            total_customers = len(customer_spending)
            total_revenue = customer_spending['total_spent'].sum()
            
            segment_stats['customer_share_pct'] = (
                segment_stats['customer_count'] / total_customers * 100
            ) if total_customers > 0 else 0
            
            segment_stats['revenue_share_pct'] = (
                segment_stats['total_revenue'] / total_revenue * 100
            ) if total_revenue > 0 else 0
            
            # Convert to list
            segments_list = []
            for _, row in segment_stats.iterrows():
                segments_list.append({
                    'segment': row['segment'],
                    'customer_count': int(row['customer_count']),
                    'total_revenue': float(row['total_revenue']),
                    'avg_revenue_per_customer': float(row['avg_revenue_per_customer']),
                    'avg_orders_per_customer': float(row['avg_orders_per_customer']),
                    'avg_order_value': float(row['avg_order_value']),
                    'customer_share_pct': float(row['customer_share_pct']),
                    'revenue_share_pct': float(row['revenue_share_pct'])
                })
            
            # Regional analysis of segments
            regional_segments = customer_spending.groupby(['region', 'segment']).size().reset_index(name='count')
            
            return {
                'segments': segments_list,
                'total_segments': len(segments_list),
                'segment_thresholds': {
                    'vip_threshold': float(spending_percentiles[0.8]),
                    'high_value_threshold': float(spending_percentiles[0.6]),
                    'medium_value_threshold': float(spending_percentiles[0.4]),
                    'low_value_threshold': float(spending_percentiles[0.2])
                },
                'regional_distribution': [
                    {
                        'region': row['region'],
                        'segment': row['segment'],
                        'customer_count': int(row['count'])
                    }
                    for _, row in regional_segments.iterrows()
                ]
            }
            
        except Exception as e:
            logger.error(f"Customer segmentation failed: {str(e)}")
            return {}
    
    def _empty_result(self, top_n: int) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'top_customers': [],
            'spending_summary': {},
            'customer_segments': {},
            'time_period_info': {
                'days_analyzed': self.days,
                'cutoff_date': self.cutoff_date.isoformat() if hasattr(self, 'cutoff_date') else '',
                'analysis_date': datetime.now().isoformat(),
                'total_customers_in_period': 0,
                'total_orders_in_period': 0
            },
            'calculation_date': pd.Timestamp.now().isoformat()
        }
    
    def get_customer_growth_trajectory(self, customer_mobile: str) -> Dict[str, Any]:
        """
        Analyze spending trajectory for a specific customer.
        
        Args:
            customer_mobile: Customer's mobile number
            
        Returns:
            Dictionary with customer's spending trajectory
        """
        try:
            customer_orders = self.orders_df[
                self.orders_df['mobile_number'] == customer_mobile
            ].copy()
            
            if customer_orders.empty:
                return {'error': 'Customer not found or no orders'}
            
            # Sort by date
            customer_orders = customer_orders.sort_values('order_date_time')
            
            # Calculate cumulative spending
            customer_orders['cumulative_spending'] = customer_orders['total_amount'].cumsum()
            
            # Calculate order frequency
            customer_orders['days_since_first_order'] = (
                customer_orders['order_date_time'] - customer_orders['order_date_time'].iloc[0]
            ).dt.days
            
            # Get customer info
            customer_info = self.customers_df[
                self.customers_df['mobile_number'] == customer_mobile
            ].iloc[0] if not self.customers_df[
                self.customers_df['mobile_number'] == customer_mobile
            ].empty else {}
            
            trajectory_data = []
            for _, order in customer_orders.iterrows():
                trajectory_data.append({
                    'order_date': order['order_date_time'].isoformat(),
                    'order_amount': float(order['total_amount']),
                    'cumulative_spending': float(order['cumulative_spending']),
                    'days_since_first_order': int(order['days_since_first_order']),
                    'order_id': order['order_id']
                })
            
            return {
                'customer_info': {
                    'mobile_number': customer_mobile,
                    'customer_name': customer_info.get('customer_name', 'Unknown'),
                    'region': customer_info.get('region', 'Unknown')
                },
                'trajectory': trajectory_data,
                'summary': {
                    'total_orders': len(customer_orders),
                    'total_spent': float(customer_orders['total_amount'].sum()),
                    'avg_order_value': float(customer_orders['total_amount'].mean()),
                    'customer_lifetime_days': int(customer_orders['days_since_first_order'].max()),
                    'spending_trend': 'increasing' if len(customer_orders) > 1 and 
                        customer_orders['total_amount'].iloc[-1] > customer_orders['total_amount'].iloc[0]
                        else 'stable'
                }
            }
            
        except Exception as e:
            logger.error(f"Customer growth trajectory analysis failed: {str(e)}")
            return {'error': f'Analysis failed: {str(e)}'}
    
    def calculate_churn_risk(self) -> Dict[str, Any]:
        """Calculate churn risk for customers based on recent activity."""
        try:
            # Define thresholds for churn risk
            high_risk_days = 45  # No orders in 45+ days
            medium_risk_days = 30  # No orders in 30-45 days
            
            current_date = datetime.now()
            
            # Get last order date for each customer
            last_orders = self.orders_df.groupby('mobile_number')['order_date_time'].max().reset_index()
            last_orders['days_since_last_order'] = (
                current_date - last_orders['order_date_time']
            ).dt.days
            
            # Merge with customer info
            churn_analysis = pd.merge(
                last_orders,
                self.customers_df,
                on='mobile_number',
                how='left'
            )
            
            # Classify churn risk
            def classify_churn_risk(days):
                if days >= high_risk_days:
                    return 'High Risk'
                elif days >= medium_risk_days:
                    return 'Medium Risk'
                else:
                    return 'Low Risk'
            
            churn_analysis['churn_risk'] = churn_analysis['days_since_last_order'].apply(classify_churn_risk)
            
            # Summarize by risk level
            risk_summary = churn_analysis.groupby('churn_risk').agg({
                'mobile_number': 'count',
                'days_since_last_order': 'mean'
            }).reset_index()
            
            risk_summary.columns = ['risk_level', 'customer_count', 'avg_days_inactive']
            
            return {
                'churn_risk_summary': [
                    {
                        'risk_level': row['risk_level'],
                        'customer_count': int(row['customer_count']),
                        'avg_days_inactive': float(row['avg_days_inactive'])
                    }
                    for _, row in risk_summary.iterrows()
                ],
                'high_risk_customers': [
                    {
                        'customer_name': row['customer_name'],
                        'mobile_number': row['mobile_number'],
                        'days_since_last_order': int(row['days_since_last_order']),
                        'region': row['region']
                    }
                    for _, row in churn_analysis[churn_analysis['churn_risk'] == 'High Risk'].iterrows()
                ]
            }
            
        except Exception as e:
            logger.error(f"Churn risk calculation failed: {str(e)}")
            return {'error': f'Churn analysis failed: {str(e)}'}
