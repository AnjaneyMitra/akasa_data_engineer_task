"""
Monthly Order Trends KPI Calculator.
Analyzes order patterns by month to observe business trends.
"""

import pandas as pd
from typing import Dict, Any, List
from .base_calculator import BaseKPICalculator

logger = BaseKPICalculator.__dict__.get('logger')


class MonthlyTrendsCalculator(BaseKPICalculator):
    """Calculate monthly order trends KPI using pandas operations."""
    
    def calculate(self) -> Dict[str, Any]:
        """
        Calculate monthly order trends - aggregate orders by month.
        
        Returns:
            Dictionary containing:
            - monthly_trends: List of monthly statistics
            - trend_summary: Overall trend analysis
            - total_months: Number of months with data
            - growth_metrics: Month-over-month growth analysis
        """
        try:
            if self.orders_df.empty:
                logger.warning("Empty orders data provided for monthly trends calculation")
                return self._empty_result()
            
            # Ensure order_date_time is datetime
            orders_df = self.orders_df.copy()
            
            # Extract month-year for grouping
            orders_df['month_year'] = orders_df['order_date_time'].dt.to_period('M')
            orders_df['year'] = orders_df['order_date_time'].dt.year
            orders_df['month'] = orders_df['order_date_time'].dt.month
            orders_df['month_name'] = orders_df['order_date_time'].dt.strftime('%B')
            
            # Group by month-year and calculate metrics
            monthly_stats = orders_df.groupby('month_year').agg({
                'order_id': 'count',
                'total_amount': ['sum', 'mean', 'std'],
                'mobile_number': 'nunique'
            }).reset_index()
            
            # Flatten column names
            monthly_stats.columns = [
                'month_year', 'total_orders', 'total_revenue', 
                'avg_order_value', 'revenue_std', 'unique_customers'
            ]
            
            # Convert month_year back to string for JSON serialization
            monthly_stats['month_year_str'] = monthly_stats['month_year'].astype(str)
            monthly_stats['year'] = monthly_stats['month_year'].dt.year
            monthly_stats['month'] = monthly_stats['month_year'].dt.month
            
            # Sort by month_year
            monthly_stats = monthly_stats.sort_values('month_year')
            
            # Calculate month-over-month growth
            monthly_stats['revenue_growth_pct'] = monthly_stats['total_revenue'].pct_change() * 100
            monthly_stats['order_growth_pct'] = monthly_stats['total_orders'].pct_change() * 100
            
            # Fill NaN values for first month
            monthly_stats = monthly_stats.fillna({
                'revenue_growth_pct': 0,
                'order_growth_pct': 0,
                'revenue_std': 0
            })
            
            # Convert to list of dictionaries
            monthly_trends_list = []
            for _, row in monthly_stats.iterrows():
                monthly_trends_list.append({
                    'period': row['month_year_str'],
                    'year': int(row['year']),
                    'month': int(row['month']),
                    'total_orders': int(row['total_orders']),
                    'total_revenue': float(row['total_revenue']),
                    'avg_order_value': float(row['avg_order_value']),
                    'revenue_std': float(row['revenue_std']),
                    'unique_customers': int(row['unique_customers']),
                    'revenue_growth_pct': float(row['revenue_growth_pct']),
                    'order_growth_pct': float(row['order_growth_pct'])
                })
            
            # Calculate trend summary
            trend_summary = self._calculate_trend_summary(monthly_stats)
            
            # Calculate growth metrics
            growth_metrics = self._calculate_growth_metrics(monthly_stats)
            
            results = {
                'monthly_trends': monthly_trends_list,
                'trend_summary': trend_summary,
                'growth_metrics': growth_metrics,
                'total_months': len(monthly_trends_list),
                'calculation_date': pd.Timestamp.now().isoformat()
            }
            
            # Validate results
            if self.validate_results(results):
                logger.info(f"Monthly trends calculation completed: {len(monthly_trends_list)} months analyzed")
                return results
            else:
                logger.error("Monthly trends calculation validation failed")
                return self._empty_result()
                
        except Exception as e:
            logger.error(f"Monthly trends calculation failed: {str(e)}")
            return self._empty_result()
    
    def _calculate_trend_summary(self, monthly_stats: pd.DataFrame) -> Dict[str, Any]:
        """Calculate overall trend summary statistics."""
        try:
            if monthly_stats.empty:
                return {}
            
            # Overall statistics
            total_revenue = monthly_stats['total_revenue'].sum()
            total_orders = monthly_stats['total_orders'].sum()
            avg_monthly_revenue = monthly_stats['total_revenue'].mean()
            avg_monthly_orders = monthly_stats['total_orders'].mean()
            
            # Peak and low months
            peak_revenue_month = monthly_stats.loc[monthly_stats['total_revenue'].idxmax()]
            low_revenue_month = monthly_stats.loc[monthly_stats['total_revenue'].idxmin()]
            
            peak_orders_month = monthly_stats.loc[monthly_stats['total_orders'].idxmax()]
            low_orders_month = monthly_stats.loc[monthly_stats['total_orders'].idxmin()]
            
            return {
                'total_revenue': float(total_revenue),
                'total_orders': int(total_orders),
                'avg_monthly_revenue': float(avg_monthly_revenue),
                'avg_monthly_orders': float(avg_monthly_orders),
                'peak_revenue_month': {
                    'period': str(peak_revenue_month['month_year']),
                    'revenue': float(peak_revenue_month['total_revenue']),
                    'orders': int(peak_revenue_month['total_orders'])
                },
                'low_revenue_month': {
                    'period': str(low_revenue_month['month_year']),
                    'revenue': float(low_revenue_month['total_revenue']),
                    'orders': int(low_revenue_month['total_orders'])
                },
                'peak_orders_month': {
                    'period': str(peak_orders_month['month_year']),
                    'revenue': float(peak_orders_month['total_revenue']),
                    'orders': int(peak_orders_month['total_orders'])
                },
                'low_orders_month': {
                    'period': str(low_orders_month['month_year']),
                    'revenue': float(low_orders_month['total_revenue']),
                    'orders': int(low_orders_month['total_orders'])
                }
            }
            
        except Exception as e:
            logger.error(f"Trend summary calculation failed: {str(e)}")
            return {}
    
    def _calculate_growth_metrics(self, monthly_stats: pd.DataFrame) -> Dict[str, Any]:
        """Calculate growth metrics and trends."""
        try:
            if monthly_stats.empty or len(monthly_stats) < 2:
                return {}
            
            # Average growth rates (excluding first month which has NaN)
            avg_revenue_growth = monthly_stats['revenue_growth_pct'].iloc[1:].mean()
            avg_order_growth = monthly_stats['order_growth_pct'].iloc[1:].mean()
            
            # Growth volatility (standard deviation of growth rates)
            revenue_growth_volatility = monthly_stats['revenue_growth_pct'].iloc[1:].std()
            order_growth_volatility = monthly_stats['order_growth_pct'].iloc[1:].std()
            
            # Overall growth from first to last month
            first_month = monthly_stats.iloc[0]
            last_month = monthly_stats.iloc[-1]
            
            overall_revenue_growth = (
                (last_month['total_revenue'] - first_month['total_revenue']) / 
                first_month['total_revenue'] * 100
            ) if first_month['total_revenue'] > 0 else 0
            
            overall_order_growth = (
                (last_month['total_orders'] - first_month['total_orders']) / 
                first_month['total_orders'] * 100
            ) if first_month['total_orders'] > 0 else 0
            
            # Trend direction
            revenue_trend = "increasing" if avg_revenue_growth > 0 else "decreasing" if avg_revenue_growth < 0 else "stable"
            order_trend = "increasing" if avg_order_growth > 0 else "decreasing" if avg_order_growth < 0 else "stable"
            
            return {
                'avg_monthly_revenue_growth_pct': float(avg_revenue_growth),
                'avg_monthly_order_growth_pct': float(avg_order_growth),
                'revenue_growth_volatility': float(revenue_growth_volatility) if not pd.isna(revenue_growth_volatility) else 0.0,
                'order_growth_volatility': float(order_growth_volatility) if not pd.isna(order_growth_volatility) else 0.0,
                'overall_revenue_growth_pct': float(overall_revenue_growth),
                'overall_order_growth_pct': float(overall_order_growth),
                'revenue_trend_direction': revenue_trend,
                'order_trend_direction': order_trend,
                'analysis_period': {
                    'start': str(first_month['month_year']),
                    'end': str(last_month['month_year']),
                    'months_analyzed': len(monthly_stats)
                }
            }
            
        except Exception as e:
            logger.error(f"Growth metrics calculation failed: {str(e)}")
            return {}
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'monthly_trends': [],
            'trend_summary': {},
            'growth_metrics': {},
            'total_months': 0,
            'calculation_date': pd.Timestamp.now().isoformat()
        }
    
    def get_quarterly_trends(self) -> Dict[str, Any]:
        """
        Get quarterly aggregated trends.
        
        Returns:
            Dictionary with quarterly trend analysis
        """
        try:
            if self.orders_df.empty:
                return {'quarterly_trends': [], 'total_quarters': 0}
            
            # Group by quarter
            orders_df = self.orders_df.copy()
            orders_df['quarter'] = orders_df['order_date_time'].dt.to_period('Q')
            
            quarterly_stats = orders_df.groupby('quarter').agg({
                'order_id': 'count',
                'total_amount': ['sum', 'mean'],
                'mobile_number': 'nunique'
            }).reset_index()
            
            # Flatten column names
            quarterly_stats.columns = [
                'quarter', 'total_orders', 'total_revenue', 
                'avg_order_value', 'unique_customers'
            ]
            
            # Convert to list
            quarterly_trends = []
            for _, row in quarterly_stats.iterrows():
                quarterly_trends.append({
                    'quarter': str(row['quarter']),
                    'total_orders': int(row['total_orders']),
                    'total_revenue': float(row['total_revenue']),
                    'avg_order_value': float(row['avg_order_value']),
                    'unique_customers': int(row['unique_customers'])
                })
            
            return {
                'quarterly_trends': quarterly_trends,
                'total_quarters': len(quarterly_trends)
            }
            
        except Exception as e:
            logger.error(f"Quarterly trends calculation failed: {str(e)}")
            return {'quarterly_trends': [], 'total_quarters': 0}
