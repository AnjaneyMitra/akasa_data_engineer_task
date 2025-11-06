"""
Regional Revenue KPI Calculator.
Analyzes revenue distribution across different geographic regions.
"""

import pandas as pd
from typing import Dict, Any, List
from .base_calculator import BaseKPICalculator

from src.common.logger import setup_logger
logger = setup_logger(__name__)


class RegionalRevenueCalculator(BaseKPICalculator):
    """Calculate regional revenue KPI using pandas operations."""
    
    def calculate(self) -> Dict[str, Any]:
        """
        Calculate regional revenue analysis.
        
        Returns:
            Dictionary containing:
            - regional_revenue: List of revenue by region
            - revenue_share: Percentage share by region
            - top_regions: Top performing regions
            - regional_metrics: Additional regional analysis
        """
        try:
            if self.customers_df.empty or self.orders_df.empty:
                logger.warning("Empty data provided for regional revenue calculation")
                return self._empty_result()
            
            # Join orders with customers to get region information
            merged_df = pd.merge(
                self.orders_df,
                self.customers_df[['mobile_number', 'region']],
                on='mobile_number',
                how='left'
            )
            
            # Check for orders without region mapping
            missing_regions = merged_df['region'].isnull().sum()
            if missing_regions > 0:
                logger.warning(f"{missing_regions} orders found without region mapping")
                # Fill with 'Unknown' for consistency
                merged_df['region'] = merged_df['region'].fillna('Unknown')
            
            # Group by region and calculate metrics
            regional_stats = merged_df.groupby('region', observed=True).agg({
                'order_id': 'count',
                'total_amount': ['sum', 'mean', 'std', 'min', 'max'],
                'mobile_number': 'nunique',
                'sku_count': 'sum'
            }).reset_index()
            
            # Flatten column names
            regional_stats.columns = [
                'region', 'total_orders', 'total_revenue', 'avg_order_value', 
                'revenue_std', 'min_order_value', 'max_order_value',
                'unique_customers', 'total_items_sold'
            ]
            
            # Calculate revenue share
            total_revenue = regional_stats['total_revenue'].sum()
            regional_stats['revenue_share_pct'] = (
                regional_stats['total_revenue'] / total_revenue * 100
            ) if total_revenue > 0 else 0
            
            # Calculate orders share
            total_orders = regional_stats['total_orders'].sum()
            regional_stats['order_share_pct'] = (
                regional_stats['total_orders'] / total_orders * 100
            ) if total_orders > 0 else 0
            
            # Calculate customers share
            total_unique_customers = regional_stats['unique_customers'].sum()
            regional_stats['customer_share_pct'] = (
                regional_stats['unique_customers'] / total_unique_customers * 100
            ) if total_unique_customers > 0 else 0
            
            # Calculate revenue per customer
            regional_stats['revenue_per_customer'] = (
                regional_stats['total_revenue'] / regional_stats['unique_customers']
            )
            
            # Sort by total revenue (descending)
            regional_stats = regional_stats.sort_values('total_revenue', ascending=False)
            
            # Fill NaN values
            regional_stats = regional_stats.fillna({
                'revenue_std': 0,
                'revenue_per_customer': 0
            })
            
            # Convert to list of dictionaries
            regional_revenue_list = []
            for _, row in regional_stats.iterrows():
                regional_revenue_list.append({
                    'region': row['region'],
                    'total_orders': int(row['total_orders']),
                    'total_revenue': float(row['total_revenue']),
                    'avg_order_value': float(row['avg_order_value']),
                    'revenue_std': float(row['revenue_std']),
                    'min_order_value': float(row['min_order_value']),
                    'max_order_value': float(row['max_order_value']),
                    'unique_customers': int(row['unique_customers']),
                    'total_items_sold': int(row['total_items_sold']),
                    'revenue_share_pct': float(row['revenue_share_pct']),
                    'order_share_pct': float(row['order_share_pct']),
                    'customer_share_pct': float(row['customer_share_pct']),
                    'revenue_per_customer': float(row['revenue_per_customer'])
                })
            
            # Calculate top regions
            top_regions = self._identify_top_regions(regional_stats)
            
            # Calculate regional metrics
            regional_metrics = self._calculate_regional_metrics(regional_stats, merged_df)
            
            results = {
                'regional_revenue': regional_revenue_list,
                'top_regions': top_regions,
                'regional_metrics': regional_metrics,
                'total_regions': len(regional_revenue_list),
                'calculation_date': pd.Timestamp.now().isoformat()
            }
            
            # Validate results
            if self.validate_results(results):
                logger.info(f"Regional revenue calculation completed: {len(regional_revenue_list)} regions analyzed")
                return results
            else:
                logger.error("Regional revenue calculation validation failed")
                return self._empty_result()
                
        except Exception as e:
            logger.error(f"Regional revenue calculation failed: {str(e)}")
            return self._empty_result()
    
    def _identify_top_regions(self, regional_stats: pd.DataFrame) -> Dict[str, Any]:
        """Identify top performing regions by different metrics."""
        try:
            if regional_stats.empty:
                return {}
            
            # Top by revenue
            top_by_revenue = regional_stats.nlargest(3, 'total_revenue')
            
            # Top by orders
            top_by_orders = regional_stats.nlargest(3, 'total_orders')
            
            # Top by customers
            top_by_customers = regional_stats.nlargest(3, 'unique_customers')
            
            # Top by average order value
            top_by_avg_order = regional_stats.nlargest(3, 'avg_order_value')
            
            return {
                'by_revenue': [
                    {
                        'region': row['region'],
                        'total_revenue': float(row['total_revenue']),
                        'revenue_share_pct': float(row['revenue_share_pct'])
                    }
                    for _, row in top_by_revenue.iterrows()
                ],
                'by_orders': [
                    {
                        'region': row['region'],
                        'total_orders': int(row['total_orders']),
                        'order_share_pct': float(row['order_share_pct'])
                    }
                    for _, row in top_by_orders.iterrows()
                ],
                'by_customers': [
                    {
                        'region': row['region'],
                        'unique_customers': int(row['unique_customers']),
                        'customer_share_pct': float(row['customer_share_pct'])
                    }
                    for _, row in top_by_customers.iterrows()
                ],
                'by_avg_order_value': [
                    {
                        'region': row['region'],
                        'avg_order_value': float(row['avg_order_value']),
                        'total_orders': int(row['total_orders'])
                    }
                    for _, row in top_by_avg_order.iterrows()
                ]
            }
            
        except Exception as e:
            logger.error(f"Top regions identification failed: {str(e)}")
            return {}
    
    def _calculate_regional_metrics(self, regional_stats: pd.DataFrame, merged_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate additional regional analysis metrics."""
        try:
            if regional_stats.empty:
                return {}
            
            # Revenue concentration (Gini coefficient approximation)
            revenue_values = regional_stats['total_revenue'].sort_values()
            n = len(revenue_values)
            cumsum = revenue_values.cumsum()
            
            # Simple concentration metric
            revenue_concentration = (
                (2 * sum((i + 1) * revenue_values.iloc[i] for i in range(n))) / 
                (n * cumsum.iloc[-1]) - (n + 1) / n
            ) if n > 1 and cumsum.iloc[-1] > 0 else 0
            
            # Regional diversity metrics
            total_revenue = regional_stats['total_revenue'].sum()
            revenue_shares = regional_stats['total_revenue'] / total_revenue
            
            # Calculate entropy (diversity measure)
            import numpy as np
            entropy = -sum(share * np.log(share) for share in revenue_shares if share > 0)
            max_entropy = np.log(len(regional_stats))
            diversity_index = entropy / max_entropy if max_entropy > 0 else 0
            
            # Performance gaps
            max_revenue = regional_stats['total_revenue'].max()
            min_revenue = regional_stats['total_revenue'].min()
            revenue_gap_ratio = max_revenue / min_revenue if min_revenue > 0 else float('inf')
            
            # Average metrics across regions
            avg_revenue_per_region = regional_stats['total_revenue'].mean()
            avg_customers_per_region = regional_stats['unique_customers'].mean()
            avg_orders_per_region = regional_stats['total_orders'].mean()
            
            # Seasonal analysis by region (if applicable)
            seasonal_patterns = self._analyze_seasonal_patterns_by_region(merged_df)
            
            return {
                'revenue_concentration_index': float(revenue_concentration),
                'diversity_index': float(diversity_index),
                'revenue_gap_ratio': float(revenue_gap_ratio) if revenue_gap_ratio != float('inf') else None,
                'avg_revenue_per_region': float(avg_revenue_per_region),
                'avg_customers_per_region': float(avg_customers_per_region),
                'avg_orders_per_region': float(avg_orders_per_region),
                'seasonal_patterns': seasonal_patterns,
                'performance_spread': {
                    'max_revenue': float(max_revenue),
                    'min_revenue': float(min_revenue),
                    'revenue_std': float(regional_stats['total_revenue'].std())
                }
            }
            
        except Exception as e:
            logger.error(f"Regional metrics calculation failed: {str(e)}")
            return {}
    
    def _analyze_seasonal_patterns_by_region(self, merged_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze seasonal patterns by region."""
        try:
            if merged_df.empty:
                return {}
            
            # Add month information
            merged_df_copy = merged_df.copy()
            merged_df_copy['month'] = merged_df_copy['order_date_time'].dt.month
            merged_df_copy['month_name'] = merged_df_copy['order_date_time'].dt.strftime('%B')
            
            # Group by region and month
            seasonal_stats = merged_df_copy.groupby(['region', 'month_name'], observed=True).agg({
                'total_amount': 'sum',
                'order_id': 'count'
            }).reset_index()
            
            # Find peak month for each region
            region_peaks = {}
            for region in seasonal_stats['region'].unique():
                region_data = seasonal_stats[seasonal_stats['region'] == region]
                if not region_data.empty:
                    peak_month = region_data.loc[region_data['total_amount'].idxmax()]
                    region_peaks[region] = {
                        'peak_month': peak_month['month_name'],
                        'peak_revenue': float(peak_month['total_amount']),
                        'peak_orders': int(peak_month['order_id'])
                    }
            
            return {
                'region_peak_months': region_peaks,
                'total_regions_analyzed': len(region_peaks)
            }
            
        except Exception as e:
            logger.error(f"Seasonal patterns analysis failed: {str(e)}")
            return {}
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'regional_revenue': [],
            'top_regions': {},
            'regional_metrics': {},
            'total_regions': 0,
            'calculation_date': pd.Timestamp.now().isoformat()
        }
    
    def get_region_comparison(self, region1: str, region2: str) -> Dict[str, Any]:
        """
        Compare metrics between two regions.
        
        Args:
            region1: First region name
            region2: Second region name
            
        Returns:
            Dictionary with comparison metrics
        """
        try:
            results = self.calculate()
            regional_data = {item['region']: item for item in results['regional_revenue']}
            
            if region1 not in regional_data or region2 not in regional_data:
                return {'error': 'One or both regions not found in data'}
            
            r1_data = regional_data[region1]
            r2_data = regional_data[region2]
            
            return {
                'region_1': region1,
                'region_2': region2,
                'revenue_comparison': {
                    'region_1_revenue': r1_data['total_revenue'],
                    'region_2_revenue': r2_data['total_revenue'],
                    'difference': r1_data['total_revenue'] - r2_data['total_revenue'],
                    'percentage_difference': (
                        (r1_data['total_revenue'] - r2_data['total_revenue']) / 
                        r2_data['total_revenue'] * 100
                    ) if r2_data['total_revenue'] > 0 else 0
                },
                'order_comparison': {
                    'region_1_orders': r1_data['total_orders'],
                    'region_2_orders': r2_data['total_orders'],
                    'difference': r1_data['total_orders'] - r2_data['total_orders']
                },
                'customer_comparison': {
                    'region_1_customers': r1_data['unique_customers'],
                    'region_2_customers': r2_data['unique_customers'],
                    'difference': r1_data['unique_customers'] - r2_data['unique_customers']
                }
            }
            
        except Exception as e:
            logger.error(f"Region comparison failed: {str(e)}")
            return {'error': f'Comparison failed: {str(e)}'}
