"""
Repeat Customers KPI Calculator.
Identifies customers with more than one order.
"""

import pandas as pd
from typing import Dict, Any, List
from .base_calculator import BaseKPICalculator

logger = BaseKPICalculator.__dict__.get('logger')


class RepeatCustomersCalculator(BaseKPICalculator):
    """Calculate repeat customers KPI using pandas operations."""
    
    def calculate(self) -> Dict[str, Any]:
        """
        Calculate repeat customers - customers with more than one order.
        
        Returns:
            Dictionary containing:
            - repeat_customers: List of customer details who made multiple orders
            - total_repeat_customers: Count of repeat customers
            - repeat_customer_rate: Percentage of customers who are repeat customers
            - total_orders_by_repeat_customers: Total orders placed by repeat customers
            - revenue_by_repeat_customers: Total revenue from repeat customers
        """
        try:
            if self.orders_df.empty or self.customers_df.empty:
                logger.warning("Empty data provided for repeat customers calculation")
                return self._empty_result()
            
            # Count orders per customer (mobile_number)
            order_counts = self.orders_df.groupby('mobile_number').agg({
                'order_id': 'count',
                'total_amount': ['sum', 'mean']
            }).reset_index()
            
            # Flatten column names
            order_counts.columns = [
                'mobile_number', 
                'order_count', 
                'total_spent', 
                'avg_order_value'
            ]
            
            # Identify repeat customers (more than 1 order)
            repeat_customers_data = order_counts[order_counts['order_count'] > 1].copy()
            
            if repeat_customers_data.empty:
                logger.info("No repeat customers found")
                return self._empty_result()
            
            # Enrich with customer information
            repeat_customers_enriched = pd.merge(
                repeat_customers_data,
                self.customers_df,
                on='mobile_number',
                how='left'
            )
            
            # Sort by total spent (descending) and then by order count
            repeat_customers_enriched = repeat_customers_enriched.sort_values(
                ['total_spent', 'order_count'], 
                ascending=[False, False]
            )
            
            # Calculate metrics
            total_customers = len(self.customers_df)
            total_repeat_customers = len(repeat_customers_enriched)
            repeat_rate = (total_repeat_customers / total_customers * 100) if total_customers > 0 else 0
            
            # Total metrics for repeat customers
            total_orders_by_repeat = repeat_customers_enriched['order_count'].sum()
            total_revenue_by_repeat = repeat_customers_enriched['total_spent'].sum()
            
            # Convert to list of dictionaries for easier consumption
            repeat_customers_list = []
            for _, row in repeat_customers_enriched.iterrows():
                repeat_customers_list.append({
                    'customer_id': row.get('customer_id', ''),
                    'customer_name': row.get('customer_name', ''),
                    'mobile_number': row['mobile_number'],
                    'region': row.get('region', ''),
                    'order_count': int(row['order_count']),
                    'total_spent': float(row['total_spent']),
                    'avg_order_value': float(row['avg_order_value'])
                })
            
            results = {
                'repeat_customers': repeat_customers_list,
                'total_repeat_customers': total_repeat_customers,
                'total_customers': total_customers,
                'repeat_customer_rate': round(repeat_rate, 2),
                'total_orders_by_repeat_customers': int(total_orders_by_repeat),
                'revenue_by_repeat_customers': float(total_revenue_by_repeat),
                'calculation_date': pd.Timestamp.now().isoformat()
            }
            
            # Validate results
            if self.validate_results(results):
                logger.info(f"Repeat customers calculation completed: {total_repeat_customers} out of {total_customers} customers ({repeat_rate:.1f}%)")
                return results
            else:
                logger.error("Repeat customers calculation validation failed")
                return self._empty_result()
                
        except Exception as e:
            logger.error(f"Repeat customers calculation failed: {str(e)}")
            return self._empty_result()
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'repeat_customers': [],
            'total_repeat_customers': 0,
            'total_customers': len(self.customers_df) if not self.customers_df.empty else 0,
            'repeat_customer_rate': 0.0,
            'total_orders_by_repeat_customers': 0,
            'revenue_by_repeat_customers': 0.0,
            'calculation_date': pd.Timestamp.now().isoformat()
        }
    
    def get_repeat_customers_by_region(self) -> Dict[str, Any]:
        """
        Get repeat customers breakdown by region.
        
        Returns:
            Dictionary with regional breakdown of repeat customers
        """
        try:
            main_results = self.calculate()
            
            if not main_results['repeat_customers']:
                return {'regions': {}, 'total_regions': 0}
            
            # Convert repeat customers to DataFrame for analysis
            repeat_df = pd.DataFrame(main_results['repeat_customers'])
            
            # Group by region
            regional_analysis = repeat_df.groupby('region').agg({
                'customer_id': 'count',
                'order_count': 'sum',
                'total_spent': 'sum',
                'avg_order_value': 'mean'
            }).reset_index()
            
            regional_analysis.columns = [
                'region', 'repeat_customers_count', 'total_orders', 
                'total_revenue', 'avg_order_value'
            ]
            
            # Convert to dictionary
            regions_dict = {}
            for _, row in regional_analysis.iterrows():
                regions_dict[row['region']] = {
                    'repeat_customers_count': int(row['repeat_customers_count']),
                    'total_orders': int(row['total_orders']),
                    'total_revenue': float(row['total_revenue']),
                    'avg_order_value': float(row['avg_order_value'])
                }
            
            return {
                'regions': regions_dict,
                'total_regions': len(regions_dict)
            }
            
        except Exception as e:
            logger.error(f"Regional repeat customers analysis failed: {str(e)}")
            return {'regions': {}, 'total_regions': 0}
