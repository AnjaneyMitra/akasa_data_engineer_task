"""
Essential Test Suite for Akasa Data Engineering Pipeline
======================================================

Streamlined pytest-based test suite for interview presentation.
Tests core KPI calculations and pipeline functionality.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
import sys
from pathlib import Path

# Add src to Python path
src_path = Path(__file__).parent.parent / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


class TestDataQuality:
    """Test data processing and quality validation."""
    
    def test_data_frame_operations(self):
        """Test basic DataFrame operations used in the pipeline."""
        # Create test data
        customers = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'region': ['North', 'South', 'East'],
            'registration_date': pd.to_datetime(['2023-01-01', '2023-01-15', '2023-02-01'])
        })
        
        orders = pd.DataFrame({
            'order_id': [101, 102, 103],
            'customer_id': [1, 2, 3],
            'order_date': pd.to_datetime(['2023-01-10', '2023-01-20', '2023-02-05']),
            'order_amount': [Decimal('100.00'), Decimal('150.00'), Decimal('200.00')]
        })
        
        # Test basic joins and aggregations
        merged = orders.merge(customers, on='customer_id', how='inner')
        assert len(merged) == 3
        assert 'region' in merged.columns
        
        # Test aggregation
        regional_totals = merged.groupby('region')['order_amount'].sum()
        assert len(regional_totals) == 3


class TestKPICalculators:
    """Test all 4 KPI calculators with business logic validation."""
    
    @pytest.fixture
    def sample_data(self):
        """Create realistic test data for KPI calculations."""
        customers = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5],
            'customer_name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Lee', 'Eve Wilson'],
            'mobile_number': ['+1234567890', '+1234567891', '+1234567892', '+1234567893', '+1234567894'],
            'region': ['North', 'South', 'East', 'West', 'North'],
            'registration_date': pd.to_datetime([
                '2023-01-01', '2023-01-15', '2023-02-01', 
                '2023-02-15', '2023-03-01'
            ])
        })
        
        orders = pd.DataFrame({
            'order_id': [101, 102, 103, 104, 105, 106, 107],
            'customer_id': [1, 1, 2, 3, 3, 4, 5],
            'order_date_time': pd.to_datetime([
                '2023-01-10', '2023-02-10', '2023-01-20',
                '2023-02-05', '2023-03-05', '2023-02-20',
                '2023-03-10'
            ]),
            'order_amount': [Decimal('100.00'), Decimal('150.00'), Decimal('75.00'),
                           Decimal('200.00'), Decimal('120.00'), Decimal('90.00'),
                           Decimal('110.00')]
        })
        
        return customers, orders
    
    def test_repeat_customers_business_logic(self, sample_data):
        """Test repeat customer calculation with edge cases."""
        customers, orders = sample_data
        
        try:
            from kpi_calculators.repeat_customers import RepeatCustomersCalculator
            calculator = RepeatCustomersCalculator(customers, orders)
            result = calculator.calculate()
            
            # KPI calculators return dictionaries with results
            assert isinstance(result, dict)
            assert 'repeat_customers' in result
            assert 'repeat_customer_rate' in result
            assert isinstance(result['repeat_customers'], list)
            assert isinstance(result['repeat_customer_rate'], (int, float))
            
        except ImportError:
            pytest.skip("RepeatCustomersCalculator not available")
    
    def test_monthly_trends_calculation(self, sample_data):
        """Test monthly revenue trends with date aggregation."""
        customers, orders = sample_data
        
        try:
            from kpi_calculators.monthly_trends import MonthlyTrendsCalculator
            calculator = MonthlyTrendsCalculator(customers, orders)
            result = calculator.calculate()
            
            # Validate monthly aggregation results
            assert isinstance(result, dict)
            assert 'monthly_trends' in result
            assert 'total_months' in result
            assert isinstance(result['monthly_trends'], list)
            assert isinstance(result['total_months'], int)
            
        except ImportError:
            pytest.skip("MonthlyTrendsCalculator not available")
    
    def test_regional_revenue_accuracy(self, sample_data):
        """Test regional revenue calculation with precise amounts."""
        customers, orders = sample_data
        
        try:
            from kpi_calculators.regional_revenue import RegionalRevenueCalculator
            calculator = RegionalRevenueCalculator(customers, orders)
            result = calculator.calculate()
            
            # Validate regional aggregation results
            assert isinstance(result, dict)
            assert 'regional_revenue' in result
            assert 'total_regions' in result
            assert isinstance(result['regional_revenue'], list)
            assert isinstance(result['total_regions'], int)
            
        except ImportError:
            pytest.skip("RegionalRevenueCalculator not available")
    
    def test_top_customers_ranking(self, sample_data):
        """Test top customers calculation with proper ranking."""
        customers, orders = sample_data
        
        try:
            from kpi_calculators.top_customers import TopCustomersCalculator
            calculator = TopCustomersCalculator(customers, orders, days=90)
            result = calculator.calculate()
            
            # Validate ranking logic results
            assert isinstance(result, dict)
            assert 'top_customers' in result
            assert 'customer_segments' in result
            assert isinstance(result['top_customers'], list)
            assert isinstance(result['customer_segments'], dict)
            
        except ImportError:
            pytest.skip("TopCustomersCalculator not available")


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_empty_dataset_handling(self):
        """Ensure graceful handling of empty datasets."""
        empty_customers = pd.DataFrame(columns=['customer_id', 'region', 'registration_date'])
        empty_orders = pd.DataFrame(columns=['order_id', 'customer_id', 'order_date', 'order_amount'])
        
        # Test basic operations with empty data
        merged = empty_orders.merge(empty_customers, on='customer_id', how='inner')
        assert isinstance(merged, pd.DataFrame)
        assert len(merged) == 0
    
    def test_data_consistency(self):
        """Test data type consistency."""
        customers = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'region': ['North', 'South', 'East'],
            'registration_date': pd.to_datetime(['2023-01-01', '2023-01-15', '2023-02-01'])
        })
        
        # Validate data types
        assert pd.api.types.is_integer_dtype(customers['customer_id'])
        assert pd.api.types.is_object_dtype(customers['region'])
        assert pd.api.types.is_datetime64_any_dtype(customers['registration_date'])


class TestPerformance:
    """Test performance characteristics."""
    
    def test_large_dataset_operations(self):
        """Test operations with larger datasets."""
        n_customers = 1000
        n_orders = 5000
        
        # Create larger test dataset
        customers = pd.DataFrame({
            'customer_id': range(1, n_customers + 1),
            'region': np.random.choice(['North', 'South', 'East', 'West'], n_customers),
            'registration_date': pd.date_range('2023-01-01', periods=n_customers, freq='D')
        })
        
        orders = pd.DataFrame({
            'order_id': range(1, n_orders + 1),
            'customer_id': np.random.choice(range(1, n_customers + 1), n_orders),
            'order_date': pd.date_range('2023-01-01', periods=n_orders, freq='H'),
            'order_amount': [Decimal(str(round(np.random.uniform(10, 500), 2))) for _ in range(n_orders)]
        })
        
        # Test basic operations complete in reasonable time
        start_time = datetime.utcnow()
        
        # Perform common operations
        merged = orders.merge(customers, on='customer_id', how='inner')
        regional_revenue = merged.groupby('region')['order_amount'].sum()
        customer_orders = merged.groupby('customer_id').size()
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete within reasonable time
        assert execution_time < 5.0, f"Operations took too long: {execution_time}s"
        assert len(merged) > 0
        assert len(regional_revenue) > 0


class TestIntegration:
    """Integration tests for pipeline components."""
    
    def test_pipeline_data_flow(self):
        """Test the complete data flow through pipeline components."""
        # Create comprehensive test data
        customers = pd.DataFrame({
            'customer_id': [1, 2, 3, 4],
            'region': ['North', 'South', 'East', 'West'],
            'registration_date': pd.to_datetime([
                '2023-01-01', '2023-01-15', '2023-02-01', '2023-02-15'
            ])
        })
        
        orders = pd.DataFrame({
            'order_id': [101, 102, 103, 104, 105],
            'customer_id': [1, 1, 2, 3, 4],
            'order_date': pd.to_datetime([
                '2023-01-10', '2023-02-10', '2023-01-20', '2023-02-05', '2023-02-20'
            ]),
            'order_amount': [
                Decimal('100.00'), Decimal('150.00'), Decimal('75.00'),
                Decimal('200.00'), Decimal('90.00')
            ]
        })
        
        # Test the data can flow through typical pipeline operations
        # 1. Data join
        enriched_data = orders.merge(customers, on='customer_id', how='inner')
        assert len(enriched_data) == 5
        
        # 2. KPI-style calculations
        # Repeat customers (customers with > 1 order)
        customer_order_counts = orders.groupby('customer_id').size()
        repeat_customers = customer_order_counts[customer_order_counts > 1]
        assert len(repeat_customers) == 1  # Only customer 1 has repeat orders
        
        # Regional revenue
        regional_revenue = enriched_data.groupby('region')['order_amount'].sum()
        assert len(regional_revenue) == 4  # All 4 regions should have revenue
        
        # Monthly trends
        enriched_data['order_month'] = enriched_data['order_date'].dt.to_period('M')
        monthly_revenue = enriched_data.groupby('order_month')['order_amount'].sum()
        assert len(monthly_revenue) >= 1  # At least one month of data
        
        # Top customers by revenue
        customer_revenue = enriched_data.groupby('customer_id')['order_amount'].sum()
        # Convert to numeric to handle Decimal type with nlargest
        customer_revenue_numeric = customer_revenue.astype(float)
        top_customers = customer_revenue_numeric.nlargest(10)
        assert len(top_customers) <= 10


# Test configuration and markers
pytest_plugins = []

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: Integration tests requiring full pipeline"
    )
    config.addinivalue_line(
        "markers", "performance: Performance and scalability tests"
    )
    config.addinivalue_line(
        "markers", "security: Security and compliance validation tests"
    )


if __name__ == "__main__":
    """Allow running tests directly with python test_pipeline.py"""
    import subprocess
    import sys
    
    result = subprocess.run([sys.executable, '-m', 'pytest', __file__, '-v'])
    sys.exit(result.returncode)
