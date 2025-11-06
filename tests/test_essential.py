"""
Streamlined pytest validation for Akasa Data Engineering Pipeline.
Essential tests demonstrating production-ready data pipeline capabilities.
"""

import pytest
import sys
import os
from pathlib import Path
import json
import tempfile

# Setup path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))


def test_pipeline_core_functionality():
    """Test core pipeline functionality - essential for production validation."""
    from pipeline.memory_pipeline import InMemoryPipeline
    
    # Initialize and test data loading
    pipeline = InMemoryPipeline()
    success = pipeline.load_data(
        'data/raw/generated_customers.csv', 
        'data/raw/generated_orders.xml'
    )
    assert success, "Pipeline failed to load test data"
    
    # Test KPI calculations
    results = pipeline.calculate_all_kpis()
    assert results is not None, "KPI calculation failed"
    
    # Validate all 4 required KPIs
    required_kpis = ['repeat_customers', 'monthly_trends', 'regional_revenue', 'top_customers']
    for kpi in required_kpis:
        assert kpi in results, f"Missing KPI: {kpi}"
    
    # Validate business metrics
    assert results['repeat_customers']['total_repeat_customers'] == 16
    assert results['monthly_trends']['total_months'] == 5
    assert results['regional_revenue']['total_regions'] == 6
    assert len(results['top_customers']['top_customers']) == 10
    
    print("✅ All 4 KPIs successfully calculated and validated")


@pytest.mark.database
def test_dual_pipeline_architecture():
    """Test dual pipeline approach - showcases architecture understanding."""
    from pipeline.memory_pipeline import InMemoryPipeline
    from pipeline.table_pipeline import TableBasedPipeline
    from config.database import db_config
    
    if not db_config.test_connection():
        pytest.skip("Database not available")
    
    # Test memory pipeline
    memory_pipeline = InMemoryPipeline()
    memory_pipeline.load_data('data/raw/generated_customers.csv', 'data/raw/generated_orders.xml')
    memory_results = memory_pipeline.calculate_all_kpis()
    
    # Test table pipeline
    table_pipeline = TableBasedPipeline()
    if table_pipeline.initialize():
        table_results = table_pipeline.run_all_kpis()
    else:
        pytest.skip("Database initialization failed")
    
    # Verify consistency between approaches
    memory_summary = memory_results['pipeline_info']['data_summary']
    table_summary = table_results['pipeline_summary']['database_summary']
    
    assert memory_summary['total_customers'] == table_summary['customers']['total_count']
    assert memory_summary['total_orders'] == table_summary['orders']['total_count']
    assert table_results['pipeline_summary']['kpis_successful'] == 4
    
    print("✅ Dual pipeline architecture validated - consistent results")


def test_data_quality_and_business_rules():
    """Test data quality and business logic validation."""
    from pipeline.memory_pipeline import InMemoryPipeline
    
    pipeline = InMemoryPipeline()
    pipeline.load_data('data/raw/generated_customers.csv', 'data/raw/generated_orders.xml')
    results = pipeline.calculate_all_kpis()
    
    # Business Rule Validation
    repeat_data = results['repeat_customers']
    
    # All repeat customers must have > 1 order
    for customer in repeat_data['repeat_customers']:
        assert customer['order_count'] > 1, f"Invalid repeat customer: {customer['customer_name']}"
    
    # Retention rate must be valid percentage
    retention_rate = repeat_data['repeat_customer_rate']
    assert 0.0 <= retention_rate <= 100.0
    
    # Regional revenue percentages should sum to ~100%
    regional_data = results['regional_revenue']
    total_percentage = sum(r['revenue_share_pct'] for r in regional_data['regional_revenue'])
    assert abs(total_percentage - 100.0) < 5.0, "Regional percentages don't sum to 100%"
    
    # Test data quality report
    with tempfile.TemporaryDirectory() as temp_dir:
        exported_files = pipeline.export_results(temp_dir)
        
        with open(exported_files['data_quality'], 'r') as f:
            quality_data = json.load(f)
        
        # Quality score must be high
        overall_score = quality_data['overall_score']['score']
        assert overall_score >= 80, f"Data quality score too low: {overall_score}"
        
        # No critical data issues
        order_quality = quality_data['order_data_quality']
        assert order_quality['negative_amounts'] == 0
        assert order_quality['duplicate_order_ids'] == 0
    
    print("✅ Business rules and data quality validated")


def test_error_handling_and_performance():
    """Test error handling and performance characteristics."""
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
    from pipeline.memory_pipeline import InMemoryPipeline
    import time

    # Test error handling
    pipeline = InMemoryPipeline()
    success = pipeline.load_data('nonexistent.csv', 'nonexistent.xml')
    assert not success, "Should fail with invalid files"

    results = pipeline.calculate_all_kpis()
    assert results == {}, "Should return empty dict with no data"
    
    # Test performance
    pipeline = InMemoryPipeline()
    
    start_time = time.time()
    success = pipeline.load_data('data/raw/generated_customers.csv', 'data/raw/generated_orders.xml')
    load_time = time.time() - start_time
    
    assert success
    assert load_time < 1.0, f"Data loading too slow: {load_time:.3f}s"
    
    start_time = time.time()
    results = pipeline.calculate_all_kpis()
    calc_time = time.time() - start_time
    
    assert results is not None
    assert calc_time < 2.0, f"KPI calculations too slow: {calc_time:.3f}s"
    
    print(f"✅ Error handling and performance validated (Load: {load_time:.3f}s, Calc: {calc_time:.3f}s)")


def test_security_and_architecture_compliance():
    """Test security best practices and architecture patterns."""
    # Test configuration externalization
    env_file = Path('.env')
    assert env_file.exists(), "Missing .env configuration file"
    
    # Test database config security
    config_file = Path('config/database.py')
    with open(config_file, 'r') as f:
        config_content = f.read()
    
    assert 'os.getenv(' in config_content, "Not using environment variables"
    
    # Test modular architecture
    src_path = Path('src')
    required_modules = ['pipeline', 'kpi_calculators', 'data_processing', 'database', 'common']
    
    for module in required_modules:
        module_path = src_path / module
        assert module_path.exists(), f"Missing module: {module}"
        assert (module_path / '__init__.py').exists(), f"Module not packaged: {module}"
    
    # Test KPI modularity
    kpi_path = src_path / 'kpi_calculators'
    kpi_files = ['repeat_customers.py', 'monthly_trends.py', 'regional_revenue.py', 'top_customers.py']
    
    for kpi_file in kpi_files:
        assert (kpi_path / kpi_file).exists(), f"Missing KPI: {kpi_file}"
    
    print("✅ Security and architecture compliance validated")


if __name__ == '__main__':
    # Run tests with pytest
    pytest.main([
        __file__, 
        '-v', 
        '--tb=short',
        '--disable-warnings'
    ])
