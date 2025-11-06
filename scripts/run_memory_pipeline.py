"""
Script to run the in-memory data pipeline.
Processes data using pandas and calculates KPIs without database operations.
"""

import sys
from pathlib import Path

# Add paths for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / 'src'))
sys.path.append(str(project_root))

from src.pipeline.memory_pipeline import InMemoryPipeline
from src.common.logger import setup_logger

logger = setup_logger(__name__)


def main():
    """Main function to run the in-memory pipeline."""
    logger.info("=== Akasa Data Engineering Pipeline - In-Memory Approach ===")
    
    try:
        # Initialize pipeline
        pipeline = InMemoryPipeline()
        
        # Data file paths
        customers_file = "data/raw/generated_customers.csv"
        orders_file = "data/raw/generated_orders.xml"
        
        logger.info("Starting in-memory data pipeline...")
        
        # Load and process data
        logger.info("Step 1: Loading and processing data...")
        if not pipeline.load_data(customers_file, orders_file):
            logger.error("Data loading failed")
            return False
        
        # Calculate all KPIs
        logger.info("Step 2: Calculating KPIs...")
        kpi_results = pipeline.calculate_all_kpis(
            top_customers_count=10,
            top_spenders_days=30
        )
        
        if not kpi_results:
            logger.error("KPI calculation failed")
            return False
        
        # Export results
        logger.info("Step 3: Exporting results...")
        exported_files = pipeline.export_results("data/processed")
        
        if exported_files:
            logger.info("Results exported successfully:")
            for result_type, file_path in exported_files.items():
                logger.info(f"  {result_type}: {file_path}")
        
        # Display summary
        logger.info("Step 4: Pipeline Summary...")
        data_summary = pipeline.get_data_summary()
        
        logger.info("=== PIPELINE EXECUTION SUMMARY ===")
        logger.info(f"Pipeline Type: In-Memory (Pandas)")
        logger.info(f"Customers Processed: {data_summary.get('customers', {}).get('total_count', 0)}")
        logger.info(f"Orders Processed: {data_summary.get('orders', {}).get('total_count', 0)}")
        logger.info(f"Total Revenue: ₹{data_summary.get('orders', {}).get('total_revenue', 0):,.2f}")
        
        # KPI Summary
        logger.info("=== KPI RESULTS SUMMARY ===")
        
        # Repeat Customers
        repeat_data = kpi_results.get('repeat_customers', {})
        repeat_count = len(repeat_data.get('repeat_customers_list', []))
        single_count = repeat_data.get('single_order_customers', 0)
        retention_rate = repeat_data.get('retention_metrics', {}).get('retention_rate_pct', 0)
        logger.info(f"Repeat Customers: {repeat_count} ({retention_rate:.1f}% retention rate)")
        logger.info(f"Single Order Customers: {single_count}")
        
        # Monthly Trends
        trends_data = kpi_results.get('monthly_trends', {})
        months_count = trends_data.get('total_months', 0)
        growth_direction = trends_data.get('growth_metrics', {}).get('revenue_trend_direction', 'unknown')
        logger.info(f"Monthly Trends: {months_count} months analyzed, trend: {growth_direction}")
        
        # Regional Revenue
        regional_data = kpi_results.get('regional_revenue', {})
        regions_count = regional_data.get('total_regions', 0)
        top_region_data = regional_data.get('top_regions', {}).get('by_revenue', [{}])
        top_region = top_region_data[0].get('region', 'N/A') if top_region_data else 'N/A'
        logger.info(f"Regional Revenue: {regions_count} regions analyzed, top region: {top_region}")
        
        # Top Customers
        top_data = kpi_results.get('top_customers', {})
        top_count = len(top_data.get('top_customers', []))
        period_info = top_data.get('time_period_info', {})
        days = period_info.get('days_analyzed', 0)
        total_customers_in_period = period_info.get('total_customers_in_period', 0)
        logger.info(f"Top Customers: {top_count} customers analyzed (last {days} days)")
        logger.info(f"Active Customers in Period: {total_customers_in_period}")
        
        # Top customer details
        if top_data.get('top_customers'):
            top_customer = top_data['top_customers'][0]
            logger.info(f"Top Customer: {top_customer.get('customer_name')} - ₹{top_customer.get('total_spent', 0):,.2f}")
        
        logger.info("=== In-Memory Pipeline Completed Successfully! ===")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
