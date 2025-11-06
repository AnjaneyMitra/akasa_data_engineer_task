"""
Script to run the complete table-based data pipeline.
Handles database setup, data ingestion, and KPI calculation.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from pipeline.table_pipeline import TableBasedPipeline
from common.logger import setup_logger
from config.database import db_config

logger = setup_logger(__name__)


def main():
    """Main execution function."""
    logger.info("=== Akasa Data Engineering - Table-Based Pipeline ===")
    
    # Check database configuration
    if not db_config.username or not db_config.password:
        logger.error("Database credentials not configured")
        logger.info("Please set DB_USERNAME and DB_PASSWORD in .env file")
        logger.info("Run 'cp .env.example .env' and update with your credentials")
        return False
    
    # Test database connection
    if not db_config.test_connection():
        logger.error("Database connection failed")
        logger.info("Please ensure MySQL is running and credentials are correct")
        logger.info("You may need to run 'python scripts/setup_database.py' first")
        return False
    
    # Run the pipeline
    try:
        with TableBasedPipeline() as pipeline:
            logger.info("Starting table-based KPI calculation pipeline...")
            
            # Run all KPI calculations
            results = pipeline.run_all_kpis()
            
            if 'error' in results:
                logger.error(f"Pipeline failed: {results['error']}")
                return False
            
            # Export results
            pipeline.export_results()
            
            # Print summary
            print_pipeline_summary(results)
            
            logger.info("Pipeline execution completed successfully!")
            return True
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        return False


def print_pipeline_summary(results: dict):
    """Print formatted pipeline summary."""
    
    print("\n" + "="*60)
    print("           TABLE-BASED PIPELINE RESULTS")
    print("="*60)
    
    # Pipeline metadata
    summary = results.get('pipeline_summary', {})
    print(f"Execution Time: {summary.get('execution_time_seconds', 0):.2f} seconds")
    print(f"KPIs Calculated: {summary.get('kpis_calculated', 0)}")
    print(f"Database: MySQL (Table-based approach)")
    
    # Database summary
    db_summary = summary.get('database_summary', {})
    customers = db_summary.get('customers', {})
    orders = db_summary.get('orders', {})
    
    print(f"\nData Summary:")
    print(f"  Total Customers: {customers.get('total_count', 0):,}")
    print(f"  Total Orders: {orders.get('total_count', 0):,}")
    print(f"  Total Revenue: ₹{orders.get('total_revenue', 0):,.2f}")
    print(f"  Average Order Value: ₹{orders.get('avg_order_value', 0):,.2f}")
    
    # KPI Results
    kpis = results.get('kpi_results', {})
    
    print(f"\n📊 KEY PERFORMANCE INDICATORS:")
    print("-"*60)
    
    # 1. Repeat Customers
    if 'repeat_customers' in kpis and 'error' not in kpis['repeat_customers']:
        rc = kpis['repeat_customers']
        print(f"\n1️⃣  REPEAT CUSTOMERS:")
        print(f"    • Total Repeat Customers: {rc['total_repeat_customers']:,}")
        print(f"    • Revenue from Repeat Customers: ₹{rc['total_revenue_from_repeat_customers']:,.2f}")
        print(f"    • Avg Orders per Repeat Customer: {rc['avg_orders_per_repeat_customer']:.1f}")
        print(f"    • Avg Revenue per Repeat Customer: ₹{rc['avg_revenue_per_repeat_customer']:,.2f}")
    
    # 2. Monthly Trends
    if 'monthly_trends' in kpis and 'error' not in kpis['monthly_trends']:
        mt = kpis['monthly_trends']
        print(f"\n2️⃣  MONTHLY ORDER TRENDS:")
        print(f"    • Months Analyzed: {mt['total_months_analyzed']}")
        print(f"    • Avg Orders per Month: {mt['avg_orders_per_month']:.0f}")
        print(f"    • Avg Revenue per Month: ₹{mt['avg_revenue_per_month']:,.2f}")
        print(f"    • Latest Month Order Growth: {mt['latest_month_order_growth_%']}%")
        print(f"    • Latest Month Revenue Growth: {mt['latest_month_revenue_growth_%']}%")
        
        # Show recent months
        monthly_data = mt.get('monthly_data', [])
        if monthly_data:
            print(f"    • Recent Months:")
            for month in monthly_data[-3:]:  # Last 3 months
                print(f"      - {month['month_name']}: {month['order_count']} orders, ₹{month['total_revenue']:,.0f}")
    
    # 3. Regional Revenue
    if 'regional_revenue' in kpis and 'error' not in kpis['regional_revenue']:
        rr = kpis['regional_revenue']
        print(f"\n3️⃣  REGIONAL REVENUE:")
        print(f"    • Regions Analyzed: {rr['total_regions']}")
        print(f"    • Top Region: {rr.get('top_region', {}).get('region', 'N/A')} - ₹{rr.get('top_region', {}).get('total_revenue', 0):,.2f}")
        print(f"    • Regional Breakdown:")
        
        for region_data in rr.get('regional_breakdown', [])[:5]:  # Top 5 regions
            percentage = region_data.get('revenue_percentage', 0)
            print(f"      - {region_data['region']}: ₹{region_data['total_revenue']:,.0f} ({percentage:.1f}%)")
    
    # 4. Top Spenders
    if 'top_spenders_30_days' in kpis and 'error' not in kpis['top_spenders_30_days']:
        ts = kpis['top_spenders_30_days']
        print(f"\n4️⃣  TOP SPENDERS (Last 30 Days):")
        print(f"    • Total Top Spenders: {ts['total_top_spenders']}")
        print(f"    • Revenue from Top Spenders: ₹{ts['total_revenue_from_top_spenders']:,.2f}")
        print(f"    • Avg Spend per Top Customer: ₹{ts['avg_spend_per_top_customer']:,.2f}")
        
        top_spenders_list = ts.get('top_spenders_list', [])
        if top_spenders_list:
            print(f"    • Top 5 Spenders:")
            for i, spender in enumerate(top_spenders_list[:5], 1):
                print(f"      {i}. {spender['customer_name']} ({spender['region']}) - ₹{spender['recent_total_spent']:,.0f}")
    
    print("\n" + "="*60)
    print("Results exported to: data/outputs/table_pipeline/table_kpi_results.json")
    print("="*60)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
