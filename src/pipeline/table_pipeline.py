"""
Table-based data pipeline using MySQL database.
Implements the complete pipeline from data ingestion to KPI calculation using SQL.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))

from config.database import db_config
from database.operations import DatabaseOperations
from data_processing.data_cleaner import DataCleaner
from common.logger import setup_logger

logger = setup_logger(__name__)


class TableBasedPipeline:
    """
    Complete table-based data pipeline for KPI calculation.
    Uses MySQL database for storage and SQL for calculations.
    """
    
    def __init__(self):
        """Initialize the table-based pipeline."""
        self.db_ops: Optional[DatabaseOperations] = None
        self.session = None
        self.pipeline_start_time = None
        self.results = {}
    
    def initialize(self) -> bool:
        """
        Initialize database connection and operations.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Test database connection
            if not db_config.test_connection():
                logger.error("Database connection failed")
                return False
            
            # Create session
            self.session = db_config.get_session()
            if self.session is None:
                logger.error("Failed to create database session")
                return False
            
            # Initialize database operations
            self.db_ops = DatabaseOperations(self.session)
            
            logger.info("Table-based pipeline initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {str(e)}")
            return False
    
    def ingest_data(self, customer_file: str, orders_file: str, incremental: bool = True) -> bool:
        """
        Ingest and process data files into database tables.
        
        Args:
            customer_file: Path to customer CSV file
            orders_file: Path to orders XML file
            incremental: Whether this is incremental data (updates existing) or full refresh
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting data ingestion - Incremental: {incremental}")
            
            # Initialize data processor
            data_cleaner = DataCleaner()
            
            # Process customer data
            if not data_cleaner.process_customer_data(customer_file):
                logger.error("Failed to process customer data")
                return False
            
            # Process order data
            if not data_cleaner.process_order_data(orders_file):
                logger.error("Failed to process order data")
                return False
            
            # Get processed DataFrames
            customers_df = data_cleaner.get_customers()
            orders_df = data_cleaner.get_orders()
            
            if customers_df is None or orders_df is None:
                logger.error("Failed to get processed data")
                return False
            
            # Load data into database
            logger.info("Loading customers into database...")
            success, errors = self.db_ops.bulk_insert_customers(customers_df)
            if not success:
                logger.error(f"Customer loading failed: {errors}")
                return False
            
            logger.info("Loading orders into database...")
            success, errors = self.db_ops.bulk_insert_orders(orders_df)
            if not success:
                logger.error(f"Order loading failed: {errors}")
                return False
            
            # Log ingestion statistics
            summary = self.db_ops.get_database_summary()
            logger.info("Data ingestion completed:")
            logger.info(f"  Total customers: {summary.get('customers', {}).get('total_count', 0)}")
            logger.info(f"  Total orders: {summary.get('orders', {}).get('total_count', 0)}")
            logger.info(f"  Total revenue: ₹{summary.get('orders', {}).get('total_revenue', 0):,.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Data ingestion failed: {str(e)}")
            return False
    
    def calculate_repeat_customers(self) -> Dict[str, Any]:
        """
        Calculate repeat customers KPI using SQL.
        
        Returns:
            Dictionary with repeat customers data and metadata
        """
        try:
            logger.info("Calculating repeat customers KPI...")
            
            repeat_customers = self.db_ops.get_repeat_customers()
            
            # Calculate summary statistics
            total_repeat_customers = len(repeat_customers)
            total_orders_by_repeat_customers = sum(c['order_count'] for c in repeat_customers)
            total_revenue_from_repeat_customers = sum(c['total_spent'] for c in repeat_customers)
            
            result = {
                'kpi_name': 'repeat_customers',
                'calculation_time': datetime.utcnow().isoformat(),
                'total_repeat_customers': total_repeat_customers,
                'total_orders_by_repeat_customers': total_orders_by_repeat_customers,
                'total_revenue_from_repeat_customers': total_revenue_from_repeat_customers,
                'avg_orders_per_repeat_customer': total_orders_by_repeat_customers / total_repeat_customers if total_repeat_customers > 0 else 0,
                'avg_revenue_per_repeat_customer': total_revenue_from_repeat_customers / total_repeat_customers if total_repeat_customers > 0 else 0,
                'repeat_customers_list': repeat_customers[:10],  # Top 10 for summary
                'full_data_count': len(repeat_customers)
            }
            
            logger.info(f"Found {total_repeat_customers} repeat customers")
            logger.info(f"Revenue from repeat customers: ₹{total_revenue_from_repeat_customers:,.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Repeat customers calculation failed: {str(e)}")
            return {'error': str(e)}
    
    def calculate_monthly_trends(self) -> Dict[str, Any]:
        """
        Calculate monthly order trends KPI using SQL.
        
        Returns:
            Dictionary with monthly trends data and metadata
        """
        try:
            logger.info("Calculating monthly order trends KPI...")
            
            monthly_trends = self.db_ops.get_monthly_order_trends()
            
            # Calculate trend analysis
            if len(monthly_trends) >= 2:
                latest_month = monthly_trends[-1]
                previous_month = monthly_trends[-2]
                
                order_growth = ((latest_month['order_count'] - previous_month['order_count']) / 
                               previous_month['order_count'] * 100) if previous_month['order_count'] > 0 else 0
                
                revenue_growth = ((latest_month['total_revenue'] - previous_month['total_revenue']) / 
                                 previous_month['total_revenue'] * 100) if previous_month['total_revenue'] > 0 else 0
            else:
                order_growth = 0
                revenue_growth = 0
            
            total_months = len(monthly_trends)
            total_orders = sum(m['order_count'] for m in monthly_trends)
            total_revenue = sum(m['total_revenue'] for m in monthly_trends)
            
            result = {
                'kpi_name': 'monthly_order_trends',
                'calculation_time': datetime.utcnow().isoformat(),
                'total_months_analyzed': total_months,
                'total_orders_all_months': total_orders,
                'total_revenue_all_months': total_revenue,
                'avg_orders_per_month': total_orders / total_months if total_months > 0 else 0,
                'avg_revenue_per_month': total_revenue / total_months if total_months > 0 else 0,
                'latest_month_order_growth_%': round(order_growth, 2),
                'latest_month_revenue_growth_%': round(revenue_growth, 2),
                'monthly_data': monthly_trends
            }
            
            logger.info(f"Analyzed {total_months} months of order trends")
            logger.info(f"Latest month growth - Orders: {order_growth:.1f}%, Revenue: {revenue_growth:.1f}%")
            
            return result
            
        except Exception as e:
            logger.error(f"Monthly trends calculation failed: {str(e)}")
            return {'error': str(e)}
    
    def calculate_regional_revenue(self) -> Dict[str, Any]:
        """
        Calculate regional revenue KPI using SQL.
        
        Returns:
            Dictionary with regional revenue data and metadata
        """
        try:
            logger.info("Calculating regional revenue KPI...")
            
            regional_revenue = self.db_ops.get_regional_revenue()
            
            # Calculate summary statistics
            total_revenue = sum(r['total_revenue'] for r in regional_revenue)
            total_customers = sum(r['customer_count'] for r in regional_revenue)
            total_orders = sum(r['order_count'] for r in regional_revenue)
            
            # Find top and bottom regions
            top_region = max(regional_revenue, key=lambda x: x['total_revenue']) if regional_revenue else None
            bottom_region = min(regional_revenue, key=lambda x: x['total_revenue']) if regional_revenue else None
            
            result = {
                'kpi_name': 'regional_revenue',
                'calculation_time': datetime.utcnow().isoformat(),
                'total_regions': len(regional_revenue),
                'total_revenue_all_regions': total_revenue,
                'total_customers_all_regions': total_customers,
                'total_orders_all_regions': total_orders,
                'avg_revenue_per_region': total_revenue / len(regional_revenue) if regional_revenue else 0,
                'top_region': top_region,
                'bottom_region': bottom_region,
                'regional_breakdown': regional_revenue
            }
            
            logger.info(f"Analyzed {len(regional_revenue)} regions")
            if top_region:
                logger.info(f"Top region: {top_region['region']} with ₹{top_region['total_revenue']:,.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Regional revenue calculation failed: {str(e)}")
            return {'error': str(e)}
    
    def calculate_top_spenders(self, days: int = 30, limit: int = 10) -> Dict[str, Any]:
        """
        Calculate top spenders in last N days KPI using SQL.
        
        Args:
            days: Number of days to analyze
            limit: Maximum number of top spenders to return
            
        Returns:
            Dictionary with top spenders data and metadata
        """
        try:
            logger.info(f"Calculating top spenders KPI for last {days} days...")
            
            top_spenders = self.db_ops.get_top_customers_last_n_days(days, limit)
            
            # Calculate summary statistics
            total_top_spenders = len(top_spenders)
            total_revenue_from_top_spenders = sum(c['recent_total_spent'] for c in top_spenders)
            total_orders_from_top_spenders = sum(c['recent_order_count'] for c in top_spenders)
            
            # Regional breakdown of top spenders
            regional_breakdown = {}
            for spender in top_spenders:
                region = spender['region']
                if region not in regional_breakdown:
                    regional_breakdown[region] = {'count': 0, 'total_spent': 0}
                regional_breakdown[region]['count'] += 1
                regional_breakdown[region]['total_spent'] += spender['recent_total_spent']
            
            result = {
                'kpi_name': 'top_spenders_last_n_days',
                'calculation_time': datetime.utcnow().isoformat(),
                'analysis_period_days': days,
                'limit': limit,
                'total_top_spenders': total_top_spenders,
                'total_revenue_from_top_spenders': total_revenue_from_top_spenders,
                'total_orders_from_top_spenders': total_orders_from_top_spenders,
                'avg_spend_per_top_customer': total_revenue_from_top_spenders / total_top_spenders if total_top_spenders > 0 else 0,
                'regional_breakdown_of_top_spenders': regional_breakdown,
                'top_spenders_list': top_spenders
            }
            
            logger.info(f"Found {total_top_spenders} top spenders in last {days} days")
            logger.info(f"Revenue from top spenders: ₹{total_revenue_from_top_spenders:,.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Top spenders calculation failed: {str(e)}")
            return {'error': str(e)}
    
    def run_all_kpis(self) -> Dict[str, Any]:
        """
        Run all KPI calculations and return comprehensive results.
        
        Returns:
            Dictionary with all KPI results and pipeline metadata
        """
        try:
            self.pipeline_start_time = datetime.utcnow()
            logger.info("Starting complete KPI calculation pipeline...")
            
            # Calculate all KPIs
            kpi_results = {
                'repeat_customers': self.calculate_repeat_customers(),
                'monthly_trends': self.calculate_monthly_trends(),
                'regional_revenue': self.calculate_regional_revenue(),
                'top_spenders_30_days': self.calculate_top_spenders(30, 10)
            }
            
            # Create pipeline summary
            pipeline_end_time = datetime.utcnow()
            execution_time = (pipeline_end_time - self.pipeline_start_time).total_seconds()
            
            pipeline_summary = {
                'pipeline_type': 'table_based',
                'execution_start_time': self.pipeline_start_time.isoformat(),
                'execution_end_time': pipeline_end_time.isoformat(),
                'execution_time_seconds': execution_time,
                'database_summary': self.db_ops.get_database_summary(),
                'kpis_calculated': len(kpi_results),
                'kpis_successful': len([k for k in kpi_results.values() if 'error' not in k]),
                'kpis_failed': len([k for k in kpi_results.values() if 'error' in k])
            }
            
            # Compile final results
            self.results = {
                'pipeline_summary': pipeline_summary,
                'kpi_results': kpi_results
            }
            
            logger.info(f"KPI pipeline completed in {execution_time:.2f} seconds")
            logger.info(f"Successful KPIs: {pipeline_summary['kpis_successful']}/{pipeline_summary['kpis_calculated']}")
            
            return self.results
            
        except Exception as e:
            logger.error(f"KPI pipeline execution failed: {str(e)}")
            return {'error': str(e)}
    
    def export_results(self, output_file: str = "data/outputs/table_pipeline/table_kpi_results.json") -> bool:
        """
        Export KPI results to JSON file.
        
        Args:
            output_file: Path to output file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.results:
                logger.error("No results to export. Run KPI calculations first.")
                return False
            
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write results to file
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Results exported to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export results: {str(e)}")
            return False
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if self.session:
                self.session.close()
                logger.info("Database session closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        if self.initialize():
            return self
        else:
            raise Exception("Failed to initialize table-based pipeline")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()


def main():
    """Main function to run the complete table-based pipeline."""
    logger.info("=== Table-Based Pipeline Execution ===")
    
    try:
        with TableBasedPipeline() as pipeline:
            
            # Step 1: Data ingestion (assuming data is already in database)
            # If you need to ingest new data, uncomment the following:
            # success = pipeline.ingest_data(
            #     "data/raw/generated_customers.csv",
            #     "data/raw/generated_orders.xml"
            # )
            # if not success:
            #     logger.error("Data ingestion failed")
            #     return False
            
            # Step 2: Run all KPI calculations
            results = pipeline.run_all_kpis()
            
            if 'error' in results:
                logger.error(f"Pipeline execution failed: {results['error']}")
                return False
            
            # Step 3: Export results
            success = pipeline.export_results()
            if not success:
                logger.warning("Failed to export results to file")
            
            # Step 4: Display summary
            summary = results['pipeline_summary']
            logger.info("=== Pipeline Execution Summary ===")
            logger.info(f"Execution time: {summary['execution_time_seconds']:.2f} seconds")
            logger.info(f"KPIs calculated: {summary['kpis_calculated']}")
            logger.info(f"Successful: {summary['kpis_successful']}")
            logger.info(f"Failed: {summary['kpis_failed']}")
            
            # Display key metrics
            kpis = results['kpi_results']
            if 'repeat_customers' in kpis and 'error' not in kpis['repeat_customers']:
                rc = kpis['repeat_customers']
                logger.info(f"Repeat customers: {rc['total_repeat_customers']}")
            
            if 'regional_revenue' in kpis and 'error' not in kpis['regional_revenue']:
                rr = kpis['regional_revenue']
                logger.info(f"Total revenue: ₹{rr['total_revenue_all_regions']:,.2f}")
                if rr['top_region']:
                    logger.info(f"Top region: {rr['top_region']['region']}")
            
            logger.info("=== Table-Based Pipeline Completed Successfully ===")
            return True
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        return False


if __name__ == "__main__":
    main()
