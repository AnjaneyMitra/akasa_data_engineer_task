"""
Data Cleaner - Unified interface for data processing
Coordinates CSV and XML parsing with comprehensive validation.
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.common.logger import setup_logger
from src.data_processing.csv_parser import CustomerCSVParser
from src.data_processing.xml_parser import OrderXMLParser

logger = setup_logger(__name__)


class DataCleaner:
    """Unified data cleaner for customer and order data."""
    
    def __init__(self):
        self.csv_parser = CustomerCSVParser()
        self.xml_parser = OrderXMLParser()
        self.customers_df = None
        self.orders_df = None
        self.processing_summary = {}
    
    def process_customer_data(self, csv_file_path: str) -> Tuple[bool, List[str]]:
        """
        Process customer CSV data.
        
        Args:
            csv_file_path: Path to customer CSV file
            
        Returns:
            Tuple of (success, errors)
        """
        logger.info(f"Processing customer data from: {csv_file_path}")
        
        try:
            self.customers_df, errors = self.csv_parser.parse_to_dataframe(csv_file_path)
            
            if self.customers_df is not None:
                logger.info(f"Successfully processed {len(self.customers_df)} customer records")
                return True, errors
            else:
                logger.error("Failed to process customer data")
                return False, errors
                
        except Exception as e:
            error_msg = f"Unexpected error processing customer data: {str(e)}"
            logger.error(error_msg)
            return False, [error_msg]
    
    def process_order_data(self, xml_file_path: str) -> Tuple[bool, List[str]]:
        """
        Process order XML data.
        
        Args:
            xml_file_path: Path to order XML file
            
        Returns:
            Tuple of (success, errors)
        """
        logger.info(f"Processing order data from: {xml_file_path}")
        
        try:
            self.orders_df, errors = self.xml_parser.parse_to_dataframe(xml_file_path)
            
            if self.orders_df is not None:
                logger.info(f"Successfully processed {len(self.orders_df)} order records")
                return True, errors
            else:
                logger.error("Failed to process order data")
                return False, errors
                
        except Exception as e:
            error_msg = f"Unexpected error processing order data: {str(e)}"
            logger.error(error_msg)
            return False, [error_msg]
    
    def validate_data_consistency(self) -> Tuple[bool, List[str]]:
        """
        Validate consistency between customer and order data.
        
        Returns:
            Tuple of (is_consistent, issues)
        """
        if self.customers_df is None or self.orders_df is None:
            return False, ["Both customer and order data must be processed first"]
        
        issues = []
        
        # Check for orders from non-existent customers
        customer_mobiles = set(self.customers_df['mobile_number'])
        order_mobiles = set(self.orders_df['mobile_number'])
        
        orphan_mobiles = order_mobiles - customer_mobiles
        if orphan_mobiles:
            issues.append(f"Found {len(orphan_mobiles)} orders from customers not in customer data: "
                         f"{list(orphan_mobiles)[:5]}{'...' if len(orphan_mobiles) > 5 else ''}")
        
        # Check for customers with no orders
        customers_without_orders = customer_mobiles - order_mobiles
        if customers_without_orders:
            issues.append(f"Found {len(customers_without_orders)} customers with no orders: "
                         f"{list(customers_without_orders)[:5]}{'...' if len(customers_without_orders) > 5 else ''}")
        
        # Check for duplicate customer IDs
        if self.customers_df['customer_id'].duplicated().any():
            duplicates = self.customers_df[self.customers_df['customer_id'].duplicated()]['customer_id'].tolist()
            issues.append(f"Found duplicate customer IDs: {duplicates}")
        
        # Check for duplicate mobile numbers in customers
        if self.customers_df['mobile_number'].duplicated().any():
            duplicates = self.customers_df[self.customers_df['mobile_number'].duplicated()]['mobile_number'].tolist()
            issues.append(f"Found duplicate mobile numbers in customers: {duplicates}")
        
        # Check for duplicate order IDs
        if self.orders_df['order_id'].duplicated().any():
            duplicates = self.orders_df[self.orders_df['order_id'].duplicated()]['order_id'].tolist()
            issues.append(f"Found duplicate order IDs: {duplicates}")
        
        # Log issues
        for issue in issues:
            logger.warning(f"Data consistency issue: {issue}")
        
        is_consistent = len(issues) == 0
        if is_consistent:
            logger.info("Data consistency validation passed")
        else:
            logger.warning(f"Data consistency validation failed with {len(issues)} issues")
        
        return is_consistent, issues
    
    def create_enriched_dataset(self) -> Optional[pd.DataFrame]:
        """
        Create an enriched dataset by joining customer and order data.
        
        Returns:
            Enriched DataFrame or None if processing failed
        """
        if self.customers_df is None or self.orders_df is None:
            logger.error("Both customer and order data must be processed first")
            return None
        
        try:
            # Join datasets on mobile_number
            enriched_df = pd.merge(
                self.orders_df,
                self.customers_df,
                on='mobile_number',
                how='left',
                suffixes=('_order', '_customer')
            )
            
            # Add derived columns
            enriched_df['days_since_order'] = (
                pd.Timestamp.now() - enriched_df['order_date_time']
            ).dt.days
            
            # Add categorical columns for analysis
            enriched_df['region'] = enriched_df['region'].astype('category')
            enriched_df['sku_category'] = enriched_df['sku_id'].str.extract(r'SKU-(\d)')[0].map({
                '1': 'Electronics',
                '2': 'Clothing', 
                '3': 'Books',
                '4': 'Home'
            }).astype('category')
            
            logger.info(f"Created enriched dataset with {len(enriched_df)} records")
            return enriched_df
            
        except Exception as e:
            logger.error(f"Error creating enriched dataset: {str(e)}")
            return None
    
    def generate_processing_summary(self) -> Dict:
        """
        Generate comprehensive processing summary.
        
        Returns:
            Dictionary containing processing statistics
        """
        summary = {
            "processing_timestamp": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            "customers": {},
            "orders": {},
            "data_quality": {}
        }
        
        # Customer summary
        if self.customers_df is not None:
            customer_report = self.csv_parser.get_data_quality_report()
            summary["customers"] = {
                "total_records": len(self.customers_df),
                "data_quality": customer_report
            }
        
        # Order summary
        if self.orders_df is not None:
            order_report = self.xml_parser.get_data_quality_report()
            summary["orders"] = {
                "total_records": len(self.orders_df),
                "data_quality": order_report
            }
        
        # Data consistency
        if self.customers_df is not None and self.orders_df is not None:
            is_consistent, issues = self.validate_data_consistency()
            summary["data_quality"] = {
                "is_consistent": is_consistent,
                "consistency_issues": issues,
                "customer_order_coverage": {
                    "customers_with_orders": len(set(self.orders_df['mobile_number'])),
                    "total_customers": len(self.customers_df),
                    "coverage_percentage": (len(set(self.orders_df['mobile_number'])) / len(self.customers_df)) * 100
                }
            }
        
        self.processing_summary = summary
        return summary
    
    def get_customers_dataframe(self) -> Optional[pd.DataFrame]:
        """Get processed customers DataFrame."""
        return self.customers_df.copy() if self.customers_df is not None else None
    
    def get_orders_dataframe(self) -> Optional[pd.DataFrame]:
        """Get processed orders DataFrame.""" 
        return self.orders_df.copy() if self.orders_df is not None else None
    
    def export_cleaned_data(self, output_dir: str = "data/processed") -> Dict[str, str]:
        """
        Export cleaned data to files.
        
        Args:
            output_dir: Directory to save cleaned files
            
        Returns:
            Dictionary with file paths of exported data
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        exported_files = {}
        
        try:
            # Export customers
            if self.customers_df is not None:
                customer_file = output_path / "cleaned_customers.csv"
                self.customers_df.to_csv(customer_file, index=False)
                exported_files["customers"] = str(customer_file)
                logger.info(f"Exported cleaned customers to: {customer_file}")
            
            # Export orders  
            if self.orders_df is not None:
                orders_file = output_path / "cleaned_orders.csv"
                self.orders_df.to_csv(orders_file, index=False)
                exported_files["orders"] = str(orders_file)
                logger.info(f"Exported cleaned orders to: {orders_file}")
            
            # Export enriched dataset
            enriched_df = self.create_enriched_dataset()
            if enriched_df is not None:
                enriched_file = output_path / "enriched_data.csv"
                enriched_df.to_csv(enriched_file, index=False)
                exported_files["enriched"] = str(enriched_file)
                logger.info(f"Exported enriched data to: {enriched_file}")
            
            # Export processing summary
            if self.processing_summary:
                import json
                summary_file = output_path / "processing_summary.json"
                
                # Convert non-serializable objects to strings
                summary_copy = self.processing_summary.copy()
                
                with open(summary_file, 'w') as f:
                    json.dump(summary_copy, f, indent=2, default=str)
                exported_files["summary"] = str(summary_file)
                logger.info(f"Exported processing summary to: {summary_file}")
            
        except Exception as e:
            logger.error(f"Error exporting cleaned data: {str(e)}")
        
        return exported_files
