"""
XML Parser for Order Data
Handles parsing, validation, and cleaning of order XML files.
"""

import xml.etree.ElementTree as ET
import pandas as pd
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.common.logger import setup_logger
from src.common.utils import (
    validate_file_exists, 
    normalize_mobile_number, 
    normalize_datetime, 
    safe_numeric_conversion
)

logger = setup_logger(__name__)


class OrderXMLParser:
    """Parser for order XML data with validation and cleaning."""
    
    REQUIRED_FIELDS = ['order_id', 'mobile_number', 'order_date_time', 'sku_id', 'sku_count', 'total_amount']
    
    def __init__(self):
        self.validation_errors = []
        self.cleaned_data = []
    
    def validate_order_record(self, order_elem: ET.Element, order_num: int) -> Tuple[bool, Dict]:
        """
        Validate a single order record.
        
        Args:
            order_elem: XML element containing order data
            order_num: Order number for error reporting
            
        Returns:
            Tuple of (is_valid, cleaned_order)
        """
        errors = []
        cleaned_order = {}
        
        # Extract all fields
        order_data = {}
        for field in self.REQUIRED_FIELDS:
            elem = order_elem.find(field)
            order_data[field] = elem.text.strip() if elem is not None and elem.text else ''
        
        # Validate order_id
        order_id = order_data.get('order_id', '')
        if not order_id:
            errors.append(f"Order {order_num}: Missing order_id")
        elif not order_id.startswith('ORD-'):
            errors.append(f"Order {order_num}: Invalid order_id format: {order_id}")
        else:
            cleaned_order['order_id'] = order_id
        
        # Validate mobile_number
        mobile_number = order_data.get('mobile_number', '')
        if not mobile_number:
            errors.append(f"Order {order_num}: Missing mobile_number")
        else:
            normalized_mobile = normalize_mobile_number(mobile_number)
            if not normalized_mobile:
                errors.append(f"Order {order_num}: Invalid mobile_number: {mobile_number}")
            else:
                cleaned_order['mobile_number'] = normalized_mobile
        
        # Validate order_date_time
        order_date_str = order_data.get('order_date_time', '')
        if not order_date_str:
            errors.append(f"Order {order_num}: Missing order_date_time")
        else:
            parsed_datetime = normalize_datetime(order_date_str)
            if not parsed_datetime:
                errors.append(f"Order {order_num}: Invalid order_date_time format: {order_date_str}")
            else:
                cleaned_order['order_date_time'] = parsed_datetime
                cleaned_order['order_date_time_str'] = order_date_str  # Keep original string
        
        # Validate sku_id
        sku_id = order_data.get('sku_id', '')
        if not sku_id:
            errors.append(f"Order {order_num}: Missing sku_id")
        elif not sku_id.startswith('SKU-'):
            errors.append(f"Order {order_num}: Invalid sku_id format: {sku_id}")
        else:
            cleaned_order['sku_id'] = sku_id
        
        # Validate sku_count
        sku_count_str = order_data.get('sku_count', '')
        if not sku_count_str:
            errors.append(f"Order {order_num}: Missing sku_count")
        else:
            sku_count = safe_numeric_conversion(sku_count_str, default=0)
            if sku_count <= 0:
                errors.append(f"Order {order_num}: Invalid sku_count: {sku_count_str}")
            else:
                cleaned_order['sku_count'] = int(sku_count)
        
        # Validate total_amount
        total_amount_str = order_data.get('total_amount', '')
        if not total_amount_str:
            errors.append(f"Order {order_num}: Missing total_amount")
        else:
            total_amount = safe_numeric_conversion(total_amount_str, default=0)
            if total_amount <= 0:
                errors.append(f"Order {order_num}: Invalid total_amount: {total_amount_str}")
            else:
                cleaned_order['total_amount'] = float(total_amount)
        
        # Additional business logic validations
        if 'sku_count' in cleaned_order and 'total_amount' in cleaned_order:
            # Check if unit price is reasonable (between ₹10 and ₹1,00,000)
            unit_price = cleaned_order['total_amount'] / cleaned_order['sku_count']
            if unit_price < 10 or unit_price > 100000:
                errors.append(f"Order {order_num}: Suspicious unit price ₹{unit_price:.2f}")
        
        # Log errors if any
        if errors:
            for error in errors:
                logger.warning(error)
            self.validation_errors.extend(errors)
            return False, order_data
        
        return True, cleaned_order
    
    def parse_xml_file(self, file_path: str) -> Tuple[List[Dict], List[str]]:
        """
        Parse and validate XML file.
        
        Args:
            file_path: Path to the XML file
            
        Returns:
            Tuple of (valid_orders, error_list)
        """
        logger.info(f"Starting to parse XML file: {file_path}")
        
        # Reset state
        self.validation_errors = []
        self.cleaned_data = []
        
        # Validate file exists
        if not validate_file_exists(file_path):
            error_msg = f"Cannot access file: {file_path}"
            logger.error(error_msg)
            return [], [error_msg]
        
        try:
            # Parse XML
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            if root.tag != 'orders':
                error_msg = f"Invalid XML structure: Expected root element 'orders', got '{root.tag}'"
                logger.error(error_msg)
                return [], [error_msg]
            
            valid_orders = []
            order_elements = root.findall('order')
            
            if not order_elements:
                error_msg = "No order elements found in XML"
                logger.warning(error_msg)
                return [], [error_msg]
            
            # Process each order
            for order_num, order_elem in enumerate(order_elements, 1):
                is_valid, cleaned_order = self.validate_order_record(order_elem, order_num)
                
                if is_valid:
                    valid_orders.append(cleaned_order)
                    self.cleaned_data.append(cleaned_order)
            
            logger.info(f"Processed {len(order_elements)} orders, {len(valid_orders)} valid orders, "
                       f"{len(self.validation_errors)} errors")
            
            return valid_orders, self.validation_errors
            
        except ET.ParseError as e:
            error_msg = f"XML parsing error in {file_path}: {str(e)}"
            logger.error(error_msg)
            return [], [error_msg]
        except Exception as e:
            error_msg = f"Error parsing XML file {file_path}: {str(e)}"
            logger.error(error_msg)
            return [], [error_msg]
    
    def parse_to_dataframe(self, file_path: str) -> Tuple[Optional[pd.DataFrame], List[str]]:
        """
        Parse XML file and return as pandas DataFrame.
        
        Args:
            file_path: Path to the XML file
            
        Returns:
            Tuple of (DataFrame or None, error_list)
        """
        valid_orders, errors = self.parse_xml_file(file_path)
        
        if not valid_orders:
            return None, errors
        
        try:
            df = pd.DataFrame(valid_orders)
            
            # Ensure proper data types
            df['order_id'] = df['order_id'].astype('string')
            df['mobile_number'] = df['mobile_number'].astype('string')
            df['sku_id'] = df['sku_id'].astype('string')
            df['sku_count'] = df['sku_count'].astype('int32')
            df['total_amount'] = df['total_amount'].astype('float64')
            
            # Convert datetime column properly
            df['order_date_time'] = pd.to_datetime(df['order_date_time'])
            
            # Add derived columns for analysis
            df['order_date'] = df['order_date_time'].dt.date
            df['order_month'] = df['order_date_time'].dt.to_period('M')
            df['unit_price'] = df['total_amount'] / df['sku_count']
            
            logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
            return df, errors
            
        except Exception as e:
            error_msg = f"Error creating DataFrame: {str(e)}"
            logger.error(error_msg)
            return None, errors + [error_msg]
    
    def get_data_quality_report(self) -> Dict:
        """
        Generate a data quality report.
        
        Returns:
            Dictionary containing data quality metrics
        """
        if not self.cleaned_data:
            return {"status": "No data processed"}
        
        # Basic statistics
        total_orders = len(self.cleaned_data)
        
        # Date range analysis
        dates = [order['order_date_time'] for order in self.cleaned_data]
        min_date = min(dates) if dates else None
        max_date = max(dates) if dates else None
        
        # Amount statistics
        amounts = [order['total_amount'] for order in self.cleaned_data]
        quantities = [order['sku_count'] for order in self.cleaned_data]
        
        # SKU analysis
        skus = [order['sku_id'] for order in self.cleaned_data]
        unique_skus = len(set(skus))
        
        # Customer analysis
        customers = [order['mobile_number'] for order in self.cleaned_data]
        unique_customers = len(set(customers))
        
        # Duplicate checks
        order_ids = [order['order_id'] for order in self.cleaned_data]
        duplicate_order_ids = len(order_ids) - len(set(order_ids))
        
        return {
            "total_orders": total_orders,
            "validation_errors": len(self.validation_errors),
            "date_range": {
                "min_date": min_date.strftime('%Y-%m-%d %H:%M:%S') if min_date else None,
                "max_date": max_date.strftime('%Y-%m-%d %H:%M:%S') if max_date else None,
                "days_span": (max_date - min_date).days if min_date and max_date else 0
            },
            "amount_stats": {
                "min_amount": min(amounts) if amounts else 0,
                "max_amount": max(amounts) if amounts else 0,
                "avg_amount": sum(amounts) / len(amounts) if amounts else 0,
                "total_revenue": sum(amounts) if amounts else 0
            },
            "quantity_stats": {
                "min_quantity": min(quantities) if quantities else 0,
                "max_quantity": max(quantities) if quantities else 0,
                "avg_quantity": sum(quantities) / len(quantities) if quantities else 0,
                "total_items": sum(quantities) if quantities else 0
            },
            "sku_analysis": {
                "unique_skus": unique_skus,
                "total_sku_instances": len(skus)
            },
            "customer_analysis": {
                "unique_customers": unique_customers,
                "total_orders": total_orders,
                "avg_orders_per_customer": total_orders / unique_customers if unique_customers > 0 else 0
            },
            "duplicate_order_ids": duplicate_order_ids
        }
