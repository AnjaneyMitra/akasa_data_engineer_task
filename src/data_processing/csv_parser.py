"""
CSV Parser for Customer Data
Handles parsing, validation, and cleaning of customer CSV files.
"""

import csv
import pandas as pd
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.common.logger import setup_logger
from src.common.utils import validate_file_exists, normalize_mobile_number

logger = setup_logger(__name__)


class CustomerCSVParser:
    """Parser for customer CSV data with validation and cleaning."""
    
    REQUIRED_COLUMNS = ['customer_id', 'customer_name', 'mobile_number', 'region']
    VALID_REGIONS = ['North', 'South', 'East', 'West', 'Central', 'Northeast']
    
    def __init__(self):
        self.validation_errors = []
        self.cleaned_data = []
    
    def validate_customer_record(self, row: Dict, row_num: int) -> Tuple[bool, Dict]:
        """
        Validate a single customer record.
        
        Args:
            row: Dictionary containing customer data
            row_num: Row number for error reporting
            
        Returns:
            Tuple of (is_valid, cleaned_row)
        """
        errors = []
        cleaned_row = row.copy()
        
        # Validate customer_id
        customer_id = row.get('customer_id', '').strip()
        if not customer_id:
            errors.append(f"Row {row_num}: Missing customer_id")
        elif not customer_id.startswith('CUST-'):
            errors.append(f"Row {row_num}: Invalid customer_id format: {customer_id}")
        else:
            cleaned_row['customer_id'] = customer_id
        
        # Validate customer_name
        customer_name = row.get('customer_name', '').strip()
        if not customer_name:
            errors.append(f"Row {row_num}: Missing customer_name")
        elif len(customer_name) < 2:
            errors.append(f"Row {row_num}: Customer name too short: {customer_name}")
        else:
            # Clean name: proper case, remove extra spaces
            cleaned_name = ' '.join(word.capitalize() for word in customer_name.split())
            cleaned_row['customer_name'] = cleaned_name
        
        # Validate mobile_number
        mobile_number = row.get('mobile_number', '').strip()
        if not mobile_number:
            errors.append(f"Row {row_num}: Missing mobile_number")
        else:
            normalized_mobile = normalize_mobile_number(mobile_number)
            if not normalized_mobile:
                errors.append(f"Row {row_num}: Invalid mobile_number: {mobile_number}")
            else:
                cleaned_row['mobile_number'] = normalized_mobile
        
        # Validate region
        region = row.get('region', '').strip()
        if not region:
            errors.append(f"Row {row_num}: Missing region")
        elif region not in self.VALID_REGIONS:
            errors.append(f"Row {row_num}: Invalid region: {region}. "
                         f"Valid regions: {', '.join(self.VALID_REGIONS)}")
        else:
            cleaned_row['region'] = region
        
        # Log errors if any
        if errors:
            for error in errors:
                logger.warning(error)
            self.validation_errors.extend(errors)
            return False, row
        
        return True, cleaned_row
    
    def parse_csv_file(self, file_path: str) -> Tuple[List[Dict], List[str]]:
        """
        Parse and validate CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple of (valid_records, error_list)
        """
        logger.info(f"Starting to parse CSV file: {file_path}")
        
        # Reset state
        self.validation_errors = []
        self.cleaned_data = []
        
        # Validate file exists
        if not validate_file_exists(file_path):
            error_msg = f"Cannot access file: {file_path}"
            logger.error(error_msg)
            return [], [error_msg]
        
        try:
            valid_records = []
            
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                # Detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                # Read CSV
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                # Validate headers
                if not all(col in reader.fieldnames for col in self.REQUIRED_COLUMNS):
                    missing_cols = set(self.REQUIRED_COLUMNS) - set(reader.fieldnames or [])
                    error_msg = f"Missing required columns: {missing_cols}"
                    logger.error(error_msg)
                    return [], [error_msg]
                
                # Process each row
                row_num = 1  # Start from 1 (after header)
                for row in reader:
                    row_num += 1
                    
                    # Skip empty rows
                    if all(not str(value).strip() for value in row.values()):
                        continue
                    
                    is_valid, cleaned_row = self.validate_customer_record(row, row_num)
                    
                    if is_valid:
                        valid_records.append(cleaned_row)
                        self.cleaned_data.append(cleaned_row)
                    
            logger.info(f"Processed {row_num-1} rows, {len(valid_records)} valid records, "
                       f"{len(self.validation_errors)} errors")
            
            return valid_records, self.validation_errors
            
        except Exception as e:
            error_msg = f"Error parsing CSV file {file_path}: {str(e)}"
            logger.error(error_msg)
            return [], [error_msg]
    
    def parse_to_dataframe(self, file_path: str) -> Tuple[Optional[pd.DataFrame], List[str]]:
        """
        Parse CSV file and return as pandas DataFrame.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple of (DataFrame or None, error_list)
        """
        valid_records, errors = self.parse_csv_file(file_path)
        
        if not valid_records:
            return None, errors
        
        try:
            df = pd.DataFrame(valid_records)
            
            # Ensure proper data types
            df['customer_id'] = df['customer_id'].astype('string')
            df['customer_name'] = df['customer_name'].astype('string')
            df['mobile_number'] = df['mobile_number'].astype('string')
            df['region'] = df['region'].astype('category')
            
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
        total_records = len(self.cleaned_data)
        
        # Region distribution
        region_counts = {}
        mobile_lengths = []
        
        for record in self.cleaned_data:
            region = record['region']
            region_counts[region] = region_counts.get(region, 0) + 1
            mobile_lengths.append(len(record['mobile_number']))
        
        # Duplicate checks
        customer_ids = [record['customer_id'] for record in self.cleaned_data]
        mobile_numbers = [record['mobile_number'] for record in self.cleaned_data]
        
        duplicate_customer_ids = len(customer_ids) - len(set(customer_ids))
        duplicate_mobile_numbers = len(mobile_numbers) - len(set(mobile_numbers))
        
        return {
            "total_records": total_records,
            "validation_errors": len(self.validation_errors),
            "region_distribution": region_counts,
            "duplicate_customer_ids": duplicate_customer_ids,
            "duplicate_mobile_numbers": duplicate_mobile_numbers,
            "mobile_number_stats": {
                "min_length": min(mobile_lengths) if mobile_lengths else 0,
                "max_length": max(mobile_lengths) if mobile_lengths else 0,
                "avg_length": sum(mobile_lengths) / len(mobile_lengths) if mobile_lengths else 0
            }
        }
