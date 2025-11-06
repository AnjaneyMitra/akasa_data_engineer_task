"""
Utility functions for the data pipeline.
Contains common helper functions used across different modules.
"""

import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import pandas as pd

from .logger import setup_logger

logger = setup_logger(__name__)


def validate_file_exists(file_path: Union[str, Path]) -> bool:
    """
    Validate that a file exists and is readable.
    
    Args:
        file_path: Path to the file to validate
        
    Returns:
        True if file exists and is readable, False otherwise
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.error(f"File does not exist: {file_path}")
            return False
        
        if not path.is_file():
            logger.error(f"Path is not a file: {file_path}")
            return False
            
        if not os.access(path, os.R_OK):
            logger.error(f"File is not readable: {file_path}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {str(e)}")
        return False


def normalize_mobile_number(mobile: str) -> Optional[str]:
    """
    Normalize mobile number format for consistent processing.
    
    Args:
        mobile: Mobile number string
        
    Returns:
        Normalized mobile number or None if invalid
    """
    if not mobile or not isinstance(mobile, str):
        return None
    
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', mobile)
    
    # Indian mobile numbers should be 10 digits
    if len(digits_only) == 10 and digits_only.startswith(('6', '7', '8', '9')):
        return digits_only
    elif len(digits_only) == 13 and digits_only.startswith('91'):
        # Remove country code
        return digits_only[2:]
    elif len(digits_only) == 12 and digits_only.startswith('91'):
        return digits_only[2:]
    
    logger.warning(f"Invalid mobile number format: {mobile}")
    return None


def normalize_datetime(dt_str: str, timezone: str = 'UTC') -> Optional[datetime]:
    """
    Normalize datetime string to consistent format with timezone awareness.
    
    Args:
        dt_str: DateTime string to normalize
        timezone: Target timezone for normalization
        
    Returns:
        Normalized datetime object or None if parsing fails
    """
    if not dt_str:
        return None
    
    # Common datetime formats to try
    formats = [
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%d-%m-%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            # For now, we'll treat all datetime as UTC
            # In production, you'd handle timezone conversion properly
            return dt
        except ValueError:
            continue
    
    logger.warning(f"Could not parse datetime: {dt_str}")
    return None


def get_date_range_last_n_days(n_days: int) -> tuple[datetime, datetime]:
    """
    Get start and end datetime for last N days from current date.
    
    Args:
        n_days: Number of days to look back
        
    Returns:
        Tuple of (start_datetime, end_datetime)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=n_days)
    
    # Set to beginning of start day and end of end day
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return start_date, end_date


def safe_numeric_conversion(value: Any, default: float = 0.0) -> float:
    """
    Safely convert value to numeric, handling various edge cases.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Numeric value or default
    """
    if pd.isna(value) or value is None:
        return default
    
    try:
        # Handle string values
        if isinstance(value, str):
            # Remove currency symbols, commas, etc.
            cleaned = re.sub(r'[^\d.-]', '', value)
            if not cleaned:
                return default
            return float(cleaned)
        
        # Direct conversion for numeric types
        return float(value)
    
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not convert '{value}' to numeric: {str(e)}")
        return default


def ensure_directory_exists(directory_path: Union[str, Path]) -> bool:
    """
    Ensure a directory exists, create it if it doesn't.
    
    Args:
        directory_path: Path to the directory
        
    Returns:
        True if directory exists or was created successfully
    """
    try:
        Path(directory_path).mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Could not create directory {directory_path}: {str(e)}")
        return False


def get_file_stats(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Get basic statistics about a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary containing file statistics
    """
    try:
        path = Path(file_path)
        if not path.exists():
            return {}
        
        stat = path.stat()
        return {
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'modified_time': datetime.fromtimestamp(stat.st_mtime),
            'is_readable': os.access(path, os.R_OK),
            'extension': path.suffix.lower()
        }
    except Exception as e:
        logger.error(f"Error getting file stats for {file_path}: {str(e)}")
        return {}


def batch_process_data(
    data: List[Any], 
    batch_size: int, 
    process_func: callable
) -> List[Any]:
    """
    Process data in batches to handle memory constraints.
    
    Args:
        data: List of data items to process
        batch_size: Size of each batch
        process_func: Function to process each batch
        
    Returns:
        List of processed results
    """
    results = []
    total_batches = (len(data) + batch_size - 1) // batch_size
    
    for i in range(0, len(data), batch_size):
        batch_num = (i // batch_size) + 1
        batch = data[i:i + batch_size]
        
        logger.info(f"Processing batch {batch_num}/{total_batches} "
                   f"(items {i+1}-{min(i+batch_size, len(data))})")
        
        try:
            batch_result = process_func(batch)
            results.extend(batch_result if isinstance(batch_result, list) else [batch_result])
        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {str(e)}")
            continue
    
    return results
