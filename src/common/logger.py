"""
Logging configuration for the data pipeline.
Provides consistent logging across all modules with security considerations.
"""

import logging
import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def setup_logger(
    name: str, 
    log_level: Optional[str] = None,
    log_to_file: bool = True
) -> logging.Logger:
    """
    Set up a logger with consistent formatting and security measures.
    
    Args:
        name: Logger name (usually __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file in addition to console
    
    Returns:
        Configured logger instance
    """
    
    # Get log level from environment or use default
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    # File handler (if requested)
    if log_to_file:
        # Create logs directory
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        file_handler = logging.FileHandler(
            log_dir / 'pipeline.log',
            encoding='utf-8'
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    
    return logger


def mask_sensitive_data(data: str, mask_char: str = '*') -> str:
    """
    Mask sensitive information for logging.
    
    Args:
        data: String that may contain sensitive information
        mask_char: Character to use for masking
    
    Returns:
        Masked string safe for logging
    """
    if not data or len(data) < 4:
        return mask_char * len(data) if data else ''
    
    # Show first 2 and last 2 characters, mask the rest
    masked_middle = mask_char * (len(data) - 4)
    return f"{data[:2]}{masked_middle}{data[-2:]}"


def log_data_quality_issue(logger: logging.Logger, issue_type: str, details: dict):
    """
    Log data quality issues in a structured format.
    
    Args:
        logger: Logger instance
        issue_type: Type of data quality issue
        details: Dictionary containing issue details (will be sanitized)
    """
    # Sanitize details to avoid logging sensitive information
    sanitized_details = {}
    for key, value in details.items():
        if key.lower() in ['password', 'token', 'key', 'secret']:
            sanitized_details[key] = '[MASKED]'
        elif key.lower() in ['mobile_number', 'phone']:
            sanitized_details[key] = mask_sensitive_data(str(value))
        else:
            sanitized_details[key] = value
    
    logger.warning(f"Data Quality Issue - {issue_type}: {sanitized_details}")


# Create default logger for the module
logger = setup_logger(__name__)
