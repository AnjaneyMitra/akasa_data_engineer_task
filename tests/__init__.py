"""
Test suite for Akasa Data Engineering Pipeline.
Comprehensive validation tests for both table-based and in-memory approaches.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

__version__ = "1.0.0"
__author__ = "Data Engineering Test Team"
