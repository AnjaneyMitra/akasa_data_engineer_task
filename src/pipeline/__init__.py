 """
Pipeline modules for data processing approaches.
Includes both table-based (MySQL) and in-memory (pandas) implementations.
"""

from .memory_pipeline import InMemoryPipeline
from .table_pipeline import TableBasedPipeline

__all__ = ['InMemoryPipeline', 'TableBasedPipeline']
