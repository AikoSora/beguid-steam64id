"""
Steam64ID to BattlEye GUID Converter Package.

This package provides functionality to generate Steam64ID mappings and convert
them to BattlEye GUID format for database storage and lookup operations.
"""

from .config import DatabaseConfig, GenerationConfig
from .converter import Steam64IDConverter
from .database import DatabaseManager, TableManager
from .generator import Steam64IDGenerator
from .progress import ProgressManager


__version__ = '1.0.0'
__author__ = 'Mikhail Shevelkov'


__all__ = (
    'DatabaseConfig',
    'GenerationConfig',
    'Steam64IDConverter',
    'DatabaseManager',
    'TableManager',
    'Steam64IDGenerator',
    'ProgressManager'
)
