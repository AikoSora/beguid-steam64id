"""
Configuration classes for Steam64ID to BattlEye GUID Converter.

This module contains dataclass configurations
for database and generation settings.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    """ Database configuration settings. """

    host: str
    port: int
    database: str
    username: str
    password: str
    min_connections: int = 5
    max_connections: int = 20


@dataclass(frozen=True)
class GenerationConfig:
    """ Configuration for Steam64ID generation. """

    start_account: int
    end_account: int
    batch_size: int
    universe: int
    account_type: int
    max_chunk_size: int = 1_000_000
    max_range_size: int = 10_000_000


__all__ = (
    'DatabaseConfig',
    'GenerationConfig',
)
