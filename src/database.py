"""
Database management module.

This module handles database connections, table management, and operations.
"""

import logging

from typing import Optional

import asyncpg

from .config import DatabaseConfig


logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections and operations.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """
        Establish database connection pool.
        """

        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections
            )
            logger.info('Database connection pool established')
        except Exception as e:
            logger.error(f'Failed to establish database connection: {e}')
            raise

    async def close(self) -> None:
        """
        Close database connection pool.
        """

        if self.pool:
            await self.pool.close()
            logger.info('Database connection pool closed')

    async def execute_query(self, query: str, *args) -> None:
        """
        Execute a database query.

        Args:
            query: SQL query to execute
            *args: Query parameters
        """

        if not self.pool:
            raise RuntimeError('Database not connected')

        async with self.pool.acquire() as conn:
            await conn.execute(query, *args)


class TableManager:
    """
    Manages database table creation and structure.
    """

    PARTITION_RANGES = [
        (1, 3_000_000),
        (3_000_001, 6_000_000),
        (6_000_001, 9_000_000),
        (9_000_001, 12_000_000),
        (12_000_001, 15_000_000),
        (15_000_001, 18_000_000),
        (18_000_001, 21_000_000),
        (21_000_001, 24_000_000),
        (24_000_001, 27_000_000),
        (27_000_001, 30_000_000),
        (30_000_001, 50_000_000),
        (50_000_001, 100_000_000),
        (100_000_001, 200_000_000),
        (200_000_001, 500_000_000),
        (500_000_001, 1_000_000_000),
        (1_000_000_001, 2_000_000_000),
        (2_000_000_001, 2_147_483_647)
    ]

    @staticmethod
    def get_table_name(account_number: int, universe: int) -> str:
        """
        Get table name for given account number and universe.

        Args:
            account_number: Account number
            universe: Steam universe

        Returns:
            Table name
        """

        for start_range, end_range in TableManager.PARTITION_RANGES:
            if start_range <= account_number <= end_range:
                return (f'steam64id_mapping_u{universe}_'
                        f'{start_range}_{end_range}')

        return f'steam64id_mapping_u{universe}_2000000001_2147483647'

    @staticmethod
    async def create_tables(db_manager: DatabaseManager) -> None:
        """
        Create all necessary database tables.

        Args:
            db_manager: Database manager instance
        """

        async with db_manager.pool.acquire() as conn:
            # Create partitioned tables for each universe
            for universe in range(4):
                for start_range, end_range in TableManager.PARTITION_RANGES:
                    table_name = (f'steam64id_mapping_u{universe}_'
                                  f'{start_range}_{end_range}')

                    await conn.execute(f'''
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            steam64id BIGINT PRIMARY KEY,
                            battleye_guid VARCHAR(32) NOT NULL UNIQUE
                        )
                    ''')

                    await conn.execute(f'''
                        CREATE INDEX IF NOT EXISTS
                        idx_battleye_guid_u{universe}_{start_range}_{end_range}
                        ON {table_name}(battleye_guid)
                    ''')

            # Create cache table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS guid_cache (
                    battleye_guid VARCHAR(32) PRIMARY KEY,
                    steam64id BIGINT NOT NULL
                )
            ''')

        logger.info('Database tables created successfully')


__all__ = (
    'DatabaseManager',
    'TableManager',
)
