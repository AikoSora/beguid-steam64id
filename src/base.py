"""
Steam64ID to BattlEye GUID Converter

This module provides functionality to generate Steam64ID mappings and convert
them to BattlEye GUID format for database storage and lookup operations.

Author: Mikhail Shevelkov
Version: 1.0.0
"""

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import asyncpg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('steam64id_converter.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    port: int
    database: str
    username: str
    password: str
    min_connections: int = 5
    max_connections: int = 20


@dataclass(frozen=True)
class GenerationConfig:
    """Configuration for Steam64ID generation."""
    start_account: int
    end_account: int
    batch_size: int
    universe: int
    account_type: int
    max_chunk_size: int = 1_000_000
    max_range_size: int = 10_000_000


class Steam64IDGenerator:
    """
    Handles Steam64ID generation according to Valve specifications.
    """

    @staticmethod
    def generate_steam64id(
        universe: int,
        account_type: int,
        account_number: int,
        y_bit: int,
    ) -> int:
        """
        Generate Steam64ID based on Valve's specification.

        Args:
            universe: Steam universe (0-3)
            account_type: Account type (1 for Individual)
            account_number: Account number
            y_bit: Authentication server bit (0 or 1)

        Returns:
            Generated Steam64ID
        """

        instance = 1  # Default instance for user accounts

        return (
            (universe << 56) |
            (account_type << 52) |
            (instance << 32) |
            (account_number << 1) |
            y_bit
        )

    @staticmethod
    def convert_to_battleye_guid(steam64id: int) -> str:
        """
        Convert Steam64ID to BattlEye GUID.

        Args:
            steam64id: Steam64ID to convert

        Returns:
            BattlEye GUID string
        """

        temp = b""
        steamid_copy = steam64id

        for _ in range(8):
            temp += bytes([steamid_copy & 0xFF])
            steamid_copy >>= 8

        return hashlib.md5(b"BE" + temp).hexdigest()


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
            logger.info("Database connection pool established")
        except Exception as e:
            logger.error(f"Failed to establish database connection: {e}")
            raise

    async def close(self) -> None:
        """
        Close database connection pool.
        """

        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def execute_query(self, query: str, *args) -> None:
        """
        Execute a database query.

        Args:
            query: SQL query to execute
            *args: Query parameters
        """

        if not self.pool:
            raise RuntimeError("Database not connected")

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
                return f"steam64id_mapping_u{universe}_{start_range}_{end_range}"

        return f"steam64id_mapping_u{universe}_2000000001_2147483647"

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
                    table_name = f"steam64id_mapping_u{universe}_{start_range}_{end_range}"

                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            steam64id BIGINT PRIMARY KEY,
                            battleye_guid VARCHAR(32) NOT NULL UNIQUE
                        )
                    """)

                    await conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_battleye_guid_u{universe}_{start_range}_{end_range}
                        ON {table_name}(battleye_guid)
                    """)

            # Create cache table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guid_cache (
                    battleye_guid VARCHAR(32) PRIMARY KEY,
                    steam64id BIGINT NOT NULL
                )
            """)

        logger.info("Database tables created successfully")


class ProgressManager:
    """
    Manages progress tracking and persistence.
    """

    def __init__(self, progress_file: str = "search_progress.json"):
        self.progress_file = Path(progress_file)

    async def save_progress(self, last_processed: int) -> None:
        """
        Save progress to file.

        Args:
            last_processed: Last processed account number
        """

        progress_data = {
            "last_processed": last_processed,
            "timestamp": asyncio.get_event_loop().time()
        }

        self.progress_file.write_text(json.dumps(progress_data, indent=2))

    async def load_progress(self) -> int:
        """
        Load progress from file.

        Returns:
            Last processed account number
        """

        try:
            if self.progress_file.exists():
                data = json.loads(self.progress_file.read_text())
                return data.get("last_processed", 0)
        except Exception as e:
            logger.warning(f"Failed to load progress: {e}")

        return 0


class Steam64IDConverter:
    """
    Main converter class for Steam64ID to BattlEye GUID operations.

    This class provides comprehensive functionality for generating, storing,
    and querying Steam64ID mappings with BattlEye GUID conversions.
    """

    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.db_manager = DatabaseManager(db_config)
        self.progress_manager = ProgressManager()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        self.generator = Steam64IDGenerator()

    async def __aenter__(self):
        """
        Async context manager entry.

        Returns:
            Self
        """

        await self.db_manager.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """

        await self.db_manager.close()
        self.thread_pool.shutdown(wait=True)

    def _generate_batch_threaded(
        self,
        universe: int,
        account_type: int,
        start_account: int,
        end_account: int,
    ) -> List[Tuple[int, str]]:
        """
        Generate Steam64ID batch in separate thread.

        Args:
            universe: Steam universe
            account_type: Account type
            start_account: Starting account number
            end_account: Ending account number

        Returns:
            List of (steam64id, battleye_guid) tuples
        """

        batch_data = []

        for account_number in range(start_account, end_account + 1):
            y_bit = random.randint(0, 1)
            steam64id = self.generator.generate_steam64id(
                universe,
                account_type,
                account_number,
                y_bit,
            )
            battleye_guid = self.generator.convert_to_battleye_guid(steam64id)
            batch_data.append((steam64id, battleye_guid))

        return batch_data

    async def _insert_batch_data(
        self,
        table_name: str,
        batch_data: List[Tuple[int, str]],
    ) -> None:
        """
        Insert batch data into specified table.

        Args:
            table_name: Table name
            batch_data: Batch data to insert
        """

        if not batch_data:
            return

        async with self.db_manager.pool.acquire() as conn:
            try:
                await conn.executemany(f"""
                    INSERT INTO {table_name}
                    (steam64id, battleye_guid)
                    VALUES ($1, $2)
                    ON CONFLICT (steam64id) DO NOTHING
                """, batch_data)

                logger.info(f"Inserted {len(batch_data)} records into {table_name}")

            except Exception as e:
                logger.error(f"Failed to insert data into {table_name}: {e}")
                raise

    async def _process_universe_chunk(
        self,
        universe: int,
        chunk_start: int,
        chunk_end: int,
        account_type: int,
    ) -> None:
        """
        Process a chunk of universe data.

        Args:
            universe: Steam universe
            chunk_start: Starting account number
            chunk_end: Ending account number
            account_type: Account type
        """

        try:
            # Calculate timeout based on chunk size
            chunk_size = chunk_end - chunk_start + 1
            timeout_minutes = max(5, chunk_size // 100_000)  # 1 minute per 100k records

            logger.info(f"Universe {universe}: processing chunk {chunk_start}-{chunk_end} with {timeout_minutes}min timeout")

            # Generate data in thread
            future = self.thread_pool.submit(
                self._generate_batch_threaded,
                universe, account_type, chunk_start, chunk_end
            )

            batch_data = future.result(timeout=timeout_minutes * 60)  # Convert to seconds
            logger.info(f"Generated {len(batch_data)} records for universe {universe}, chunk {chunk_start}-{chunk_end}")

            # Group data by table
            table_data: Dict[str, List[Tuple[int, str]]] = {}
            for steam64id, battleye_guid in batch_data:
                account_number = (steam64id >> 1) & 0x7FFFFFFF
                table_name = TableManager.get_table_name(account_number, universe)

                if table_name not in table_data:
                    table_data[table_name] = []

                table_data[table_name].append((steam64id, battleye_guid))

            # Insert data into tables
            for table_name, data in table_data.items():
                await self._insert_batch_data(table_name, data)

        except concurrent.futures.TimeoutError:
            logger.error(f"Timeout processing chunk {chunk_start}-{chunk_end} for universe {universe}")
            # Retry with smaller chunk
            await self._retry_with_smaller_chunk(universe, chunk_start, chunk_end, account_type)
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_start}-{chunk_end} for universe {universe}: {e}")

    async def _retry_with_smaller_chunk(
        self,
        universe: int,
        chunk_start: int,
        chunk_end: int,
        account_type: int,
    ) -> None:
        """
        Retry processing with smaller chunk size.

        Args:
            universe: Steam universe
            chunk_start: Starting account number
            chunk_end: Ending account number
            account_type: Account type
        """

        chunk_size = chunk_end - chunk_start + 1
        smaller_chunk_size = chunk_size // 4  # Reduce chunk size by 4

        logger.info(f"Retrying universe {universe} with smaller chunks: {smaller_chunk_size}")

        for sub_chunk_start in range(chunk_start, chunk_end + 1, smaller_chunk_size):
            sub_chunk_end = min(sub_chunk_start + smaller_chunk_size - 1, chunk_end)

            try:
                # Generate data in thread with shorter timeout
                future = self.thread_pool.submit(
                    self._generate_batch_threaded,
                    universe, account_type, sub_chunk_start, sub_chunk_end
                )

                batch_data = future.result(timeout=300)  # 5 minutes timeout
                logger.info(f"Generated {len(batch_data)} records for sub-chunk {sub_chunk_start}-{sub_chunk_end}")

                # Group and insert data
                table_data: Dict[str, List[Tuple[int, str]]] = {}
                for steam64id, battleye_guid in batch_data:
                    account_number = (steam64id >> 1) & 0x7FFFFFFF
                    table_name = TableManager.get_table_name(account_number, universe)

                    if table_name not in table_data:
                        table_data[table_name] = []

                    table_data[table_name].append((steam64id, battleye_guid))

                for table_name, data in table_data.items():
                    await self._insert_batch_data(table_name, data)

            except concurrent.futures.TimeoutError:
                logger.error(f"Timeout on sub-chunk {sub_chunk_start}-{sub_chunk_end}, skipping")
                continue
            except Exception as e:
                logger.error(f"Error on sub-chunk {sub_chunk_start}-{sub_chunk_end}: {e}")
                continue

    async def _populate_universe(
        self,
        universe: int,
        start_account: int,
        end_account: int,
        config: GenerationConfig,
    ) -> None:
        """
        Populate a single universe with data.

        Args:
            universe: Steam universe
            start_account: Starting account number
            end_account: Ending account number
            config: Generation configuration
        """

        logger.info(f"Processing universe {universe}")

        # Calculate chunk size - smaller for better reliability
        chunk_size = min((end_account - start_account + 1) // 8, config.max_chunk_size // 2)
        chunk_size = max(chunk_size, config.batch_size)

        logger.info(f"Universe {universe}: chunk size = {chunk_size}")

        # Create chunk tasks
        chunk_tasks = []
        for chunk_start in range(start_account, end_account + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_account)
            task = self._process_universe_chunk(universe, chunk_start, chunk_end, config.account_type)
            chunk_tasks.append(task)

        # Process chunks
        await asyncio.gather(*chunk_tasks, return_exceptions=True)
        logger.info(f"Universe {universe} processing completed")

    async def populate_database(self, config: GenerationConfig) -> None:
        """
        Populate database with Steam64ID mappings.

        Args:
            config: Generation configuration
        """

        logger.info(f"Starting database population: {config.start_account} to {config.end_account}")

        # Process in chunks to handle large ranges efficiently
        current_start = config.start_account
        chunk_size = config.max_range_size

        while current_start <= config.end_account:
            chunk_end = min(current_start + chunk_size - 1, config.end_account)

            logger.info(f"Processing chunk: {current_start} to {chunk_end}")

            # Create chunk config
            chunk_config = GenerationConfig(
                start_account=current_start,
                end_account=chunk_end,
                batch_size=config.batch_size,
                universe=config.universe,
                account_type=config.account_type,
                max_chunk_size=config.max_chunk_size,
                max_range_size=config.max_range_size
            )

            # Process chunk
            await self._process_chunk(chunk_config)

            # Save progress
            await self.progress_manager.save_progress(chunk_end)

            # Move to next chunk
            current_start = chunk_end + 1

            logger.info(f"Completed chunk: {current_start} to {chunk_end}")

        logger.info("Database population completed")

    async def _process_chunk(self, config: GenerationConfig) -> None:
        """Process a single chunk of data.

        Args:
            config: Generation configuration
        """

        # Process each universe
        universe_tasks = []
        for universe in range(4):
            task = self._populate_universe(universe, config.start_account, config.end_account, config)
            universe_tasks.append(task)

        await asyncio.gather(*universe_tasks)

    async def find_steam64id_by_guid(self, battleye_guid: str) -> Optional[int]:
        """
        Find Steam64ID by BattlEye GUID.

        Args:
            battleye_guid: BattlEye GUID to search for

        Returns:
            Steam64ID if found, None otherwise
        """

        # Check cache first
        cached_result = await self._get_from_cache(battleye_guid)
        if cached_result:
            return cached_result

        # Search in all partitions
        async with self.db_manager.pool.acquire() as conn:
            for universe in range(4):
                for start_range, end_range in TableManager.PARTITION_RANGES:
                    table_name = f"steam64id_mapping_u{universe}_{start_range}_{end_range}"

                    row = await conn.fetchrow(f"""
                        SELECT steam64id FROM {table_name} 
                        WHERE battleye_guid = $1
                    """, battleye_guid)

                    if row:
                        steam64id = row['steam64id']
                        await self._add_to_cache(battleye_guid, steam64id)
                        return steam64id

        return None

    async def _get_from_cache(self, battleye_guid: str) -> Optional[int]:
        """
        Get Steam64ID from cache.

        Args:
            battleye_guid: BattlEye GUID to search for

        Returns:
            Steam64ID if found, None otherwise
        """

        async with self.db_manager.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT steam64id FROM guid_cache 
                WHERE battleye_guid = $1
            """, battleye_guid)

            return row['steam64id'] if row else None

    async def _add_to_cache(self, battleye_guid: str, steam64id: int) -> None:
        """
        Add Steam64ID to cache.

        Args:
            battleye_guid: BattlEye GUID
            steam64id: Steam64ID
        """

        async with self.db_manager.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guid_cache (battleye_guid, steam64id)
                VALUES ($1, $2)
                ON CONFLICT (battleye_guid) DO NOTHING
            """, battleye_guid, steam64id)

    async def start_generation(self) -> None:
        """
        Start the Steam64ID generation process.

        Returns:
            None
        """

        # Create tables
        await TableManager.create_tables(self.db_manager)

        # Load progress
        last_processed = await self.progress_manager.load_progress()
        start_account = last_processed + 1 if last_processed > 0 else 1

        # Configure generation for full range with smaller chunks
        config = GenerationConfig(
            start_account=start_account,
            end_account=(1 << 31) - 1,  # Full range
            batch_size=25_000,  # Smaller batch size
            universe=1,
            account_type=1,
            max_chunk_size=500_000,  # Smaller chunks for reliability
            max_range_size=5_000_000  # Smaller ranges
        )

        logger.info(f"Starting generation from {start_account} to {config.end_account}")
        logger.info(f"Total records to process: {config.end_account - start_account + 1:,}")

        # Populate database
        await self.populate_database(config)


async def main():
    """
    Main application entry point.
    """

    # Database configuration
    db_config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="steam64id_db",
        username="postgres",
        password="password"
    )

    # Run converter
    async with Steam64IDConverter(db_config) as converter:
        await converter.start_generation()


if __name__ == "__main__":
    asyncio.run(main())
