"""
Main Steam64ID Converter Module.

This module provides the main converter class that orchestrates all operations.
"""

import asyncio
import concurrent.futures
import logging

from typing import Dict, List, Optional, Tuple

from .config import DatabaseConfig, GenerationConfig
from .database import DatabaseManager, TableManager
from .generator import Steam64IDGenerator
from .progress import ProgressManager


logger = logging.getLogger(__name__)


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
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=8
        )
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

        return self.generator.generate_batch(
            universe, account_type, start_account, end_account
        )

    async def _insert_batch_data_with_retry(
        self,
        table_name: str,
        batch_data: List[Tuple[int, str]],
        max_retries: int = 3,
    ) -> None:
        """
        Insert batch data with connection retry logic.

        Args:
            table_name: Table name
            batch_data: Batch data to insert
            max_retries: Maximum retry attempts
        """

        if not batch_data:
            return

        for attempt in range(max_retries):
            try:
                async with self.db_manager.pool.acquire() as conn:
                    await conn.executemany(f'''
                        INSERT INTO {table_name}
                        (steam64id, battleye_guid)
                        VALUES ($1, $2)
                        ON CONFLICT (steam64id) DO NOTHING
                    ''', batch_data)

                    logger.info(f'Inserted {len(batch_data)} records '
                                f'into {table_name}')
                    return  # Success, exit retry loop

            except Exception as e:
                error_msg = str(e).lower()

                # Critical errors that should stop the process
                critical_errors = [
                    'no space left on device',
                    'disk full',
                    'could not extend file',
                    'insufficient disk space',
                    'out of disk space',
                    'disk quota exceeded'
                ]

                if any(critical_error in error_msg for critical_error in critical_errors):
                    logger.critical(f'CRITICAL DATABASE ERROR: {e}')
                    logger.critical('Stopping generation due to disk space '
                                    'issues!')
                    raise RuntimeError(f'Critical database error: {e}') from e

                # Connection errors - retry
                connection_errors = [
                    'connection was closed',
                    'connection is closed',
                    'connection lost',
                    'connection timeout',
                    'connection refused',
                    'server closed the connection'
                ]

                if any(conn_error in error_msg for conn_error in connection_errors):
                    if attempt < max_retries - 1:
                        logger.warning(f'Connection error on attempt '
                                       f'{attempt + 1}/{max_retries}: {e}')
                        logger.info('Retrying in 5 seconds...')
                        await asyncio.sleep(5)
                        continue
                    else:
                        logger.error(f'Failed to insert after {max_retries} '
                                     f'attempts: {e}')
                        raise

                # Other errors - log and raise
                logger.error(f'Failed to insert data into {table_name}: {e}')
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
            timeout_minutes = max(5, chunk_size // 100_000)

            logger.info(f'Universe {universe}: processing chunk '
                        f'{chunk_start}-{chunk_end} with {timeout_minutes}min '
                        f'timeout')

            # Generate data in thread
            future = self.thread_pool.submit(
                self._generate_batch_threaded,
                universe, account_type, chunk_start, chunk_end
            )

            batch_data = future.result(timeout=timeout_minutes * 60)
            logger.info(f'Generated {len(batch_data)} records for universe '
                        f'{universe}, chunk {chunk_start}-{chunk_end}')

            # Group data by table
            table_data: Dict[str, List[Tuple[int, str]]] = {}
            for steam64id, battleye_guid in batch_data:
                account_number = (steam64id >> 1) & 0x7FFFFFFF
                table_name = TableManager.get_table_name(
                    account_number, universe
                )

                if table_name not in table_data:
                    table_data[table_name] = []

                table_data[table_name].append((steam64id, battleye_guid))

            # Insert data into tables
            for table_name, data in table_data.items():
                await self._insert_batch_data_with_retry(table_name, data)

        except concurrent.futures.TimeoutError:
            logger.error(f'Timeout processing chunk {chunk_start}-{chunk_end} '
                         f'for universe {universe}')
            # Retry with smaller chunk
            await self._retry_with_smaller_chunk(
                universe, chunk_start, chunk_end, account_type
            )
        except Exception as e:
            logger.error(f'Error processing chunk {chunk_start}-{chunk_end} '
                         f'for universe {universe}: {e}')

    async def _retry_with_smaller_chunk(
        self,
        universe: int,
        chunk_start: int,
        chunk_end: int,
        account_type: int,
    ) -> None:
        """
        Retry processing with smaller chunk size and connection retry.

        Args:
            universe: Steam universe
            chunk_start: Starting account number
            chunk_end: Ending account number
            account_type: Account type
        """

        chunk_size = chunk_end - chunk_start + 1
        smaller_chunk_size = chunk_size // 4  # Reduce chunk size by 4

        logger.info(f'Retrying universe {universe} with smaller chunks: '
                    f'{smaller_chunk_size}')

        for sub_chunk_start in range(chunk_start, chunk_end + 1, smaller_chunk_size):
            sub_chunk_end = min(sub_chunk_start + smaller_chunk_size - 1, chunk_end)

            try:
                # Generate data in thread with shorter timeout
                future = self.thread_pool.submit(
                    self._generate_batch_threaded,
                    universe, account_type, sub_chunk_start, sub_chunk_end
                )

                batch_data = future.result(timeout=300)  # 5 minutes timeout
                logger.info(f'Generated {len(batch_data)} records for '
                            f'sub-chunk {sub_chunk_start}-{sub_chunk_end}')

                # Group and insert data with retry
                table_data: Dict[str, List[Tuple[int, str]]] = {}
                for steam64id, battleye_guid in batch_data:
                    account_number = (steam64id >> 1) & 0x7FFFFFFF
                    table_name = TableManager.get_table_name(
                        account_number, universe
                    )

                    if table_name not in table_data:
                        table_data[table_name] = []

                    table_data[table_name].append((steam64id, battleye_guid))

                for table_name, data in table_data.items():
                    await self._insert_batch_data_with_retry(table_name, data)

            except concurrent.futures.TimeoutError:
                logger.error(f'Timeout on sub-chunk {sub_chunk_start}-'
                             f'{sub_chunk_end}, skipping')
                continue
            except RuntimeError as e:
                # Critical error - re-raise to stop the entire process
                logger.critical(f'CRITICAL ERROR in sub-chunk '
                                f'{sub_chunk_start}-{sub_chunk_end}: {e}')
                raise
            except Exception as e:
                logger.error(f'Error on sub-chunk {sub_chunk_start}-'
                             f'{sub_chunk_end}: {e}')
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

        logger.info(f'Processing universe {universe}')

        # Calculate chunk size - smaller for better reliability
        chunk_size = min((end_account - start_account + 1) // 8, config.max_chunk_size // 2)
        chunk_size = max(chunk_size, config.batch_size)

        logger.info(f'Universe {universe}: chunk size = {chunk_size}')

        # Create chunk tasks
        chunk_tasks = []
        for chunk_start in range(start_account, end_account + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_account)
            task = self._process_universe_chunk(
                universe, chunk_start, chunk_end, config.account_type
            )
            chunk_tasks.append(task)

        # Process chunks
        await asyncio.gather(*chunk_tasks, return_exceptions=True)
        logger.info(f'Universe {universe} processing completed')

    async def populate_database(self, config: GenerationConfig) -> None:
        """
        Populate database with Steam64ID mappings.

        Args:
            config: Generation configuration
        """

        logger.info(f'Starting database population: {config.start_account} '
                    f'to {config.end_account}')

        # Process in chunks to handle large ranges efficiently
        current_start = config.start_account
        chunk_size = config.max_range_size

        while current_start <= config.end_account:
            chunk_end = min(current_start + chunk_size - 1, config.end_account)

            logger.info(f'Processing chunk: {current_start} to {chunk_end}')

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

            logger.info(f'Completed chunk: {current_start} to {chunk_end}')

        logger.info('Database population completed')

    async def _process_chunk(self, config: GenerationConfig) -> None:
        """
        Process a single chunk of data.

        Args:
            config: Generation configuration
        """

        # Process each universe
        universe_tasks = []
        for universe in range(4):
            task = self._populate_universe(
                universe, config.start_account, config.end_account, config
            )
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
                    table_name = (f'steam64id_mapping_u{universe}_'
                                  f'{start_range}_{end_range}')

                    row = await conn.fetchrow(f'''
                        SELECT steam64id FROM {table_name}
                        WHERE battleye_guid = $1
                    ''', battleye_guid)

                    if row:
                        steam64id = row['steam64id']
                        await self._add_to_cache(battleye_guid, steam64id)
                        return steam64id
                    print(f'No steam64id found for guid {battleye_guid} '
                          f'in table {table_name}')

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
            row = await conn.fetchrow('''
                SELECT steam64id FROM guid_cache
                WHERE battleye_guid = $1
            ''', battleye_guid)

            return row['steam64id'] if row else None

    async def _add_to_cache(self, battleye_guid: str, steam64id: int) -> None:
        """
        Add Steam64ID to cache.

        Args:
            battleye_guid: BattlEye GUID
            steam64id: Steam64ID
        """

        async with self.db_manager.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO guid_cache (battleye_guid, steam64id)
                VALUES ($1, $2)
                ON CONFLICT (battleye_guid) DO NOTHING
            ''', battleye_guid, steam64id)

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

        logger.info(f'Starting generation from {start_account} '
                    f'to {config.end_account}')
        logger.info(f'Total records to process: '
                    f'{config.end_account - start_account + 1:,}')

        # Populate database
        await self.populate_database(config)


__all__ = (
    'Steam64IDConverter',
)
