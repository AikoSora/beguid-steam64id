"""
Main application entry point for Steam64ID to BattlEye GUID Converter.

This module provides the main function to run the converter application.
"""

import asyncio
import logging

from src import DatabaseConfig, Steam64IDConverter

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


async def main():
    """Main application entry point."""
    # Database configuration
    db_config = DatabaseConfig(
        host='127.0.0.1',  # Use IP instead of localhost
        port=5432,
        database='steam64id_db',
        username='postgres',
        password='password'
    )

    # Run converter
    async with Steam64IDConverter(db_config) as converter:
        # Find steam64id by guid
        # await converter.find_steam64id_by_guid('6d67b58640e2f223c22101615936a32c')

        # Start generation if you don't have a database yet
        await converter.start_generation()


if __name__ == '__main__':
    asyncio.run(main())
