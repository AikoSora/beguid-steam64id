"""
Progress management module.

This module handles progress tracking and persistence for the generation process.
"""

import asyncio
import json
import logging

from pathlib import Path


logger = logging.getLogger(__name__)


class ProgressManager:
    """
    Manages progress tracking and persistence.
    """

    def __init__(self, progress_file: str = 'search_progress.json'):
        self.progress_file = Path(progress_file)

    async def save_progress(self, last_processed: int) -> None:
        """
        Save progress to file.

        Args:
            last_processed: Last processed account number
        """

        progress_data = {
            'last_processed': last_processed,
            'timestamp': asyncio.get_event_loop().time()
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
                return data.get('last_processed', 0)
        except Exception as e:
            logger.warning(f'Failed to load progress: {e}')

        return 0


__all__ = (
    'ProgressManager',
)
