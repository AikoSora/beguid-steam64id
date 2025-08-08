"""
Steam64ID Generator Module.

This module handles Steam64ID generation according to Valve specifications
and conversion to BattlEye GUID format.
"""

from hashlib import md5
from random import randint

from typing import List, Tuple


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

        return md5(b'BE' + temp).hexdigest()

    def generate_batch(
        self,
        universe: int,
        account_type: int,
        start_account: int,
        end_account: int,
    ) -> List[Tuple[int, str]]:
        """
        Generate Steam64ID batch.

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
            y_bit = randint(0, 1)
            steam64id = self.generate_steam64id(
                universe,
                account_type,
                account_number,
                y_bit,
            )
            battleye_guid = self.convert_to_battleye_guid(steam64id)
            batch_data.append((steam64id, battleye_guid))

        return batch_data


__all__ = (
    'Steam64IDGenerator',
)
