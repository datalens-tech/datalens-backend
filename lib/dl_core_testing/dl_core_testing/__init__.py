"""
Helper functions and classes for usage in tests of all dependent projects
"""

import pathlib

import dl_testing


dl_testing.register_all_assert_rewrites(__name__, pathlib.Path(__file__).parent)

from .configuration import UnitedStorageConfiguration
from .environment import prepare_united_storage_from_config


__all__ = [
    "UnitedStorageConfiguration",
    "prepare_united_storage_from_config",
]
