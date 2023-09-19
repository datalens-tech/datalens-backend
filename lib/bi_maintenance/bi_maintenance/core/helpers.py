"""
Common cases:

from bi_maintenance.core.helpers import get_migration_entry, dump_entry_data
entry = get_migration_entry('fweavr4awr332c')
dump_entry_data(entry)
"""


import json

from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager


try:
    from bi_maintenance.api.common import MaintenanceEnvironmentManager  # type: ignore  # TODO: fix
except ImportError:
    # Most likely will lead to errors
    from bi_maintenance.core.common import MaintenanceEnvironmentManagerBase as MaintenanceEnvironmentManager

from dl_core.us_dataset import Dataset


def get_migration_entry(entry_id: str) -> USMigrationEntry:
    usm = MaintenanceEnvironmentManager().get_usm_from_env(use_sr_factory=False, is_async_env=False)
    return usm.get_by_id(entry_id=entry_id, expected_type=USMigrationEntry)


def get_sync_usm(is_async_env: bool = True) -> SyncUSManager:
    return MaintenanceEnvironmentManager().get_usm_from_env(is_async_env=is_async_env)


def get_entry(entry_id: str, is_async_env: bool = True) -> USEntry:
    usm = MaintenanceEnvironmentManager().get_usm_from_env(is_async_env=is_async_env)
    return usm.get_by_id(entry_id=entry_id, expected_type=USMigrationEntry)


def get_dataset(entry_id: str, is_async_env: bool = True) -> Dataset:
    usm = MaintenanceEnvironmentManager().get_usm_from_env(is_async_env=is_async_env)
    return usm.get_by_id(entry_id=entry_id, expected_type=Dataset)


def dump_entry_data(entry: USMigrationEntry) -> str:
    return json.dumps(entry.data)
