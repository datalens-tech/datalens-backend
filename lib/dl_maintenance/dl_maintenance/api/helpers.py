"""
Common cases:

from dl_api_lib.app_settings import DataApiAppSettings
from dl_data_api.app_factory import StandaloneDataApiAppFactory

from dl_maintenance.api.common import MaintenanceEnvironmentManager
from dl_maintenance.api.helpers import get_migration_entry, dump_entry_data

mm = MaintenanceEnvironmentManager(app_settings_cls=DataApiAppSettings, app_factory_cls=StandaloneDataApiAppFactory)
entry = get_migration_entry(mm, 'fweavr4awr332c')
dump_entry_data(entry)

"""

import json

from dl_core.us_dataset import Dataset
from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_maintenance.api.common import MaintenanceEnvironmentManager


def get_migration_entry(m_manager: MaintenanceEnvironmentManager, entry_id: str) -> USMigrationEntry:
    usm = m_manager.get_usm_from_env(use_sr_factory=False, is_async_env=False)
    return usm.get_by_id(entry_id=entry_id, expected_type=USMigrationEntry)


def get_entry(m_manager: MaintenanceEnvironmentManager, entry_id: str, is_async_env: bool = True) -> USEntry:
    usm = m_manager.get_usm_from_env(is_async_env=is_async_env)
    return usm.get_by_id(entry_id=entry_id, expected_type=USMigrationEntry)


def get_dataset(m_manager: MaintenanceEnvironmentManager, entry_id: str, is_async_env: bool = True) -> Dataset:
    usm = m_manager.get_usm_from_env(is_async_env=is_async_env)
    return usm.get_by_id(entry_id=entry_id, expected_type=Dataset)


def dump_entry_data(entry: USMigrationEntry) -> str:
    return json.dumps(entry.data)
