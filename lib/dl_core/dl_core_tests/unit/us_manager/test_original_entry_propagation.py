from __future__ import annotations

from typing import Optional
from unittest.mock import Mock

import attr
from marshmallow import fields as ma_fields
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.base_models import (
    BaseAttrsDataModel,
    EntryLocation,
    PathEntryLocation,
)
from dl_core.lifecycle.base import (
    EntryLifecycleManager,
    PostSaveHookResult,
)
from dl_core.lifecycle.factory_base import EntryLifecycleManagerFactoryBase
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_entry import USEntry
from dl_core.us_manager.storage_schemas.base import DefaultStorageSchema
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager


class TestUSEntry(USEntry):
    @attr.s
    class DataModel(BaseAttrsDataModel):
        test_field: str | None = attr.ib(default=None)


class TestUSEntryStorageSchema(DefaultStorageSchema):
    TARGET_CLS = TestUSEntry.DataModel

    test_field = ma_fields.String(allow_none=True)


class TestLifecycleManager(EntryLifecycleManager[TestUSEntry]):
    ENTRY_CLS = TestUSEntry

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pre_save_hook_mock = Mock()
        self.post_save_hook_mock = Mock()

    def pre_save_hook(self) -> None:
        super().pre_save_hook()

        # Record call to pre_save_hook
        self.pre_save_hook_mock(
            entry=self.entry,
            original_entry=self.original_entry,
        )

    def post_save_hook(self) -> PostSaveHookResult:
        result = super().post_save_hook()

        # Record call to post_save_hook
        self.post_save_hook_mock(
            entry=self.entry,
            original_entry=self.original_entry,
        )

        return result


class TestLifecycleManagerFactory(EntryLifecycleManagerFactoryBase):
    def __init__(self):
        self.created_managers: list[TestLifecycleManager] = []

    def get_lifecycle_manager(
        self,
        entry: USEntry,
        us_manager: USManagerBase,
        service_registry: ServicesRegistry,
        original_entry: USEntry | None = None,
    ) -> EntryLifecycleManager:
        manager = TestLifecycleManager(
            us_manager=us_manager,
            entry=entry,
            service_registry=service_registry,
            original_entry=original_entry,
        )
        self.created_managers.append(manager)

        return manager


class TestMockedSyncUSManager(MockedSyncUSManager):
    @classmethod
    def _get_entry_class(
        cls,
        *,
        us_scope: str,
        us_type: str,
        entry_key: Optional[EntryLocation] = None,
    ) -> type[USEntry]:
        if us_scope == "test" and us_type == "test":
            return TestUSEntry

        return super()._get_entry_class(us_scope=us_scope, us_type=us_type, entry_key=entry_key)


@pytest.fixture(scope="function")
def test_lifecycle_factory() -> TestLifecycleManagerFactory:
    return TestLifecycleManagerFactory()


@pytest.fixture(scope="function")
def test_us_manager(
    bi_context: RequestContextInfo,
    crypto_keys_config: CryptoKeysConfig,
    default_service_registry: ServicesRegistry,
    test_lifecycle_factory: TestLifecycleManagerFactory,
) -> USManagerBase:
    us_manager = TestMockedSyncUSManager(
        bi_context=bi_context,
        crypto_keys_config=crypto_keys_config,
        services_registry=default_service_registry,
        lifecycle_manager_factory=test_lifecycle_factory,
    )

    us_manager._MAP_TYPE_TO_SCHEMA[TestUSEntry.DataModel] = TestUSEntryStorageSchema

    return us_manager


@pytest.fixture(scope="function")
def entry(
    test_us_manager: USManagerBase,
    test_lifecycle_factory: TestLifecycleManagerFactory,
) -> TestUSEntry:
    entry = TestUSEntry(
        data={
            "test_field": "hehe",
        },
        entry_key=PathEntryLocation(path="/test/test_entry"),
        us_manager=test_us_manager,
        data_strict=False,
    )
    entry.scope = "test"
    entry.type_ = "test"

    try:
        test_us_manager.save(entry)

        # Clean this list for test
        test_lifecycle_factory.created_managers = []

        yield entry
    finally:
        test_us_manager.delete(entry)


def test_us_manager_entry_clone(
    test_us_manager: USManagerBase,
    entry: TestUSEntry,
) -> None:
    # Clone entry
    original_entry = test_us_manager.clone_entry_instance(entry)

    # Modify something
    entry.data.test_field = "not hehe"

    # Check that original left intact
    assert entry.data.test_field == "not hehe"
    assert original_entry.data.test_field == "hehe"


def test_original_entry_with_clone_in_save(
    test_us_manager: USManagerBase,
    test_lifecycle_factory: TestLifecycleManagerFactory,
    entry: TestUSEntry,
) -> None:
    # Clone original entry before changes
    original_entry = test_us_manager.clone_entry_instance(entry)

    # Modify something
    entry.data.test_field = "not hehe"

    # Save entry
    test_us_manager.save(
        entry=entry,
        original_entry=original_entry,
    )

    # First was created when entry was created, but then this list is being cleared
    assert len(test_lifecycle_factory.created_managers) == 1
    lifecycle_manager = test_lifecycle_factory.created_managers[0]

    # Verify original_entry was propagated
    assert lifecycle_manager._original_entry is original_entry

    # Verify hooks were called with proper values
    lifecycle_manager.pre_save_hook_mock.assert_called_once_with(
        entry=entry,
        original_entry=original_entry,
    )
    lifecycle_manager.post_save_hook_mock.assert_called_once_with(
        entry=entry,
        original_entry=original_entry,
    )
