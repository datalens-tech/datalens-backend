from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import attr
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_constants import (
    MigrationStatus,
    RLSPatternType,
    RLSSubjectType,
)
from dl_core.fields import (
    BIField,
    DirectCalculationSpec,
)
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistry
from dl_core.services_registry.top_level import DefaultServicesRegistry
from dl_core.us_dataset import Dataset
from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase
from dl_core.us_manager.schema_migration.migrations.resolve_group_slugs import (
    migrate_resolve_group_slugs_down,
    migrate_resolve_group_slugs_down_async,
    migrate_resolve_group_slugs_up,
    migrate_resolve_group_slugs_up_async,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core.utils import FutureRef
from dl_core_tests.db.base import DefaultCoreTestClass
from dl_rls.models import (
    RLSEntry,
    RLSSubject,
)
from dl_rls.subject_resolver import BaseSubjectResolver

if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


@attr.s
class _StubSubjectResolver(BaseSubjectResolver):
    """Resolver stub: returns a real id for known slugs from a static mapping."""

    mapping: dict[str, str | None] = attr.ib(factory=dict)

    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        raise NotImplementedError

    async def get_groups_by_subject(
        self,
        rci: RequestContextInfo,
        by_id: bool = False,
    ) -> list[str]:
        return []

    def resolve_group_slug(
        self,
        group_slug: str,
        rci: RequestContextInfo,
    ) -> str | None:
        return self.mapping.get(group_slug)


@attr.s
class _StubInstSR(InstallationSpecificServiceRegistry):
    """Installation-specific SR stub: returns a preconfigured subject resolver."""

    _subject_resolver: BaseSubjectResolver = attr.ib()

    async def get_subject_resolver(self) -> BaseSubjectResolver:
        return self._subject_resolver


@attr.s
class _ActiveDatasetSchemaMigration(BaseEntrySchemaMigration):
    """Test variant of the production migration with ``downgrade_only=False`` so
    the up branch actually runs through the framework on read.
    """

    @property
    def migrations(self) -> list[Migration]:
        return [
            Migration(
                version=datetime(2026, 5, 16, 12, 0, 0),
                name="Resolve RLS group slugs to real subject ids",
                up_function=migrate_resolve_group_slugs_up,
                down_function=migrate_resolve_group_slugs_down,
                await_up_function=migrate_resolve_group_slugs_up_async,
                await_down_function=migrate_resolve_group_slugs_down_async,
                downgrade_only=False,
            ),
        ]


class _ActiveDatasetSchemaMigrationFactory(EntrySchemaMigrationFactoryBase):
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
        service_registry: ServicesRegistry | None = None,
    ) -> BaseEntrySchemaMigration:
        return _ActiveDatasetSchemaMigration(services_registry=service_registry)


def _install_subject_resolver(
    services_registry: ServicesRegistry,
    stub_resolver: _StubSubjectResolver,
) -> None:
    """Attach a stub ``InstallationSpecificServiceRegistry`` to the SR.

    The production migration reads the installation-specific SR via
    ``get_installation_specific_service_registry_opt()``, which returns
    ``services_registry._inst_specific_sr``. The default test SR has
    ``_inst_specific_sr=None``, so swapping the field in-place gives the
    migration the surface it needs without any monkeypatching of class methods.
    """
    assert isinstance(
        services_registry, DefaultServicesRegistry
    ), f"Expected DefaultServicesRegistry, got {type(services_registry).__name__}"
    inst_sr = _StubInstSR(
        service_registry_ref=FutureRef(),
        subject_resolver=stub_resolver,
    )
    inst_sr._service_registry_ref.fulfill(services_registry)
    services_registry._inst_specific_sr = inst_sr


class TestDatasetRlsMigration(DefaultCoreTestClass):
    """Single sync round-trip that proves the migration factory is wired into
    the US manager and that the dataset lifecycle preserves the resolved RLS
    payload through save/reload.

    Sync/async parity for the up function itself lives in the unit-level
    ``test_dataset_rls_migration.py``; framework-level downgrade semantics live
    in ``test_migration.py``.
    """

    @pytest.fixture
    def sync_us_migration_manager(
        self,
        conn_default_sync_us_manager: SyncUSManager,
    ) -> SyncUSManager:
        manager = conn_default_sync_us_manager.clone(
            schema_migration_factory=_ActiveDatasetSchemaMigrationFactory(),
        )
        stub_resolver = _StubSubjectResolver(mapping={"@group:my-group": "real-id-42"})
        _install_subject_resolver(manager.get_services_registry(), stub_resolver)
        return manager

    def test_read_path_resolves_slug(
        self,
        sync_us_migration_manager: SyncUSManager,
        saved_dataset: Dataset,
    ) -> None:
        dataset = saved_dataset
        field_guid = "rls-test-field"
        # Add a field so the matching RLS row survives ``_dump_rls`` in the
        # lifecycle pre-save hook.
        dataset.result_schema.fields.append(
            BIField.make(
                guid=field_guid,
                title=field_guid,
                calc_spec=DirectCalculationSpec(source=field_guid, avatar_id=None),
            )
        )
        dataset.rls.items.append(
            RLSEntry(
                field_guid=field_guid,
                allowed_value="some-value",
                subject=RLSSubject(
                    subject_type=RLSSubjectType.group,
                    subject_id="my-group",
                    subject_name="@group:my-group",
                ),
                pattern_type=RLSPatternType.value,
            )
        )
        sync_us_migration_manager.save(dataset)

        migrated = sync_us_migration_manager.get_by_id(entry_id=dataset.uuid)

        group_items = [item for item in migrated.rls.items if item.subject.subject_type == RLSSubjectType.group]
        assert len(group_items) == 1
        assert group_items[0].subject.subject_id == "real-id-42"
        assert group_items[0].subject.subject_name == "@group:my-group"
        assert migrated.migration_status == MigrationStatus.migrated_up
