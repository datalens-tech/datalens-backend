from collections.abc import (
    Callable,
    Iterator,
)
import copy
from typing import Any

import attr
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_core.services_registry.compute_executor import ComputeExecutorTPE
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistry
from dl_core.services_registry.top_level import DummyServiceRegistry
from dl_core.us_manager.schema_migration.dataset import DatasetSchemaMigration
from dl_core.us_manager.schema_migration.migrations.resolve_group_slugs import (
    migrate_resolve_group_slugs_up,
    migrate_resolve_group_slugs_up_async,
)
from dl_core.utils import FutureRef
from dl_rls.exc import RLSError
from dl_rls.models import (
    RLS_FAILED_USER_NAME_PREFIX,
    RLSSubject,
)
from dl_rls.subject_resolver import BaseSubjectResolver

_UNSET: Any = object()


@attr.s
class _StubInstSR(InstallationSpecificServiceRegistry):
    """Test stub: returns a configured subject resolver (or raises on acquisition)."""

    _subject_resolver: BaseSubjectResolver | None = attr.ib(default=None)
    _raise_on_get_resolver: type[BaseException] | None = attr.ib(default=None)

    async def get_subject_resolver(self) -> BaseSubjectResolver:
        if self._raise_on_get_resolver is not None:
            raise self._raise_on_get_resolver()
        assert self._subject_resolver is not None
        return self._subject_resolver


@attr.s
class _StubSubjectResolver(BaseSubjectResolver):
    """Mapping-based subject resolver. ``None`` values trigger the notfound path.
    A configured exception is raised by ``resolve_group_slug`` on every call.
    """

    mapping: dict[str, str | None] = attr.ib(factory=dict)
    raise_on_resolve: BaseException | None = attr.ib(default=None)
    resolved_calls: list[str] = attr.ib(factory=list)

    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        raise NotImplementedError

    async def get_groups_by_subject(
        self,
        rci: RequestContextInfo,
        by_id: bool = False,
    ) -> list[str]:
        return []

    def resolve_group_slug(self, group_slug: str, rci: RequestContextInfo) -> str | None:
        self.resolved_calls.append(group_slug)
        if self.raise_on_resolve is not None:
            raise self.raise_on_resolve
        return self.mapping[group_slug]


def _make_dict_item(
    subject_type: str = "group",
    subject_id: str = "my-group",
    subject_name: str = "@group:my-group",
    field_guid: str = "field-1",
    allowed_value: str | None = "v1",
    pattern_type: str = "value",
) -> dict[str, Any]:
    return {
        "field_guid": field_guid,
        "allowed_value": allowed_value,
        "subject": {
            "subject_type": subject_type,
            "subject_id": subject_id,
            "subject_name": subject_name,
        },
        "pattern_type": pattern_type,
    }


def _make_entry(items: list[dict] | None, schema_version: str = "1") -> dict[str, Any]:
    return {
        "type": "dataset",
        "scope": "dataset",
        "data": {
            "schema_version": schema_version,
            "rls": items,
        },
    }


def _make_sr(
    resolver: BaseSubjectResolver | None = None,
    *,
    raise_on_get_resolver: type[BaseException] | None = None,
    inst_specific_sr: Any = _UNSET,
) -> DummyServiceRegistry:
    """Build a ``DummyServiceRegistry`` for migration tests.

    By default attaches a ``_StubInstSR`` wired to ``resolver``. Pass
    ``inst_specific_sr=None`` to test the no-inst-SR branch, or pass an explicit
    instance to override.
    """
    if inst_specific_sr is _UNSET:
        inst_specific_sr = _StubInstSR(
            service_registry_ref=FutureRef(),
            subject_resolver=resolver,
            raise_on_get_resolver=raise_on_get_resolver,
        )
    sr = DummyServiceRegistry(
        rci=RequestContextInfo.create_empty(),
        inst_specific_sr=inst_specific_sr,
    )
    if isinstance(inst_specific_sr, _StubInstSR):
        inst_specific_sr._service_registry_ref.fulfill(sr)
    return sr


@pytest.fixture
def patch_dummy_compute_executor(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """``DummyServiceRegistry.get_compute_executor`` raises by default, but the async
    migration offloads the blocking sync resolver to it. Wire a real thread-pool
    executor for the async tests that reach resolution."""
    executor = ComputeExecutorTPE()
    monkeypatch.setattr(DummyServiceRegistry, "get_compute_executor", lambda self: executor)
    try:
        yield
    finally:
        executor.close()


# ---------------------------------------------------------------------------
# Sync branch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "items",
    [
        pytest.param(None, id="no_rls_subtree"),
        pytest.param([], id="empty_items"),
        pytest.param(
            [_make_dict_item(subject_type="user", subject_id="u1", subject_name="u1@example.com")],
            id="only_user_entries",
        ),
        pytest.param(
            [_make_dict_item(subject_type="group", subject_id="real-id", subject_name="@group:g1")],
            id="group_already_resolved",
        ),
    ],
)
def test_up_sync_noop_when_no_slug_resolution_needed(items: list[dict] | None) -> None:
    entry = _make_entry(items)
    original = copy.deepcopy(entry)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:never": "x"}))

    migrate_resolve_group_slugs_up(entry, services_registry=sr)

    assert entry == original


@pytest.mark.parametrize(
    "sr_factory, expected_exc",
    [
        pytest.param(lambda: None, RLSError, id="services_registry_is_none"),
        pytest.param(
            lambda: _make_sr(None, inst_specific_sr=None),
            RLSError,
            id="inst_specific_sr_is_none",
        ),
        pytest.param(
            lambda: _make_sr(None, raise_on_get_resolver=NotImplementedError),
            NotImplementedError,
            id="get_subject_resolver_not_implemented",
        ),
    ],
)
def test_up_sync_raises_when_resolver_acquisition_fails(
    sr_factory: Callable[[], DummyServiceRegistry | None],
    expected_exc: type[BaseException],
) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)

    with pytest.raises(expected_exc):
        migrate_resolve_group_slugs_up(entry, services_registry=sr_factory())


def test_up_sync_resolvable_slug_writes_real_id() -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:g1": "real-id-1"}))

    migrate_resolve_group_slugs_up(entry, services_registry=sr)

    assert entry["data"]["rls"][0]["subject"] == {
        "subject_type": "group",
        "subject_id": "real-id-1",
        "subject_name": "@group:g1",
    }


def test_up_sync_unresolvable_slug_marks_notfound() -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:g1": None}))

    migrate_resolve_group_slugs_up(entry, services_registry=sr)

    assert entry["data"]["rls"][0]["subject"] == {
        "subject_type": "notfound",
        "subject_id": "g1",
        "subject_name": RLS_FAILED_USER_NAME_PREFIX + "@group:g1",
    }


def test_up_sync_keeps_item_when_resolution_unsupported() -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    original = copy.deepcopy(entry)
    sr = _make_sr(_StubSubjectResolver(raise_on_resolve=NotImplementedError()))

    migrate_resolve_group_slugs_up(entry, services_registry=sr)

    assert entry == original


def test_up_sync_propagates_unexpected_resolver_error() -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(raise_on_resolve=RuntimeError("boom")))

    with pytest.raises(RuntimeError):
        migrate_resolve_group_slugs_up(entry, services_registry=sr)


def test_up_sync_propagates_rls_error() -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(raise_on_resolve=RLSError("nope")))

    with pytest.raises(RLSError):
        migrate_resolve_group_slugs_up(entry, services_registry=sr)


def test_up_sync_mixed_batch() -> None:
    items = [
        _make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1"),
        _make_dict_item(subject_type="group", subject_id="g2", subject_name="@group:g2"),
        _make_dict_item(subject_type="group", subject_id="real-id-c", subject_name="@group:g3"),
        _make_dict_item(subject_type="user", subject_id="u1", subject_name="u1@example.com"),
    ]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:g1": "real-id-1", "@group:g2": None}))

    migrate_resolve_group_slugs_up(entry, services_registry=sr)

    assert [item["subject"] for item in entry["data"]["rls"]] == [
        {"subject_type": "group", "subject_id": "real-id-1", "subject_name": "@group:g1"},
        {
            "subject_type": "notfound",
            "subject_id": "g2",
            "subject_name": RLS_FAILED_USER_NAME_PREFIX + "@group:g2",
        },
        {"subject_type": "group", "subject_id": "real-id-c", "subject_name": "@group:g3"},
        {"subject_type": "user", "subject_id": "u1", "subject_name": "u1@example.com"},
    ]


def test_up_sync_resolves_repeated_slug_once() -> None:
    items = [
        _make_dict_item(subject_id="dup", subject_name="@group:dup", field_guid="f1"),
        _make_dict_item(subject_id="dup", subject_name="@group:dup", field_guid="f2"),
        _make_dict_item(subject_id="dup", subject_name="@group:dup", field_guid="f3"),
    ]
    entry = _make_entry(items)
    resolver = _StubSubjectResolver(mapping={"@group:dup": "real-dup"})
    sr = _make_sr(resolver)

    migrate_resolve_group_slugs_up(entry, services_registry=sr)

    assert resolver.resolved_calls == ["@group:dup"]  # cached: resolved once for 3 items
    assert all(item["subject"]["subject_id"] == "real-dup" for item in entry["data"]["rls"])


# ---------------------------------------------------------------------------
# Async branch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "items",
    [
        pytest.param(None, id="no_rls_subtree"),
        pytest.param([], id="empty_items"),
        pytest.param(
            [_make_dict_item(subject_type="user", subject_id="u1", subject_name="u1@example.com")],
            id="only_user_entries",
        ),
        pytest.param(
            [_make_dict_item(subject_type="group", subject_id="real-id", subject_name="@group:g1")],
            id="group_already_resolved",
        ),
    ],
)
@pytest.mark.asyncio
async def test_up_async_noop_when_no_slug_resolution_needed(items: list[dict] | None) -> None:
    entry = _make_entry(items)
    original = copy.deepcopy(entry)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:never": "x"}))

    await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)

    assert entry == original


@pytest.mark.parametrize(
    "sr_factory, expected_exc",
    [
        pytest.param(lambda: None, RLSError, id="services_registry_is_none"),
        pytest.param(
            lambda: _make_sr(None, inst_specific_sr=None),
            RLSError,
            id="inst_specific_sr_is_none",
        ),
        pytest.param(
            lambda: _make_sr(None, raise_on_get_resolver=NotImplementedError),
            NotImplementedError,
            id="get_subject_resolver_not_implemented",
        ),
    ],
)
@pytest.mark.asyncio
async def test_up_async_raises_when_resolver_acquisition_fails(
    sr_factory: Callable[[], DummyServiceRegistry | None],
    expected_exc: type[BaseException],
) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)

    with pytest.raises(expected_exc):
        await migrate_resolve_group_slugs_up_async(entry, services_registry=sr_factory())


@pytest.mark.asyncio
async def test_up_async_resolvable_slug_writes_real_id(patch_dummy_compute_executor: None) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:g1": "real-id-async"}))

    await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)

    assert entry["data"]["rls"][0]["subject"]["subject_id"] == "real-id-async"


@pytest.mark.asyncio
async def test_up_async_unresolvable_slug_marks_notfound(patch_dummy_compute_executor: None) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:g1": None}))

    await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)

    assert entry["data"]["rls"][0]["subject"] == {
        "subject_type": "notfound",
        "subject_id": "g1",
        "subject_name": RLS_FAILED_USER_NAME_PREFIX + "@group:g1",
    }


@pytest.mark.asyncio
async def test_up_async_keeps_item_when_resolution_unsupported(patch_dummy_compute_executor: None) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    original = copy.deepcopy(entry)
    sr = _make_sr(_StubSubjectResolver(raise_on_resolve=NotImplementedError()))

    await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)

    assert entry == original


@pytest.mark.asyncio
async def test_up_async_propagates_unexpected_resolver_error(patch_dummy_compute_executor: None) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(raise_on_resolve=RuntimeError("boom")))

    with pytest.raises(RuntimeError):
        await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)


@pytest.mark.asyncio
async def test_up_async_propagates_rls_error(patch_dummy_compute_executor: None) -> None:
    items = [_make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1")]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(raise_on_resolve=RLSError("nope")))

    with pytest.raises(RLSError):
        await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)


@pytest.mark.asyncio
async def test_up_async_mixed_batch(patch_dummy_compute_executor: None) -> None:
    items = [
        _make_dict_item(subject_type="group", subject_id="g1", subject_name="@group:g1"),
        _make_dict_item(subject_type="group", subject_id="g2", subject_name="@group:g2"),
        _make_dict_item(subject_type="group", subject_id="real-id-c", subject_name="@group:g3"),
        _make_dict_item(subject_type="user", subject_id="u1", subject_name="u1@example.com"),
    ]
    entry = _make_entry(items)
    sr = _make_sr(_StubSubjectResolver(mapping={"@group:g1": "real-id-1", "@group:g2": None}))

    await migrate_resolve_group_slugs_up_async(entry, services_registry=sr)

    assert [item["subject"] for item in entry["data"]["rls"]] == [
        {"subject_type": "group", "subject_id": "real-id-1", "subject_name": "@group:g1"},
        {
            "subject_type": "notfound",
            "subject_id": "g2",
            "subject_name": RLS_FAILED_USER_NAME_PREFIX + "@group:g2",
        },
        {"subject_type": "group", "subject_id": "real-id-c", "subject_name": "@group:g3"},
        {"subject_type": "user", "subject_id": "u1", "subject_name": "u1@example.com"},
    ]


def test_production_migration_is_downgrade_only_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_RLS_MIGRATION", "1")
    declared = DatasetSchemaMigration().migrations

    assert len(declared) == 1
    assert declared[0].downgrade_only is True
    assert declared[0].name == "Resolve RLS group slugs to real subject ids"
