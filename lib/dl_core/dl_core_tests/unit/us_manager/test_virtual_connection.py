from typing import (
    ClassVar,
    Self,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

import attr
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)
from dl_core.base_models import DefaultConnectionRef
from dl_core.data_source.sql import StandardSQLDataSource
from dl_core.data_source.type_mapping import _DSRC_TYPES
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.data_source_spec.collection import DataSourceCollectionSpec
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_connection import CONNECTION_TYPES
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager
import dl_retrier


class FakeConnBase(ConnectionBase):
    """
    Empty connection type
    """

    @attr.s(kw_only=True)
    class DataModel(ConnectionBase.DataModel):
        pass

    @property
    def cache_ttl_sec_override(self) -> None:
        return None

    def get_conn_dto(self):  # type: ignore[override]
        raise NotImplementedError

    @classmethod
    def sync_create_virtual(cls, connection_id: str, us_manager: USManagerBase) -> Self:
        return cls(
            uuid=connection_id,
            data=cls.DataModel(),
            type_=ConnectionType.unknown.value,
            us_manager=us_manager,
            data_strict=False,
        )

    @classmethod
    async def async_create_virtual(cls, connection_id: str, us_manager: USManagerBase) -> Self:
        return cls(
            uuid=connection_id,
            data=cls.DataModel(),
            type_=ConnectionType.unknown.value,
            us_manager=us_manager,
            data_strict=False,
        )


class FakeVirtualConn(FakeConnBase):
    """
    Connection that acts like a virtual
    """

    is_virtual = True


class FakeNonVirtualConn(FakeConnBase):
    """
    Connection that acts like a non-virtual
    """

    is_virtual = False


def register_conn_type(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    cls: type[ConnectionBase],
) -> ConnectionType:
    if not ConnectionType.is_declared(name):
        ConnectionType.declare(name)

    conn_type = ConnectionType(name)
    monkeypatch.setitem(CONNECTION_TYPES, conn_type, cls)

    return conn_type


@pytest.fixture
def virtual_conn_type(monkeypatch: pytest.MonkeyPatch) -> ConnectionType:
    # Wrap with MagicMock to record calls
    monkeypatch.setattr(FakeVirtualConn, "sync_create_virtual", MagicMock(wraps=FakeVirtualConn.sync_create_virtual))
    monkeypatch.setattr(FakeVirtualConn, "async_create_virtual", AsyncMock(wraps=FakeVirtualConn.async_create_virtual))

    return register_conn_type(monkeypatch, "test_virtual_conn_type", FakeVirtualConn)


@pytest.fixture
def non_virtual_conn_type(monkeypatch: pytest.MonkeyPatch) -> ConnectionType:
    # Wrap with MagicMock to record calls
    monkeypatch.setattr(
        FakeNonVirtualConn, "sync_create_virtual", MagicMock(wraps=FakeNonVirtualConn.sync_create_virtual)
    )
    monkeypatch.setattr(
        FakeNonVirtualConn, "async_create_virtual", AsyncMock(wraps=FakeNonVirtualConn.async_create_virtual)
    )

    return register_conn_type(monkeypatch, "test_non_virtual_conn_type", FakeNonVirtualConn)


def make_get_by_id_mock(manager: USManagerBase, mock_cls: type) -> MagicMock:
    # Wrap get_by_id with MagicMock to record calls
    return mock_cls(
        side_effect=lambda entry_id, *_a, **_kw: FakeNonVirtualConn(
            us_manager=manager,
            type_=ConnectionType.unknown.value,
        )
    )


@pytest.fixture
def sync_us_manager(
    bi_context: RequestContextInfo,
    crypto_keys_config,
    default_service_registry,
) -> MockedSyncUSManager:
    manager = MockedSyncUSManager(
        bi_context=bi_context,
        crypto_keys_config=crypto_keys_config,
        services_registry=default_service_registry,
    )

    # Emulate US request with call recorder
    manager.get_by_id = make_get_by_id_mock(manager, MagicMock)

    return manager


@pytest.fixture
def async_us_manager(
    bi_context: RequestContextInfo,
    crypto_keys_config,
    default_service_registry,
) -> AsyncUSManager:
    manager = AsyncUSManager(
        bi_context=bi_context,
        crypto_keys_config=crypto_keys_config,
        us_auth_context=USAuthContextMaster(us_master_token="FakeKey"),
        us_base_url="http://localhost:100500",
        us_api_prefix="dummy",
        ca_data=b"",
        services_registry=default_service_registry,
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
    )

    # Emulate US request with call recorder
    manager.get_by_id = make_get_by_id_mock(manager, AsyncMock)

    return manager


def test_sync_virtual_uses_create_virtual_and_skips_us(
    sync_us_manager: MockedSyncUSManager,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-vc-1")

    sync_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=virtual_conn_type,
        referrer=None,
    )

    # Virtual constructor used
    FakeVirtualConn.sync_create_virtual.assert_called_once()
    assert FakeVirtualConn.sync_create_virtual.call_args.kwargs["connection_id"] == "conn-vc-1"

    # US not used
    sync_us_manager.get_by_id.assert_not_called()

    # Left in cache
    assert conn_ref in sync_us_manager._loaded_entries


def test_sync_virtual_is_cached(
    sync_us_manager: MockedSyncUSManager,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-vc-cache")

    sync_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=virtual_conn_type,
        referrer=None,
    )
    sync_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=virtual_conn_type,
        referrer=None,
    )

    # Double load should call virtual constructor only once
    FakeVirtualConn.sync_create_virtual.assert_called_once()
    assert FakeVirtualConn.sync_create_virtual.call_args.kwargs["connection_id"] == "conn-vc-cache"

    # US not used
    sync_us_manager.get_by_id.assert_not_called()


def test_sync_without_connection_type_falls_through_to_us(
    sync_us_manager: MockedSyncUSManager,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-no-type")

    sync_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        referrer=None,
    )

    # Virtual constructor not used
    FakeVirtualConn.sync_create_virtual.assert_not_called()

    # US used
    sync_us_manager.get_by_id.assert_called_once()
    assert sync_us_manager.get_by_id.call_args.args[0] == "conn-no-type"


def test_sync_non_virtual_falls_through_to_us(
    sync_us_manager: MockedSyncUSManager,
    non_virtual_conn_type: ConnectionType,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-non-virtual")

    sync_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=non_virtual_conn_type,
        referrer=None,
    )

    # Virtual constructor not used because connection is not virtual
    FakeNonVirtualConn.sync_create_virtual.assert_not_called()

    # US used
    sync_us_manager.get_by_id.assert_called_once()
    assert sync_us_manager.get_by_id.call_args.args[0] == "conn-non-virtual"


@pytest.mark.asyncio
async def test_async_virtual_uses_create_virtual_and_skips_us(
    async_us_manager: AsyncUSManager,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-vc-async-1")

    await async_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=virtual_conn_type,
        referrer=None,
    )

    # Virtual constructor used
    FakeVirtualConn.async_create_virtual.assert_awaited_once()
    assert FakeVirtualConn.async_create_virtual.call_args.kwargs["connection_id"] == "conn-vc-async-1"

    # US not used
    async_us_manager.get_by_id.assert_not_awaited()

    # Left in cache
    assert conn_ref in async_us_manager._loaded_entries


@pytest.mark.asyncio
async def test_async_virtual_is_cached(
    async_us_manager: AsyncUSManager,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-vc-async-cache")

    await async_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=virtual_conn_type,
        referrer=None,
    )
    await async_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=virtual_conn_type,
        referrer=None,
    )

    # Double load should call virtual constructor only once
    FakeVirtualConn.async_create_virtual.assert_awaited_once()
    assert FakeVirtualConn.async_create_virtual.call_args.kwargs["connection_id"] == "conn-vc-async-cache"

    # US not used
    async_us_manager.get_by_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_without_connection_type_falls_through_to_us(
    async_us_manager: AsyncUSManager,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-async-no-type")

    await async_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        referrer=None,
    )

    # Virtual constructor not used
    FakeVirtualConn.async_create_virtual.assert_not_awaited()

    # US used
    async_us_manager.get_by_id.assert_awaited_once()
    assert async_us_manager.get_by_id.call_args.args[0] == "conn-async-no-type"


@pytest.mark.asyncio
async def test_async_non_virtual_falls_through_to_us(
    async_us_manager: AsyncUSManager,
    non_virtual_conn_type: ConnectionType,
    virtual_conn_type: ConnectionType,
) -> None:
    conn_ref = DefaultConnectionRef(conn_id="conn-async-non-virtual")

    await async_us_manager.ensure_connection_preloaded(
        conn_ref=conn_ref,
        connection_type=non_virtual_conn_type,
        referrer=None,
    )

    # Virtual constructor not used because connection is not virtual
    FakeNonVirtualConn.async_create_virtual.assert_not_awaited()

    # US used
    async_us_manager.get_by_id.assert_awaited_once()
    assert async_us_manager.get_by_id.call_args.args[0] == "conn-async-non-virtual"


@pytest.fixture
def virtual_source_type(
    virtual_conn_type: ConnectionType,
    monkeypatch: pytest.MonkeyPatch,
) -> DataSourceType:
    """
    Create a virtual datasource type class and register it. Returns registered type.
    """

    type_name = "test_virtual_source_type"
    if not DataSourceType.is_declared(type_name):
        DataSourceType.declare(type_name)
    source_type = DataSourceType(type_name)

    # Minimal datasource
    class FakeVirtualDataSource(StandardSQLDataSource):
        conn_type: ClassVar[ConnectionType] = virtual_conn_type

    # Register this datasource
    monkeypatch.setitem(_DSRC_TYPES, source_type, FakeVirtualDataSource)

    return source_type


def make_dataset_with_source(
    us_manager: USManagerBase,
    conn_id: str,
    source_type: DataSourceType,
) -> Dataset:
    """
    Create a dataset with a virtual source type
    """

    data = Dataset.DataModel(
        name="test-ds",
        source_collections=[
            DataSourceCollectionSpec(
                id="src-1",
                origin=DataSourceSpec(
                    source_type=source_type,
                    connection_ref=DefaultConnectionRef(conn_id=conn_id),
                ),
            )
        ],
    )

    return Dataset(
        uuid="ds-1",
        data=data,
        type_=Dataset.scope,
        us_manager=us_manager,
        data_strict=False,
    )


def test_sync_load_dataset_dependencies_respects_virtual_source(
    sync_us_manager: MockedSyncUSManager,
    virtual_conn_type: ConnectionType,
    virtual_source_type: DataSourceType,
) -> None:
    """
    Test that loading dependencies with ``respect_sources=True`` will use
    dataset's and virtual connections are loaded using ``create_virtual`` method.
    """

    conn_id = "conn-vc-ds"
    dataset = make_dataset_with_source(sync_us_manager, conn_id, virtual_source_type)

    sync_us_manager.load_dataset_dependencies(dataset, respect_sources=True)

    # Virtual connection is created using create_virtual
    FakeVirtualConn.sync_create_virtual.assert_called_once()
    assert FakeVirtualConn.sync_create_virtual.call_args.kwargs["connection_id"] == conn_id

    # US not used
    sync_us_manager.get_by_id.assert_not_called()

    # Virtual connection exists in cache
    assert DefaultConnectionRef(conn_id=conn_id) in sync_us_manager._loaded_entries


def test_sync_load_dataset_dependencies_without_respect_sources_falls_through_to_us(
    sync_us_manager: MockedSyncUSManager,
    virtual_conn_type: ConnectionType,
    virtual_source_type: DataSourceType,
) -> None:
    """
    Test that loading dependencies with ``respect_sources=False`` will use only
    ```USEntry.links```. Virtual connections won't be loaded using
    ``create_virtual``.
    """

    conn_id = "conn-vc-ds-no-respect"
    dataset = make_dataset_with_source(sync_us_manager, conn_id, virtual_source_type)

    sync_us_manager.load_dataset_dependencies(dataset, respect_sources=False)

    # Virtual connection's create_virtual not used
    FakeVirtualConn.sync_create_virtual.assert_not_called()

    # US used
    sync_us_manager.get_by_id.assert_called_once()

    # Broken link is loaded in cache
    assert sync_us_manager.get_by_id.call_args.args[0] == conn_id
