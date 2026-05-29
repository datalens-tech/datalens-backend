from collections.abc import Callable

import pytest

from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)
from dl_core.data_source import type_mapping
from dl_core.data_source.type_mapping import get_connection_type_for_source_type


def test_none_source_type_returns_none() -> None:
    assert get_connection_type_for_source_type(None) is None


def test_unregistered_source_type_returns_none() -> None:
    source_type = DataSourceType.declare("unregistered_source")

    assert get_connection_type_for_source_type(source_type) is None


@pytest.fixture
def source_registrator(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[DataSourceType, type], None]:
    def register(source_type: DataSourceType, source_cls: type) -> None:
        monkeypatch.setitem(type_mapping._DSRC_TYPES, source_type, source_cls)

    return register


def test_source_cls_without_conn_type_returns_none(
    source_registrator: Callable[[DataSourceType, type], None],
) -> None:
    if not DataSourceType.is_declared("source_no_conn_type"):
        DataSourceType.declare("source_no_conn_type")
    source_type = DataSourceType("source_no_conn_type")

    class NoConnTypeSource:
        pass

    source_registrator(source_type, NoConnTypeSource)

    assert get_connection_type_for_source_type(source_type) is None


def test_source_cls_with_conn_type_returns_conn_type(
    source_registrator: Callable[[DataSourceType, type], None],
) -> None:
    if not DataSourceType.is_declared("source_with_conn_type"):
        DataSourceType.declare("source_with_conn_type")
    source_type = DataSourceType("source_with_conn_type")

    if not ConnectionType.is_declared("connection_with_conn_type"):
        ConnectionType.declare("connection_with_conn_type")
    connection_type = ConnectionType("connection_with_conn_type")

    class SourceWithConnType:
        conn_type = connection_type

    source_registrator(source_type, SourceWithConnType)

    assert get_connection_type_for_source_type(source_type) == connection_type
