from typing import Collection

from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)
from dl_core.data_source.base import DataSource


_DSRC_TYPES: dict[DataSourceType, type[DataSource]] = {}


def list_registered_source_types() -> Collection[DataSourceType]:
    return set(_DSRC_TYPES)


def get_data_source_class(ds_type: DataSourceType) -> type[DataSource]:
    """Return ``DataSource`` subclass to be used for given dataset type."""
    return _DSRC_TYPES[ds_type]


def register_data_source_class(source_type: DataSourceType, source_cls: type[DataSource]) -> None:
    """Register ``DataSource`` subclass in the mapping."""
    _DSRC_TYPES[source_type] = source_cls


def get_connection_type_for_source_type(source_type: DataSourceType | None) -> ConnectionType | None:
    """
    Return the connection type associated with the given data source type
    """

    if source_type is None:
        return None

    dsrc_cls = _DSRC_TYPES.get(source_type)
    if dsrc_cls is None:
        return None

    if hasattr(dsrc_cls, "conn_type"):
        return dsrc_cls.conn_type

    return None
