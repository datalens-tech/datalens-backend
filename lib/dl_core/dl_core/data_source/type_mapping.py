from typing import Collection

from dl_constants.enums import DataSourceType
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
