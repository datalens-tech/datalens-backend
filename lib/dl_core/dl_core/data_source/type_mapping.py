from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Collection,
    Type,
)


if TYPE_CHECKING:
    from dl_constants.enums import CreateDSFrom
    from dl_core.data_source.base import DataSource


_DSRC_TYPES: dict[CreateDSFrom, Type[DataSource]] = {}


def list_registered_source_types() -> Collection[CreateDSFrom]:
    return set(_DSRC_TYPES)


def get_data_source_class(ds_type: CreateDSFrom) -> Type[DataSource]:
    """Return ``DataSource`` subclass to be used for given dataset type."""
    return _DSRC_TYPES[ds_type]


def register_data_source_class(source_type: CreateDSFrom, source_cls: Type[DataSource]) -> None:
    """Register ``DataSource`` subclass in the mapping."""
    _DSRC_TYPES[source_type] = source_cls
