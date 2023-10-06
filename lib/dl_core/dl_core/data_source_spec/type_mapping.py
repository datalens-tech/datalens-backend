from __future__ import annotations

from typing import Type

from dl_constants.enums import DataSourceType
from dl_core.data_source_spec.base import DataSourceSpec


_DSRC_SPEC_CLASSES: dict[DataSourceType, Type[DataSourceSpec]] = {}


def get_data_source_spec_class(ds_type: DataSourceType) -> Type[DataSourceSpec]:
    """Return ``DataSourceSpec`` subclass to be used for given dataset type."""
    return _DSRC_SPEC_CLASSES[ds_type]


def register_data_source_spec_class(source_type: DataSourceType, spec_cls: Type[DataSourceSpec]) -> None:
    """Register ``DataSourceSpec`` subclass in the mapping."""
    _DSRC_SPEC_CLASSES[source_type] = spec_cls
