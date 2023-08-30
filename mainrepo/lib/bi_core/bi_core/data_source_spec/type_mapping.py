from __future__ import annotations

from typing import Type

from bi_constants.enums import CreateDSFrom

from bi_core.data_source_spec.base import DataSourceSpec


_DSRC_SPEC_CLASSES: dict[CreateDSFrom, Type[DataSourceSpec]] = {}


def get_data_source_spec_class(ds_type: CreateDSFrom) -> Type[DataSourceSpec]:
    """Return ``DataSourceSpec`` subclass to be used for given dataset type."""
    return _DSRC_SPEC_CLASSES[ds_type]


def register_data_source_spec_class(source_type: CreateDSFrom, spec_cls: Type[DataSourceSpec]) -> None:
    """Register ``DataSourceSpec`` subclass in the mapping."""
    _DSRC_SPEC_CLASSES[source_type] = spec_cls
