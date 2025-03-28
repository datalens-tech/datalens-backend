from dl_constants.enums import DataSourceType
from dl_core.data_source_spec.base import DataSourceSpec


_DSRC_SPEC_CLASSES: dict[DataSourceType, type[DataSourceSpec]] = {}


def get_data_source_spec_class(ds_type: DataSourceType) -> type[DataSourceSpec]:
    """Return ``DataSourceSpec`` subclass to be used for given dataset type."""
    return _DSRC_SPEC_CLASSES[ds_type]


def register_data_source_spec_class(source_type: DataSourceType, spec_cls: type[DataSourceSpec]) -> None:
    """Register ``DataSourceSpec`` subclass in the mapping."""
    _DSRC_SPEC_CLASSES[source_type] = spec_cls
