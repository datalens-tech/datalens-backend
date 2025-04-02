from dl_constants.enums import DataSourceType
from dl_core.data_source_spec_mutator.base import DataSourceSpecMutator


_REGISTRY: dict[DataSourceType, type[DataSourceSpecMutator]] = {}


def get_data_source_spec_mutator_class(ds_type: DataSourceType) -> type[DataSourceSpecMutator]:
    return _REGISTRY[ds_type]


def register_data_source_spec_mutator_class(
    source_type: DataSourceType,
    mutator_cls: type[DataSourceSpecMutator],
) -> None:
    _REGISTRY[source_type] = mutator_cls


__all__ = [
    "get_data_source_spec_mutator_class",
    "register_data_source_spec_mutator_class",
]
