import attr

from dl_core.data_source_spec import (
    DataSourceCollectionSpec,
    DataSourceSpec,
)
from dl_core.data_source_spec_mutator.type_mapping import get_data_source_spec_mutator_class
from dl_core.us_dataset import Dataset


@attr.s(frozen=True)
class DataSourceCollectionSpecMutator:
    _dataset: Dataset = attr.ib(kw_only=True)

    def _mutate_spec(self, spec: DataSourceSpec) -> DataSourceSpec:
        mutator_cls = get_data_source_spec_mutator_class(ds_type=spec.source_type)
        mutator = mutator_cls(dataset=self._dataset)
        return mutator.mutate(spec=spec)

    def mutate(self, spec: DataSourceCollectionSpec) -> DataSourceCollectionSpec:
        if spec.origin is not None:
            new_origin = self._mutate_spec(spec=spec.origin)
            if new_origin != spec.origin:
                spec = attr.evolve(spec, origin=new_origin)

        return spec


__all__ = [
    "DataSourceCollectionSpecMutator",
]
