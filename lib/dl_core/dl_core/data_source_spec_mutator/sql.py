import attr

from dl_core.data_source_spec import (
    DataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.data_source_spec_mutator.base import DataSourceSpecMutator


@attr.s(frozen=True)
class SubselectDataSourceSpecMutator(DataSourceSpecMutator):
    def mutate(self, spec: DataSourceSpec) -> DataSourceSpec:
        assert isinstance(spec, SubselectDataSourceSpec)

        if spec.subsql is None:
            return spec

        new_subsql = self._template_str(spec.subsql)
        if new_subsql != spec.subsql:
            spec = attr.evolve(spec, subsql=new_subsql)

        return spec


__all__ = [
    "SubselectDataSourceSpecMutator",
]
