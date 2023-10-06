import abc
from typing import Sequence

import attr

from dl_api_lib.query.registry import get_multi_query_mutator_factory
from dl_constants.enums import (
    QueryProcessingMode,
    SourceBackendType,
)
from dl_core.us_dataset import Dataset
from dl_formula.core.dialect import DialectCombo
from dl_query_processing.multi_query.factory import MultiQueryMutatorFactoryBase
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase


class SRMultiQueryMutatorFactory(abc.ABC):
    @abc.abstractmethod
    def get_mqm_factory(
        self,
        backend_type: SourceBackendType,
        dialect: DialectCombo,
        dataset: Dataset,
    ) -> MultiQueryMutatorFactoryBase:
        raise NotImplementedError

    def get_multi_query_mutators(
        self,
        backend_type: SourceBackendType,
        dialect: DialectCombo,
        dataset: Dataset,
    ) -> Sequence[MultiQueryMutatorBase]:
        mqm_factory = self.get_mqm_factory(backend_type=backend_type, dialect=dialect, dataset=dataset)
        return mqm_factory.get_mutators()


@attr.s
class DefaultSRMultiQueryMutatorFactory(SRMultiQueryMutatorFactory):
    _query_proc_mode: QueryProcessingMode = attr.ib(kw_only=True)

    def get_mqm_factory(
        self,
        backend_type: SourceBackendType,
        dialect: DialectCombo,
        dataset: Dataset,
    ) -> MultiQueryMutatorFactoryBase:
        # Try to get for the specified query mode
        factory = get_multi_query_mutator_factory(
            query_proc_mode=self._query_proc_mode,
            backend_type=backend_type,
            dialect=dialect,
            result_schema=dataset.result_schema,
        )
        if factory is None:
            # Try again for the basic mode
            factory = get_multi_query_mutator_factory(
                query_proc_mode=QueryProcessingMode.basic,
                backend_type=backend_type,
                dialect=dialect,
                result_schema=dataset.result_schema,
            )

        assert factory is not None
        return factory
