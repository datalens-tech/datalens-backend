import abc
import logging
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


LOGGER = logging.getLogger(__name__)


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
        LOGGER.info(
            f"Resolved MQM factory for backend_type {backend_type.name} "
            f"and dialect {dialect.common_name_and_version} "
            f"in {self._query_proc_mode.name} mode "
            f"to {type(factory).__name__}"
        )
        return factory
