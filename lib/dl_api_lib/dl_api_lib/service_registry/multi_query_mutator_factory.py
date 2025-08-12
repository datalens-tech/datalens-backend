import logging
from typing import (
    Sequence,
)

import attr

from dl_api_lib.query.registry import get_multi_query_mutator_factory_class
from dl_constants.enums import (
    QueryProcessingMode,
    SourceBackendType,
)
from dl_core.us_dataset import Dataset
from dl_formula.core.dialect import DialectCombo
from dl_query_processing.multi_query.factory import MultiQueryMutatorFactoryBase
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase


LOGGER = logging.getLogger(__name__)


@attr.s
class SRMultiQueryMutatorFactory:
    _query_proc_mode: QueryProcessingMode = attr.ib(kw_only=True)
    _mqm_factory_cls_cache: dict[tuple[SourceBackendType, DialectCombo], type[MultiQueryMutatorFactoryBase]] = attr.ib(
        init=False, factory=dict
    )

    def get_mqm_factory_cls(
        self,
        backend_type: SourceBackendType,
        dialect: DialectCombo,
    ) -> type[MultiQueryMutatorFactoryBase]:
        # Try to get for the specified query mode
        factory_cls = get_multi_query_mutator_factory_class(
            query_proc_mode=self._query_proc_mode,
            backend_type=backend_type,
            dialect=dialect,
        )
        LOGGER.info(
            f"Resolved MQM factory for backend_type {backend_type.name} "
            f"and dialect {dialect.common_name_and_version} "
            f"in {self._query_proc_mode.name} mode "
            f"to {factory_cls.__name__}"
        )
        return factory_cls

    def get_multi_query_mutators(
        self,
        backend_type: SourceBackendType,
        dialect: DialectCombo,
        dataset: Dataset,
    ) -> Sequence[MultiQueryMutatorBase]:
        cache_key = (backend_type, dialect)
        if (mqm_factory_cls := self._mqm_factory_cls_cache.get(cache_key)) is None:
            mqm_factory_cls = self.get_mqm_factory_cls(backend_type=backend_type, dialect=dialect)
            self._mqm_factory_cls_cache[cache_key] = mqm_factory_cls
        mqm_factory = mqm_factory_cls(result_schema=dataset.result_schema)
        return mqm_factory.get_mutators()
