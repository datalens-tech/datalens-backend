from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib.service_registry.field_id_generator_factory import FieldIdGeneratorFactory
from dl_api_lib.service_registry.formula_parser_factory import FormulaParserFactory
from dl_api_lib.service_registry.multi_query_mutator_factory import (
    DefaultSRMultiQueryMutatorFactory,
    SRMultiQueryMutatorFactory,
)
from dl_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager
from dl_api_lib.service_registry.typed_query_processor_factory import (
    DefaultQueryProcessorFactory,
    TypedQueryProcessorFactory,
)
from dl_constants.enums import QueryProcessingMode
from dl_core.services_registry.top_level import (
    DefaultServicesRegistry,
    ServicesRegistry,
)
from dl_core.utils import FutureRef
from dl_formula.parser.factory import ParserType
from dl_i18n.localizer_base import (
    Localizer,
    LocalizerFactory,
)
from dl_pivot.base.transformer_factory import PivotTransformerFactory
from dl_rls.subject_resolver import BaseSubjectResolver


if TYPE_CHECKING:
    from dl_api_lib.service_registry.dataset_validator_factory import DatasetValidatorFactory


@attr.s
class ApiServiceRegistry(ServicesRegistry, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_formula_parser_factory(self) -> FormulaParserFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def get_dataset_validator_factory(self) -> "DatasetValidatorFactory":
        raise NotImplementedError

    @abc.abstractmethod
    def get_field_id_generator_factory(self) -> FieldIdGeneratorFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def get_supported_functions_manager(self) -> SupportedFunctionsManager:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_subject_resolver(self) -> BaseSubjectResolver:
        raise NotImplementedError

    @abc.abstractmethod
    def get_localizer(self) -> Localizer:
        raise NotImplementedError

    @abc.abstractmethod
    def get_connector_availability(self) -> ConnectorAvailabilityConfig:
        raise NotImplementedError

    @abc.abstractmethod
    def get_multi_query_mutator_factory_factory(self) -> SRMultiQueryMutatorFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def get_pivot_transformer_factory(self) -> PivotTransformerFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def get_typed_query_processor_factory(self) -> TypedQueryProcessorFactory:
        raise NotImplementedError


@attr.s
class DefaultApiServiceRegistry(DefaultServicesRegistry, ApiServiceRegistry):  # noqa
    _default_formula_parser_type: Optional[ParserType] = attr.ib(kw_only=True, default=None)
    _formula_parser_factory: Optional[FormulaParserFactory] = attr.ib(kw_only=True)
    _dataset_validator_factory: Optional["DatasetValidatorFactory"] = attr.ib(kw_only=True)
    _field_id_generator_factory: Optional[FieldIdGeneratorFactory] = attr.ib(kw_only=True)
    _supported_functions_manager: Optional[SupportedFunctionsManager] = attr.ib(kw_only=True, default=None)
    _localizer_factory: Optional[LocalizerFactory] = attr.ib(kw_only=True, default=None)
    _localizer_fallback: Optional[Localizer] = attr.ib(kw_only=True, default=None)
    _connector_availability: Optional[ConnectorAvailabilityConfig] = attr.ib(kw_only=True, default=None)
    _query_proc_mode: QueryProcessingMode = attr.ib(kw_only=True, default=QueryProcessingMode.basic)
    _pivot_transformer_factory: Optional[PivotTransformerFactory] = attr.ib(kw_only=True, default=None)
    _typed_query_processor_factory: TypedQueryProcessorFactory = attr.ib(kw_only=True)

    @_formula_parser_factory.default  # noqa
    def _default_formula_parser_factory(self) -> FormulaParserFactory:
        return FormulaParserFactory(default_formula_parser_type=self._default_formula_parser_type)

    @_field_id_generator_factory.default  # noqa
    def _default_field_id_generator_factory(self) -> FieldIdGeneratorFactory:
        return FieldIdGeneratorFactory()

    @_typed_query_processor_factory.default  # noqa
    def _default_typed_query_processor_factory(self) -> TypedQueryProcessorFactory:
        return DefaultQueryProcessorFactory(service_registry_ref=FutureRef.fulfilled(self))

    def get_formula_parser_factory(self) -> FormulaParserFactory:
        assert self._formula_parser_factory is not None
        return self._formula_parser_factory

    def get_dataset_validator_factory(self) -> "DatasetValidatorFactory":
        assert self._dataset_validator_factory is not None
        return self._dataset_validator_factory

    def get_field_id_generator_factory(self) -> FieldIdGeneratorFactory:
        assert self._field_id_generator_factory is not None
        return self._field_id_generator_factory

    def get_supported_functions_manager(self) -> SupportedFunctionsManager:
        assert self._supported_functions_manager is not None
        return self._supported_functions_manager

    async def get_subject_resolver(self) -> BaseSubjectResolver:
        assert self._inst_specific_sr is not None
        return await self._inst_specific_sr.get_subject_resolver()

    def get_localizer(self) -> Localizer:
        assert self._localizer_factory is not None
        return self._localizer_factory.get_for_locale(
            self.rci.locale or "unknown",
            fallback=self._localizer_fallback,
        )

    def get_connector_availability(self) -> ConnectorAvailabilityConfig:
        assert self._connector_availability is not None
        return self._connector_availability

    def get_multi_query_mutator_factory_factory(self) -> SRMultiQueryMutatorFactory:
        return DefaultSRMultiQueryMutatorFactory(query_proc_mode=self._query_proc_mode)

    def get_pivot_transformer_factory(self) -> PivotTransformerFactory:
        assert self._pivot_transformer_factory is not None
        return self._pivot_transformer_factory

    def get_typed_query_processor_factory(self) -> TypedQueryProcessorFactory:
        return self._typed_query_processor_factory

    def close(self) -> None:
        if self._formula_parser_factory is not None:
            self._formula_parser_factory.close()

        super().close()

    async def close_async(self) -> None:
        if self._formula_parser_factory is not None:
            self._formula_parser_factory.close()

        await super().close_async()
