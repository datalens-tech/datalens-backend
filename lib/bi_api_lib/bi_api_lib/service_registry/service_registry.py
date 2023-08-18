from __future__ import annotations

import abc
from typing import Optional, TYPE_CHECKING

import attr

from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.service_registry.field_id_generator_factory import FieldIdGeneratorFactory
from bi_api_lib.service_registry.formula_parser_factory import FormulaParserFactory
from bi_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager
from bi_core.i18n.localizer_base import LocalizerFactory, Localizer
from bi_api_lib.utils.rls import BaseSubjectResolver
from bi_core.services_registry.top_level import DefaultServicesRegistry, ServicesRegistry
from bi_dls_client.dls_client import DLSClient
from bi_dls_client.subject_resolver import DLSSubjectResolver
from bi_formula.parser.factory import ParserType

if TYPE_CHECKING:
    from bi_api_lib.service_registry.dataset_validator_factory import DatasetValidatorFactory


@attr.s
class BiApiServiceRegistry(ServicesRegistry, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_formula_parser_factory(self) -> FormulaParserFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def get_dataset_validator_factory(self) -> 'DatasetValidatorFactory':
        raise NotImplementedError

    @abc.abstractmethod
    def get_field_id_generator_factory(self) -> FieldIdGeneratorFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def get_supported_functions_manager(self) -> SupportedFunctionsManager:
        raise NotImplementedError

    @abc.abstractmethod
    def get_dls_client(self) -> DLSClient:
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


@attr.s
class DefaultBiApiServiceRegistry(DefaultServicesRegistry, BiApiServiceRegistry):  # noqa
    _default_formula_parser_type: Optional[ParserType] = attr.ib(kw_only=True, default=None)
    _formula_parser_factory: Optional[FormulaParserFactory] = attr.ib(kw_only=True)
    _dataset_validator_factory: Optional['DatasetValidatorFactory'] = attr.ib(kw_only=True)
    _field_id_generator_factory: Optional[FieldIdGeneratorFactory] = attr.ib(kw_only=True)
    _supported_functions_manager: Optional[SupportedFunctionsManager] = attr.ib(kw_only=True, default=None)
    _dls_client: Optional[DLSClient] = attr.ib(kw_only=True, default=None)
    _use_iam_subject_resolver: bool = attr.ib(kw_only=True, default=False)
    _localizer_factory: Optional[LocalizerFactory] = attr.ib(kw_only=True, default=None)
    _localizer_fallback: Optional[Localizer] = attr.ib(kw_only=True, default=None)
    _connector_availability: Optional[ConnectorAvailabilityConfig] = attr.ib(kw_only=True, default=None)

    @_formula_parser_factory.default  # noqa
    def _default_formula_parser_factory(self) -> FormulaParserFactory:
        return FormulaParserFactory(default_formula_parser_type=self._default_formula_parser_type)

    @_field_id_generator_factory.default  # noqa
    def _default_field_id_generator_factory(self) -> FieldIdGeneratorFactory:
        return FieldIdGeneratorFactory()

    def get_formula_parser_factory(self) -> FormulaParserFactory:
        assert self._formula_parser_factory is not None
        return self._formula_parser_factory

    def get_dataset_validator_factory(self) -> 'DatasetValidatorFactory':
        assert self._dataset_validator_factory is not None
        return self._dataset_validator_factory

    def get_field_id_generator_factory(self) -> FieldIdGeneratorFactory:
        assert self._field_id_generator_factory is not None
        return self._field_id_generator_factory

    def get_supported_functions_manager(self) -> SupportedFunctionsManager:
        assert self._supported_functions_manager is not None
        return self._supported_functions_manager

    def get_dls_client(self) -> DLSClient:
        assert self._dls_client is not None
        return self._dls_client

    async def get_subject_resolver(self) -> BaseSubjectResolver:
        if self._use_iam_subject_resolver:
            assert self._inst_specific_sr is not None
            return await self._inst_specific_sr.get_subject_resolver()
        # TODO: remove after enabling IAM resolver on YC prod
        return DLSSubjectResolver(dls_client=self.get_dls_client())

    def get_localizer(self) -> Localizer:
        assert self._localizer_factory is not None
        return self._localizer_factory.get_for_locale(
            self.rci.locale or 'unknown',
            fallback=self._localizer_fallback,
        )

    def get_connector_availability(self) -> ConnectorAvailabilityConfig:
        assert self._connector_availability is not None
        return self._connector_availability

    def close(self) -> None:
        if self._formula_parser_factory is not None:
            self._formula_parser_factory.close()

        super().close()

    async def close_async(self) -> None:
        if self._formula_parser_factory is not None:
            self._formula_parser_factory.close()

        await super().close_async()
