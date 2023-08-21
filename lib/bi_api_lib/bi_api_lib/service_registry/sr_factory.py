from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

import attr

from bi_i18n.localizer_base import LocalizerFactory, Localizer
from bi_core.services_registry.sr_factories import DefaultSRFactory
from bi_core.services_registry.top_level import ServicesRegistry
from bi_core.components.ids import FieldIdGeneratorType
from bi_core.utils import FutureRef
from bi_formula.parser.factory import ParserType

from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.service_registry.dataset_validator_factory import DefaultDatasetValidatorFactory
from bi_api_lib.service_registry.dls_client_factory import DLSClientFactory
from bi_api_lib.service_registry.field_id_generator_factory import FieldIdGeneratorFactory
from bi_api_lib.service_registry.service_registry import DefaultBiApiServiceRegistry
from bi_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager


if TYPE_CHECKING:
    from bi_api_commons.base_models import RequestContextInfo


@attr.s
class DefaultBiApiSRFactory(DefaultSRFactory[DefaultBiApiServiceRegistry]):
    service_registry_cls = DefaultBiApiServiceRegistry
    _supported_functions_manager: Optional[SupportedFunctionsManager] = attr.ib(kw_only=True, default=None)
    _default_formula_parser_type: Optional[ParserType] = attr.ib(kw_only=True, default=None)
    _field_id_generator_type: FieldIdGeneratorType = attr.ib(kw_only=True, default=FieldIdGeneratorType.readable)
    _dls_client_factory: Optional[DLSClientFactory] = attr.ib(kw_only=True, default=None)
    _use_iam_subject_resolver: bool = attr.ib(default=False)
    _localizer_factory: Optional[LocalizerFactory] = attr.ib(default=None)
    _localizer_fallback: Optional[Localizer] = attr.ib(default=None)
    _connector_availability: Optional[ConnectorAvailabilityConfig] = attr.ib(default=None)

    def additional_sr_constructor_kwargs(
            self, request_context_info: RequestContextInfo, sr_ref: FutureRef[ServicesRegistry],
    ) -> Dict[str, Any]:
        return dict(
            default_formula_parser_type=self._default_formula_parser_type,
            dataset_validator_factory=DefaultDatasetValidatorFactory(
                is_bleeding_edge_user=self.is_bleeding_edge_user(request_context_info),
            ),
            field_id_generator_factory=FieldIdGeneratorFactory(
                field_id_generator_type=self._field_id_generator_type,
            ),
            supported_functions_manager=self._supported_functions_manager,
            dls_client=self._dls_client_factory.get_client(request_context_info) if self._dls_client_factory is not None else None,
            use_iam_subject_resolver=self._use_iam_subject_resolver,
            localizer_factory=self._localizer_factory,
            localizer_fallback=self._localizer_fallback,
            connector_availability=self._connector_availability,
        )
