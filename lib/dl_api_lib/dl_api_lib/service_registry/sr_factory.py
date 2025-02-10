from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import attr

from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib.service_registry.dataset_validator_factory import DefaultDatasetValidatorFactory
from dl_api_lib.service_registry.field_id_generator_factory import FieldIdGeneratorFactory
from dl_api_lib.service_registry.service_registry import DefaultApiServiceRegistry
from dl_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager
from dl_constants.enums import QueryProcessingMode
from dl_core.components.ids import FieldIdGeneratorType
from dl_core.services_registry.sr_factories import DefaultSRFactory
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.utils import FutureRef
from dl_formula.parser.factory import ParserType
from dl_i18n.localizer_base import (
    Localizer,
    LocalizerFactory,
)
from dl_pivot.base.transformer_factory import PivotTransformerFactory


if TYPE_CHECKING:
    from dl_api_commons.base_models import RequestContextInfo


@attr.s
class DefaultApiSRFactory(DefaultSRFactory[DefaultApiServiceRegistry]):
    service_registry_cls = DefaultApiServiceRegistry
    _supported_functions_manager: Optional[SupportedFunctionsManager] = attr.ib(kw_only=True, default=None)
    _default_formula_parser_type: Optional[ParserType] = attr.ib(kw_only=True, default=None)
    _field_id_generator_type: FieldIdGeneratorType = attr.ib(kw_only=True, default=FieldIdGeneratorType.readable)
    _localizer_factory: Optional[LocalizerFactory] = attr.ib(default=None)
    _localizer_fallback: Optional[Localizer] = attr.ib(default=None)
    _connector_availability: Optional[ConnectorAvailabilityConfig] = attr.ib(default=None)
    _query_proc_mode: QueryProcessingMode = attr.ib(kw_only=True, default=QueryProcessingMode.basic)
    _pivot_transformer_factory: Optional[PivotTransformerFactory] = attr.ib(kw_only=True, default=None)

    def additional_sr_constructor_kwargs(
        self,
        request_context_info: RequestContextInfo,
        sr_ref: FutureRef[ServicesRegistry],
    ) -> dict[str, Any]:
        return dict(
            default_formula_parser_type=self._default_formula_parser_type,
            dataset_validator_factory=DefaultDatasetValidatorFactory(),
            field_id_generator_factory=FieldIdGeneratorFactory(
                field_id_generator_type=self._field_id_generator_type,
            ),
            supported_functions_manager=self._supported_functions_manager,
            localizer_factory=self._localizer_factory,
            localizer_fallback=self._localizer_fallback,
            connector_availability=self._connector_availability,
            query_proc_mode=self._query_proc_mode,
            pivot_transformer_factory=self._pivot_transformer_factory,
        )
