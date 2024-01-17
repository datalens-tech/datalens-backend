import abc
from typing import (
    ClassVar,
    Optional,
    Tuple,
    Type,
)

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.source_base import (
    DataSourceBaseSchema,
    DataSourceTemplateBaseSchema,
)
from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_api_connector.form_config.models.base import ConnectionFormFactory
from dl_api_lib.query.registry import MQMFactorySettingItem
from dl_constants.enums import QueryProcessingMode
from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreSourceDefinition,
)
from dl_formula.core.dialect import (
    DialectCombo,
    DialectName,
)
from dl_i18n.localizer_base import TranslationConfig
from dl_query_processing.compilation.filter_compiler import (
    FilterFormulaCompiler,
    MainFilterFormulaCompiler,
)
from dl_query_processing.multi_query.factory import (
    DefaultMultiQueryMutatorFactory,
    NoCompengMultiQueryMutatorFactory,
)


class ApiSourceDefinition(abc.ABC):
    core_source_def_cls: ClassVar[Type[CoreSourceDefinition]]
    api_schema_cls: ClassVar[Type[DataSourceBaseSchema]] = DataSourceBaseSchema
    template_api_schema_cls: ClassVar[Type[DataSourceTemplateBaseSchema]] = DataSourceTemplateBaseSchema


class ApiConnectionDefinition(abc.ABC):
    core_conn_def_cls: ClassVar[Type[CoreConnectionDefinition]]
    api_generic_schema_cls: ClassVar[Type[ConnectionSchema]]
    alias: ClassVar[Optional[str]] = None  # TODO remove in favor of info provider
    info_provider_cls: ClassVar[Type[ConnectionInfoProvider]]
    form_factory_cls: ClassVar[Optional[Type[ConnectionFormFactory]]] = None


class ApiBackendDefinition(abc.ABC):
    core_backend_definition: Type[CoreBackendDefinition]

    formula_dialect_name: ClassVar[DialectName] = DialectName.DUMMY
    multi_query_mutation_factories: tuple[MQMFactorySettingItem, ...] = (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.basic,
            factory_cls=DefaultMultiQueryMutatorFactory,
        ),
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.no_compeng,
            factory_cls=NoCompengMultiQueryMutatorFactory,
        ),
    )
    is_forkable: ClassVar[bool] = True
    is_compeng_executable: ClassVar[bool] = False
    filter_formula_compiler_cls: ClassVar[Type[FilterFormulaCompiler]] = MainFilterFormulaCompiler


class ApiConnector(abc.ABC):
    backend_definition: Type[ApiBackendDefinition]
    connection_definitions: ClassVar[Tuple[Type[ApiConnectionDefinition], ...]] = ()
    source_definitions: ClassVar[Tuple[Type[ApiSourceDefinition], ...]] = ()
    translation_configs: ClassVar[frozenset[TranslationConfig]] = frozenset()
    compeng_dialect: Optional[DialectCombo] = None
