import abc
from typing import (
    ClassVar,
    Collection,
    Optional,
)

import attr

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.source_base import (
    DataSourceBaseSchema,
    DataSourceTemplateBaseSchema,
)
from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_api_connector.form_config.models.base import ConnectionFormFactory
from dl_constants.enums import (
    QueryProcessingMode,
    SourceBackendType,
)
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
    MultiQueryMutatorFactoryBase,
    NoCompengMultiQueryMutatorFactory,
)


class ApiSourceDefinition(abc.ABC):
    core_source_def_cls: ClassVar[type[CoreSourceDefinition]]
    api_schema_cls: ClassVar[type[DataSourceBaseSchema]] = DataSourceBaseSchema
    template_api_schema_cls: ClassVar[type[DataSourceTemplateBaseSchema]] = DataSourceTemplateBaseSchema


class ApiConnectionDefinition(abc.ABC):
    core_conn_def_cls: ClassVar[type[CoreConnectionDefinition]]
    api_generic_schema_cls: ClassVar[type[ConnectionSchema]]
    alias: ClassVar[Optional[str]] = None  # TODO remove in favor of info provider
    info_provider_cls: ClassVar[type[ConnectionInfoProvider]]
    form_factory_cls: ClassVar[Optional[type[ConnectionFormFactory]]] = None


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class MQMFactoryKey:
    query_proc_mode: QueryProcessingMode
    backend_type: SourceBackendType
    dialect: Optional[DialectCombo]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class MQMFactorySettingItem:
    query_proc_mode: QueryProcessingMode
    factory_cls: type[MultiQueryMutatorFactoryBase]
    dialects: Collection[Optional[DialectCombo]] = attr.ib(default=(None,))


class ApiBackendDefinition(abc.ABC):
    core_backend_definition: type[CoreBackendDefinition]

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
    filter_formula_compiler_cls: ClassVar[type[FilterFormulaCompiler]] = MainFilterFormulaCompiler


class ApiConnector(abc.ABC):
    backend_definition: type[ApiBackendDefinition]
    connection_definitions: ClassVar[tuple[type[ApiConnectionDefinition], ...]] = ()
    source_definitions: ClassVar[tuple[type[ApiSourceDefinition], ...]] = ()
    translation_configs: ClassVar[frozenset[TranslationConfig]] = frozenset()
    compeng_dialect: Optional[DialectCombo] = None
