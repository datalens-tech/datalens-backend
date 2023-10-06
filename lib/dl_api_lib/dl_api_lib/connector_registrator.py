from typing import Type

from dl_api_connector.api_schema.source import (
    register_source_api_schema,
    register_source_template_api_schema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)
from dl_api_lib.connection_forms.registry import register_connection_form_factory_cls
from dl_api_lib.connection_info import register_connector_info_provider_cls
from dl_api_lib.connector_alias import register_connector_alias
from dl_api_lib.dashsql.registry import register_dash_sql_param_literalizer_cls
from dl_api_lib.dataset.dialect import register_dialect_name
from dl_api_lib.i18n.registry import register_translation_configs
from dl_api_lib.query.registry import (
    register_compeng_dialect,
    register_filter_formula_compiler_cls,
    register_is_compeng_executable,
    register_is_forkable_source,
    register_multi_query_mutator_factory_cls,
)
from dl_api_lib.schemas.connection import register_sub_schema_class


class ApiConnectorRegistrator:
    @classmethod
    def register_source_definition(cls, source_def: Type[ApiSourceDefinition]) -> None:
        register_source_api_schema(
            source_type=source_def.core_source_def_cls.source_type,
            schema_cls=source_def.api_schema_cls,
        )
        register_source_template_api_schema(
            source_type=source_def.core_source_def_cls.source_type,
            schema_cls=source_def.template_api_schema_cls,
        )

    @classmethod
    def register_connection_definition(cls, conn_def: Type[ApiConnectionDefinition]) -> None:
        conn_type = conn_def.core_conn_def_cls.conn_type
        register_connector_info_provider_cls(conn_type=conn_type, info_provider_cls=conn_def.info_provider_cls)
        register_connector_alias(conn_type=conn_type, alias=conn_def.alias)
        register_sub_schema_class(conn_type=conn_type, schema_cls=conn_def.api_generic_schema_cls)
        if conn_def.form_factory_cls is not None:
            register_connection_form_factory_cls(conn_type=conn_type, factory_cls=conn_def.form_factory_cls)

    @classmethod
    def register_connector(cls, connector: Type[ApiConnector]) -> None:
        # backend_type-related stuff - TODO: Move to a separate entity
        backend_type = connector.core_connector_cls.backend_type
        register_dialect_name(backend_type=backend_type, dialect_name=connector.formula_dialect_name)
        for mqm_setting_item in connector.multi_query_mutation_factories:
            register_multi_query_mutator_factory_cls(
                query_proc_mode=mqm_setting_item.query_proc_mode,
                backend_type=backend_type,
                dialects=mqm_setting_item.dialects,
                factory_cls=mqm_setting_item.factory_cls,
            )
        register_is_forkable_source(backend_type=backend_type, is_forkable=connector.is_forkable)
        register_is_compeng_executable(backend_type=backend_type, is_compeng_executable=connector.is_compeng_executable)
        register_filter_formula_compiler_cls(
            backend_type=backend_type,
            filter_compiler_cls=connector.filter_formula_compiler_cls,
        )
        register_dash_sql_param_literalizer_cls(
            backend_type=backend_type,
            literalizer_cls=connector.dashsql_literalizer_cls,
        )

        # everything else
        for source_def in connector.source_definitions:
            cls.register_source_definition(source_def=source_def)
        for conn_def in connector.connection_definitions:
            cls.register_connection_definition(conn_def=conn_def)
        register_translation_configs(connector.translation_configs)
        if connector.compeng_dialect is not None:
            register_compeng_dialect(connector.compeng_dialect)


CONN_REG_BI_API = ApiConnectorRegistrator
