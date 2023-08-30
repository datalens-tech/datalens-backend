from bi_connector_snowflake.core.connector import (
    SnowFlakeCoreConnector,
    SnowFlakeCoreConnectionDefinition,
    SnowFlakeCoreTableSourceDefinition,
)

from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)

from bi_connector_snowflake.formula.constants import DIALECT_NAME_SNOWFLAKE
from bi_connector_snowflake.bi.api_schema.source import (  # type: ignore
    SnowFlakeTableDataSourceSchema,
    SnowFlakeTableDataSourceTemplateSchema,
)
from bi_connector_snowflake.bi.api_schema.connection import SnowFlakeConnectionSchema
from bi_connector_snowflake.bi.connection_form.form_config import SnowFlakeConnectionFormFactory
from bi_connector_snowflake.bi.connection_info import SnowflakeConnectionInfoProvider
from bi_connector_snowflake.bi.i18n.localizer import CONFIGS


class SnowFlakeBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = SnowFlakeCoreTableSourceDefinition
    api_schema_cls = SnowFlakeTableDataSourceSchema
    template_api_schema_cls = SnowFlakeTableDataSourceTemplateSchema


class SnowFlakeBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = SnowFlakeCoreConnectionDefinition
    api_generic_schema_cls = SnowFlakeConnectionSchema
    info_provider_cls = SnowflakeConnectionInfoProvider
    form_factory_cls = SnowFlakeConnectionFormFactory


class SnowFlakeBiApiConnector(BiApiConnector):
    core_connector_cls = SnowFlakeCoreConnector
    connection_definitions = (SnowFlakeBiApiConnectionDefinition,)
    source_definitions = (SnowFlakeBiApiTableSourceDefinition,)
    formula_dialect_name = DIALECT_NAME_SNOWFLAKE
    translation_configs = frozenset(CONFIGS)
