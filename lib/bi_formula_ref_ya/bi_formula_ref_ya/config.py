from bi_formula.core.dialect import StandardDialect as D
from bi_formula.definitions.scope import Scope

from bi_formula_ref.paths import FuncPathTemplate, CatPathTemplate
from bi_formula_ref.audience import Audience
from bi_formula_ref.config import (
    RefDocGeneratorConfig, FuncDocConfigVersion, FuncDocTemplateConfig
)

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_yql.formula.constants import YqlDialect
from bi_connector_metrica.formula.constants import MetricaDialect
from bi_connector_oracle.formula.constants import OracleDialect
from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_postgresql.formula.constants import PostgreSQLDialect


DOC_GEN_CONFIG_YC = RefDocGeneratorConfig(
    func_doc_configs={
        FuncDocConfigVersion.overview_shortcut: FuncDocTemplateConfig(
            template_file='doc_func_long.md.jinja',
            func_file_path=FuncPathTemplate('function-ref/{func_name}.md'),
            cat_file_path=CatPathTemplate('function-ref/{category_name}-functions.md'),
        ),
    },
    doc_toc_filename='toc.yaml',
    doc_all_filename='function-ref/all.md',
    doc_avail_filename='function-ref/availability.md',
    function_scopes={
        Audience(name='internal'): Scope.DOCUMENTED | Scope.INTERNAL,
        Audience(name='external'): Scope.DOCUMENTED | Scope.YACLOUD,
    },
    block_conditions={'ycloud': True},
    supported_dialects=frozenset({
        D.ANY,
        *ClickHouseDialect.and_above(ClickHouseDialect.CLICKHOUSE_21_8).to_list(),
        MetricaDialect.METRIKAAPI,
        *MssqlDialect.and_above(MssqlDialect.MSSQLSRV_14_0).to_list(),
        *MySQLDialect.and_above(MySQLDialect.MYSQL_5_6).to_list(),
        *OracleDialect.and_above(OracleDialect.ORACLE_12_0).to_list(),
        *PostgreSQLDialect.and_above(PostgreSQLDialect.POSTGRESQL_9_3).to_list(),
        YqlDialect.YDB,
    }),
    default_example_dialect=ClickHouseDialect.CLICKHOUSE_22_10,
)
