from bi_formula.core.dialect import StandardDialect as D
from bi_formula.definitions.scope import Scope

from bi_formula_ref.paths import FuncPathTemplate, CatPathTemplate
from bi_formula_ref.audience import DEFAULT_AUDIENCE
from bi_formula_ref.config import (
    RefDocGeneratorConfig, FuncDocConfigVersion, FuncDocTemplateConfig
)

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_postgresql.formula.constants import PostgreSQLDialect


DOC_GEN_CONFIG_DC = RefDocGeneratorConfig(
    func_doc_configs={
        FuncDocConfigVersion.overview_shortcut: FuncDocTemplateConfig(
            template_file='doc_func_long.md.jinja',
            func_file_path=FuncPathTemplate('{func_name}.md'),
            cat_file_path=CatPathTemplate('{category_name}-functions.md'),
        ),
    },
    doc_toc_filename='toc.yaml',
    doc_all_filename='all.md',
    doc_avail_filename='availability.md',
    gen_availability_table=False,
    function_scopes={
        DEFAULT_AUDIENCE: Scope.DOCUMENTED | Scope.DOUBLECLOUD,
    },
    block_conditions={'doublecloud': True},
    supported_dialects=frozenset({
        D.ANY,
        *ClickHouseDialect.and_above(ClickHouseDialect.CLICKHOUSE_21_8).to_list(),
        *MssqlDialect.and_above(MssqlDialect.MSSQLSRV_14_0).to_list(),
        *MySQLDialect.and_above(MySQLDialect.MYSQL_5_6).to_list(),
        *PostgreSQLDialect.and_above(PostgreSQLDialect.POSTGRESQL_9_3).to_list(),
    }),
    default_example_dialect=ClickHouseDialect.CLICKHOUSE_22_10,
)
