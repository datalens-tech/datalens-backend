from __future__ import annotations

from typing import Dict, Set, TYPE_CHECKING

from bi_formula.core.dialect import DialectCombo, StandardDialect as D
from bi_formula.definitions.scope import Scope

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_yql.formula.constants import YqlDialect
from bi_connector_metrica.formula.constants import MetricaDialect
from bi_connector_oracle.formula.constants import OracleDialect
from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_postgresql.formula.constants import PostgreSQLDialect

from bi_formula_ref.registry.dialect_base import DialectExtractorBase
from bi_formula_ref.registry.scopes import SCOPES_BASE

if TYPE_CHECKING:
    from bi_formula_ref.registry.env import GenerationEnvironment
    import bi_formula_ref.registry.base as _registry_base


_SCOPES_ALL = SCOPES_BASE | Scope.INTERNAL | Scope.YACLOUD | Scope.DOUBLECLOUD
_SCOPES_INT_YC = SCOPES_BASE | Scope.INTERNAL | Scope.YACLOUD
SUPPORTED_DIALECTS: Dict[DialectCombo, int] = {
    MetricaDialect.METRIKAAPI: _SCOPES_INT_YC,
    D.ANY: _SCOPES_ALL,
    YqlDialect.YDB: _SCOPES_INT_YC,
    **{
        d: _SCOPES_INT_YC
        for d in OracleDialect.and_above(OracleDialect.ORACLE_12_0).to_list()
    },
    **{
        d: _SCOPES_ALL
        for d in (
            *ClickHouseDialect.and_above(ClickHouseDialect.CLICKHOUSE_21_8).to_list(),
            *MssqlDialect.and_above(MssqlDialect.MSSQLSRV_14_0).to_list(),
            *MySQLDialect.and_above(MySQLDialect.MYSQL_5_6).to_list(),
            *PostgreSQLDialect.and_above(PostgreSQLDialect.POSTGRESQL_9_3).to_list(),
        )
    },
}
COMPENG_SUPPORT = (
    # Lowest versions of all backends that are compatible with COMPENG
    *ClickHouseDialect.CLICKHOUSE.to_list(),
    *PostgreSQLDialect.POSTGRESQL.to_list(),
    *MySQLDialect.MYSQL.to_list(),
    *MssqlDialect.MSSQLSRV.to_list(),
    *OracleDialect.ORACLE.to_list(),
)
EXPAND_COMPENG = True


def should_include_dialect(dialect: DialectCombo, scopes: int) -> bool:
    for inc_dialect, dialect_scopes in SUPPORTED_DIALECTS.items():
        if dialect & inc_dialect and (dialect_scopes & scopes) == scopes:
            return True
    return False


class DefaultDialectExtractor(DialectExtractorBase):
    def get_dialects(
            self, item: _registry_base.FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> Set[DialectCombo]:
        if item.category.name == 'window':  # TODO: Maybe separate into two different classes?
            if EXPAND_COMPENG:
                return set(d for d in COMPENG_SUPPORT if should_include_dialect(d, scopes=env.scopes))
            return set()

        def_list = item.get_implementation_specs(env=env)
        dialect_mask = D.EMPTY
        for defn in def_list:
            dialect_mask |= defn.dialects

        return {
            dialect
            for dialect in dialect_mask.to_list()
            if should_include_dialect(dialect, scopes=env.scopes)
        }
