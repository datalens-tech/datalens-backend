from __future__ import annotations

from typing import TYPE_CHECKING

from bi_formula.core.dialect import DialectCombo, StandardDialect as D

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_oracle.formula.constants import OracleDialect
from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_postgresql.formula.constants import PostgreSQLDialect

from bi_formula_ref.registry.dialect_base import DialectExtractorBase

if TYPE_CHECKING:
    from bi_formula_ref.registry.env import GenerationEnvironment
    import bi_formula_ref.registry.base as _registry_base


COMPENG_SUPPORT = (
    # Lowest versions of all backends that are compatible with COMPENG
    *ClickHouseDialect.CLICKHOUSE.to_list(),
    *PostgreSQLDialect.POSTGRESQL.to_list(),
    *MySQLDialect.MYSQL.to_list(),
    *MssqlDialect.MSSQLSRV.to_list(),
    *OracleDialect.ORACLE.to_list(),
)
EXPAND_COMPENG = True


class DefaultDialectExtractor(DialectExtractorBase):
    def get_dialects(
            self, item: _registry_base.FunctionDocRegistryItem, env: GenerationEnvironment,
    ) -> set[DialectCombo]:
        if item.category.name == 'window':  # TODO: Maybe separate into two different classes?
            if EXPAND_COMPENG:
                return set(dialect for dialect in COMPENG_SUPPORT if dialect in env.supported_dialects)
            return set()

        def_list = item.get_implementation_specs(env=env)
        dialect_mask = D.EMPTY
        for defn in def_list:
            dialect_mask |= defn.dialects

        return {
            dialect
            for dialect in dialect_mask.to_list()
            if dialect in env.supported_dialects
        }
