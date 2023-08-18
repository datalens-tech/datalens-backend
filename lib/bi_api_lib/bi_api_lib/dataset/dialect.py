from __future__ import annotations

from typing import Dict

from bi_constants.enums import SourceBackendType

from bi_formula.core.dialect import DialectName

from bi_connector_mssql.core.constants import BACKEND_TYPE_MSSQL
from bi_connector_mysql.core.constants import BACKEND_TYPE_MYSQL
from bi_connector_oracle.core.constants import BACKEND_TYPE_ORACLE
from bi_connector_postgresql.core.greenplum.constants import BACKEND_TYPE_GREENPLUM
from bi_connector_postgresql.core.postgresql.constants import BACKEND_TYPE_POSTGRES
from bi_connector_metrica.core.constants import BACKEND_TYPE_METRICA_API, BACKEND_TYPE_APPMETRICA_API
from bi_connector_yql.core.ydb.constants import BACKEND_TYPE_YDB
from bi_connector_yql.core.yq.constants import BACKEND_TYPE_YQ


_DIALECT_NAMES_FROM_SA: Dict[SourceBackendType, DialectName] = {}


def register_dialect_name(backend_type: SourceBackendType, dialect_name: DialectName) -> None:
    if backend_type in _DIALECT_NAMES_FROM_SA:
        assert _DIALECT_NAMES_FROM_SA[backend_type] == dialect_name
    _DIALECT_NAMES_FROM_SA[backend_type] = dialect_name


def resolve_dialect_name(backend_type: SourceBackendType) -> DialectName:
    return _DIALECT_NAMES_FROM_SA.get(backend_type, DialectName.DUMMY)


# TODO: Connectorize
register_dialect_name(backend_type=BACKEND_TYPE_MYSQL, dialect_name=DialectName.MYSQL)
register_dialect_name(backend_type=BACKEND_TYPE_POSTGRES, dialect_name=DialectName.POSTGRESQL)
register_dialect_name(backend_type=BACKEND_TYPE_MSSQL, dialect_name=DialectName.MSSQLSRV)
register_dialect_name(backend_type=BACKEND_TYPE_ORACLE, dialect_name=DialectName.ORACLE)
register_dialect_name(backend_type=SourceBackendType.CLICKHOUSE, dialect_name=DialectName.CLICKHOUSE)
register_dialect_name(backend_type=SourceBackendType.CHYT, dialect_name=DialectName.CLICKHOUSE)
register_dialect_name(backend_type=SourceBackendType.CHYDB, dialect_name=DialectName.CLICKHOUSE)
register_dialect_name(backend_type=BACKEND_TYPE_GREENPLUM, dialect_name=DialectName.POSTGRESQL)
register_dialect_name(backend_type=BACKEND_TYPE_METRICA_API, dialect_name=DialectName.METRIKAAPI)
register_dialect_name(backend_type=BACKEND_TYPE_APPMETRICA_API, dialect_name=DialectName.METRIKAAPI)
register_dialect_name(backend_type=BACKEND_TYPE_YQ, dialect_name=DialectName.YQ)
register_dialect_name(backend_type=BACKEND_TYPE_YDB, dialect_name=DialectName.YDB)
