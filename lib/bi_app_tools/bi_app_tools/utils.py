from __future__ import annotations


SA_DIALECTS: tuple[tuple[str, str, str], ...] = (
    ("appmetrica_api", "sqlalchemy_metrika_api.base", "AppMetricaApiDialect"),
    ("bi_clickhouse", "bi_sqlalchemy_clickhouse.base", "BIClickHouseDialect"),
    ("bi_mssql", "bi_sqlalchemy_mssql.base", "BIMSSQLDialect"),
    ("bi_mysql", "bi_sqlalchemy_mysql.base", "BIMySQLDialect"),
    ("bi_oracle", "bi_sqlalchemy_oracle.base", "BIOracleDialect"),
    ("bi_postgresql", "bi_sqlalchemy_postgres.base", "BIPGDialect"),
    ("bi_promql", "bi_sqlalchemy_promql.base", "PromQLDialect"),
    ("bi_yq", "bi_sqlalchemy_yq.base", "BIYQDialect"),
    ("bi_chyt", "bi_sqlalchemy_chyt.base", "BICHYTDialect"),
    ("clickhouse", "clickhouse_sqlalchemy.drivers.http.base", "dialect"),
    ("gsheets", "bi_sqlalchemy_gsheets.base", "GSheetsDialect"),
    ("metrika_api", "sqlalchemy_metrika_api.base", "MetrikaApiDialect"),
    ("yql", "ydb.sqlalchemy", "YqlDialect"),
    ("bi_bitrix", "bi_sqlalchemy_bitrix.base", "BitrixDialect"),
)


def register_sa_dialects() -> None:
    """
    In case entrypoint-register does not work (e.g. in the arcadia build),
    register all known SQLAlchemy dialects explicitly.
    """
    import sqlalchemy as sa  # type: ignore
    for name, module, class_name in SA_DIALECTS:
        sa.dialects.registry.register(name, module, class_name)
