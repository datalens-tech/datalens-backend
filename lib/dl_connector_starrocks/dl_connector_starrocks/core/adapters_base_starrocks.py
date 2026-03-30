import attr
import sqlalchemy as sa
import sqlalchemy.dialects.mysql as sa_mysql

from dl_sqlalchemy_starrocks.base import BIStarRocksDialect
from dl_type_transformer.native_type import SATypeSpec

from dl_connector_starrocks.core.constants import (
    CONNECTION_TYPE_STARROCKS,
    STARROCKS_SYSTEM_CATALOGS,
)
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


_IDENTIFIER_PREPARER = BIStarRocksDialect().identifier_preparer

_SYSTEM_CATALOGS_SQL_LIST = ", ".join(f"'{c}'" for c in STARROCKS_SYSTEM_CATALOGS)


def _quote_ident(name: str) -> str:
    """Backtick-quote a StarRocks identifier using the MySQL dialect's identifier preparer."""
    return _IDENTIFIER_PREPARER.quote_identifier(name)


def _info_schema_ref(catalog: str, table: str) -> str:
    """Build a fully qualified reference: `catalog`.`information_schema`.`table`.

    Raw SQL with per-segment quoting is required because MySQL dialect backtick-quotes
    dotted schema names as a single identifier, which StarRocks rejects.
    """
    return f"{_quote_ident(catalog)}.{_quote_ident('information_schema')}.{_quote_ident(table)}"


class StarRocksQueryConstructorMixin:
    def _compile_pagination_params(
        self, search_text: str | None, limit: int | None, offset: int | None
    ) -> list[sa.sql.expression.BindParameter]:
        params = []
        if search_text:
            params.append(sa.bindparam("search_text", f"%{search_text}%", type_=sa.String))
        if limit is not None:
            params.append(sa.bindparam("limit", limit, type_=sa.Integer))
            if offset:
                params.append(sa.bindparam("offset", offset, type_=sa.Integer))
        return params

    def _get_pagination_sql_parts(self, limit: int | None, offset: int | None) -> list[str]:
        sql_parts = []
        if limit is not None:
            # In MySQL/StarRocks OFFSET requires LIMIT to be present.
            # We have a default for these parameters in the listing endpoint but
            # it would be correct to take this into account when constructing a query
            sql_parts.append("LIMIT :limit")
            if offset:
                sql_parts.append("OFFSET :offset")
        return sql_parts

    def get_list_tables_query(
        self,
        catalog: str,
        search_text: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> sa.sql.elements.TextClause:
        ref = _info_schema_ref(catalog, "tables")
        sql_parts = [
            f"SELECT TABLE_SCHEMA, TABLE_NAME FROM {ref}" f" WHERE TABLE_SCHEMA NOT IN ({_SYSTEM_CATALOGS_SQL_LIST})"
        ]
        if search_text:
            sql_parts.append("AND TABLE_NAME LIKE :search_text")
        sql_parts.append("ORDER BY TABLE_SCHEMA, TABLE_NAME")
        sql_parts.extend(self._get_pagination_sql_parts(limit, offset))
        sql = " ".join(sql_parts)
        query = sa.text(sql)
        params = self._compile_pagination_params(search_text, limit, offset)
        if params:
            return query.bindparams(*params)
        return query

    def get_table_info_query(self, catalog: str, database: str, table_name: str) -> sa.sql.elements.TextClause:
        ref = _info_schema_ref(catalog, "columns")
        return sa.text(
            f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE"
            f" FROM {ref}"
            f" WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table_name"
            f" ORDER BY ORDINAL_POSITION"
        ).bindparams(
            sa.bindparam("database", database, type_=sa.String),
            sa.bindparam("table_name", table_name, type_=sa.String),
        )

    def get_table_exists_query(self, catalog: str, database: str, table_name: str) -> sa.sql.elements.TextClause:
        ref = _info_schema_ref(catalog, "tables")
        return sa.text(
            f"SELECT COUNT(*) FROM {ref}" f" WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table_name"
        ).bindparams(
            sa.bindparam("database", database, type_=sa.String),
            sa.bindparam("table_name", table_name, type_=sa.String),
        )


@attr.s(cmp=False)
class BaseStarRocksAdapter(StarRocksQueryConstructorMixin):
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    conn_type = CONNECTION_TYPE_STARROCKS

    # StarRocks uses MySQL wire protocol, so type codes are mostly MySQL-compatible.
    # Integer keys are MySQL protocol field type codes from field_types.h:
    # https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
    # StarRocks types: https://docs.starrocks.io/docs/sql-reference/data-types/
    _type_code_to_sa: dict[int, SATypeSpec] | None = {
        1: sa_mysql.TINYINT,  #   MYSQL_TYPE_TINY        (BOOLEAN, TINYINT)
        2: sa_mysql.SMALLINT,  #  MYSQL_TYPE_SHORT       (SMALLINT)
        3: sa_mysql.INTEGER,  #   MYSQL_TYPE_LONG        (INT)
        4: sa_mysql.FLOAT,  #     MYSQL_TYPE_FLOAT       (FLOAT)
        5: sa_mysql.DOUBLE,  #    MYSQL_TYPE_DOUBLE      (DOUBLE)
        8: sa_mysql.BIGINT,  #    MYSQL_TYPE_LONGLONG    (BIGINT)
        10: sa_mysql.DATE,  #     MYSQL_TYPE_DATE        (DATE)
        11: sa_mysql.TIME,  #     MYSQL_TYPE_TIME        (TIME)
        12: sa_mysql.DATETIME,  # MYSQL_TYPE_DATETIME    (DATETIME)
        246: sa_mysql.DECIMAL,  # MYSQL_TYPE_NEWDECIMAL  (DECIMAL)
        252: sa_mysql.TEXT,  #    MYSQL_TYPE_BLOB        (VARBINARY)
        253: sa_mysql.VARCHAR,  # MYSQL_TYPE_VAR_STRING  (VARCHAR, STRING)
        254: sa_mysql.CHAR,  #    MYSQL_TYPE_STRING      (LARGEINT, CHAR, JSON, HLL, BITMAP, ARRAY, MAP, STRUCT)
    }
