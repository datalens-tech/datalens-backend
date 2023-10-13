from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
    Tuple,
    Type,
)

import oracledb  # type: ignore  # TODO: fix
import sqlalchemy as sa
import sqlalchemy.dialects.oracle.base as sa_ora  # not all data types are imported in init in older SA versions
from sqlalchemy.sql.type_api import TypeEngine

from dl_core.connection_executors.adapters.adapters_base_sa_classic import (
    BaseClassicAdapter,
    ClassicSQLConnLineConstructor,
)
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_models import (
    DBIdent,
    SchemaIdent,
    TableIdent,
)
from dl_core.db.native_type import SATypeSpec

from dl_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from dl_connector_oracle.core.target_dto import OracleConnTargetDTO


class OracleConnLineConstructor(ClassicSQLConnLineConstructor[OracleConnTargetDTO]):
    """"""
    def _get_dsn_params(
        self,
        safe_db_symbols: Tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        return dict(
            super()._get_dsn_params(
                safe_db_symbols=safe_db_symbols,
                db_name=db_name,
                standard_auth=standard_auth,
            ),
            db_name_type=self._target_dto.db_name_type.value.upper(),
        )


class OracleDefaultAdapter(BaseClassicAdapter[OracleConnTargetDTO]):
    conn_type = CONNECTION_TYPE_ORACLE
    conn_line_constructor_type: ClassVar[Type[OracleConnLineConstructor]] = OracleConnLineConstructor

    dsn_template = (
        "{dialect}://{user}:{passwd}@(DESCRIPTION="
        "(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))"
        "(CONNECT_DATA=({db_name_type}={db_name})))"
    )

    def _test(self) -> None:
        self.execute(DBAdapterQuery("SELECT 1 FROM DUAL")).get_all()

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return self.execute(DBAdapterQuery("SELECT * FROM V$VERSION", db_name=db_ident.db_name)).get_all()[0][0]

    def normalize_sa_col_type(self, sa_col_type: TypeEngine) -> TypeEngine:
        if isinstance(sa_col_type, sa_ora.NUMBER) and not sa_col_type.scale:
            return sa.Integer()

        return super().normalize_sa_col_type(sa_col_type)

    def _is_table_exists(self, table_ident: TableIdent) -> bool:
        sa_exists_result = super()._is_table_exists(table_ident)

        # Dialect does not check for views so we will check it manually
        if not sa_exists_result:
            assert "'" not in table_ident.table_name  # FIXME: quote the name properly

            rows = self.execute(
                DBAdapterQuery(
                    f"SELECT view_name FROM all_views WHERE UPPER(view_name) = '{table_ident.table_name.upper()}'"
                )
            ).get_all()

            return len(rows) > 0

        return sa_exists_result

    _type_code_to_sa = {
        oracledb.NUMBER: sa_ora.NUMBER,
        oracledb.STRING: sa_ora.VARCHAR,  # e.g. 'VARCHAR2(44)'
        oracledb.NATIVE_FLOAT: sa_ora.BINARY_FLOAT,  # ... or `sa_ora.BINARY_FLOAT`
        oracledb.FIXED_CHAR: sa_ora.CHAR,  # e.g. 'CHAR(1)'
        oracledb.FIXED_NCHAR: sa_ora.NCHAR,  # e.g. 'NCHAR(1)'
        oracledb.NCHAR: sa_ora.NVARCHAR,  # e.g. 'NVARCHAR(5)'
        oracledb.DATETIME: sa_ora.DATE,
        oracledb.TIMESTAMP: sa_ora.TIMESTAMP,  # e.g. 'TIMESTAMP(9)', 'TIMESTAMP(9) WITH TIME ZONE'
        # # For newer versions of oracledb, just in case.
        # # Listed are all types from 8.1.0,
        # # Enabled are types listed in `dl_core.db.conversions`.
        # getattr(oracledb, 'DB_TYPE_BFILE', None): sa_ora.NULL,
        getattr(oracledb, "DB_TYPE_BINARY_DOUBLE", None): sa_ora.BINARY_DOUBLE,
        getattr(oracledb, "DB_TYPE_BINARY_FLOAT", None): sa_ora.BINARY_FLOAT,
        # getattr(oracledb, 'DB_TYPE_BINARY_INTEGER', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_BLOB', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_BOOLEAN', None): sa_ora.NULL,
        getattr(oracledb, "DB_TYPE_CHAR", None): sa_ora.CHAR,
        # getattr(oracledb, 'DB_TYPE_CLOB', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_CURSOR', None): sa_ora.NULL,
        getattr(oracledb, "DB_TYPE_DATE", None): sa_ora.DATE,
        # getattr(oracledb, 'DB_TYPE_INTERVAL_DS', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_INTERVAL_YM', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_JSON', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_LONG', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_LONG_RAW', None): sa_ora.NULL,
        getattr(oracledb, "DB_TYPE_NCHAR", None): sa_ora.NCHAR,
        # getattr(oracledb, 'DB_TYPE_NCLOB', None): sa_ora.NULL,
        getattr(oracledb, "DB_TYPE_NUMBER", None): sa_ora.NUMBER,
        getattr(oracledb, "DB_TYPE_NVARCHAR", None): sa_ora.NVARCHAR,
        # getattr(oracledb, 'DB_TYPE_OBJECT', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_RAW', None): sa_ora.NULL,
        # getattr(oracledb, 'DB_TYPE_ROWID', None): sa_ora.NULL,
        getattr(oracledb, "DB_TYPE_TIMESTAMP", None): sa_ora.TIMESTAMP,
        # getattr(oracledb, 'DB_TYPE_TIMESTAMP_LTZ', None): sa_ora.NULL,
        # # NOTE: not `timestamptz` here; matches the view schema, somehow.
        getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", None): sa_ora.TIMESTAMP,
        getattr(oracledb, "DB_TYPE_VARCHAR", None): sa_ora.VARCHAR,
    }

    def _cursor_column_to_name(self, cursor_col, dialect=None) -> str:  # type: ignore  # TODO: fix
        assert dialect, "required in this case"
        # To match the `get_columns` result.
        # Generally just lowercases the name.
        # Notably, column names seem to be case-insensitive in oracle.
        return dialect.normalize_name(cursor_col[0])

    def _cursor_column_to_sa(self, cursor_col, require: bool = True) -> Optional[SATypeSpec]:  # type: ignore  # TODO: fix
        """
        cursor_col:

            name, type, display_size, internal_size, precision, scale, null_ok

        See also:
        https://cx-oracle.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.description
        """
        type_code = cursor_col[1]
        if type_code is None:  # shouldn't really happen
            if require:
                raise ValueError(f"Cursor column has no type: {cursor_col!r}")
            return None
        sa_cls = self._type_code_to_sa.get(type_code)
        if sa_cls is None:
            if require:
                raise ValueError(f"Unknown cursor type: {type_code!r}")
            return None

        # It would be nice to distinguish timestamp/timestamptz here, but the
        # only observed difference is `display_size=23` for tz-*naive*
        # timestamp.

        if sa_cls is sa_ora.NUMBER:
            # See also: `self.normalize_sa_col_type`
            precision = cursor_col[4]
            scale = cursor_col[5]

            # Going by the comparison with the 'create view' -> SA logic.
            if scale == -127:
                scale = 0

            sa_type = sa_cls(precision, scale)
        else:
            sa_type = sa_cls

        return sa_type

    def _cursor_column_to_nullable(self, cursor_col) -> Optional[bool]:  # type: ignore  # TODO: fix
        return bool(cursor_col[6])

    def _make_cursor_info(self, cursor, db_session=None) -> dict:  # type: ignore  # TODO: fix
        return dict(
            super()._make_cursor_info(cursor, db_session=db_session),
            cxoracle_types=[self._cursor_type_to_str(column[1]) for column in cursor.description],
        )

    ORACLE_LIST_SOURCES_ALL_SCHEMA_SQL = """
        SELECT OWNER, TABLE_NAME FROM ALL_TABLES
        WHERE nvl(tablespace_name, 'no tablespace')
            NOT IN ('SYSTEM', 'SYSAUX')
            AND IOT_NAME IS NULL
            AND DURATION IS NULL
    """

    def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        if schema_ident.schema_name is not None:
            return super()._get_tables(schema_ident)

        db_name = schema_ident.db_name
        db_engine = self.get_db_engine(db_name)

        query = self.ORACLE_LIST_SOURCES_ALL_SCHEMA_SQL
        result = db_engine.execute(sa.text(query))
        return [
            TableIdent(
                db_name=db_name,
                schema_name=owner,
                table_name=table_name,
            )
            for owner, table_name in result
        ]

    @staticmethod
    def _cursor_type_to_str(value: Any) -> str:
        return value.name.lower()
