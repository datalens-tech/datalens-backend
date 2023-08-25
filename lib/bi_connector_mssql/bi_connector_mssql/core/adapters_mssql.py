from __future__ import annotations

import datetime
import decimal
import logging
import re
from typing import Optional, Tuple, Type, List
from urllib.parse import quote_plus

import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy.dialects import mssql as ms_types
import pyodbc

import bi_core.exc as exc
from bi_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from bi_core.connectors.base.error_transformer import DBExcKWArgs
from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery, RawColumnInfo, RawSchemaInfo
from bi_core.connection_models import DBIdent, SATextTableDefinition, SchemaIdent, TableIdent
from bi_core.db.native_type import CommonNativeType

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mssql.core.exc import SyncMssqlSourceDoesNotExistError


LOGGER = logging.getLogger(__name__)


class MSSQLDefaultAdapter(BaseClassicAdapter):
    conn_type = CONNECTION_TYPE_MSSQL

    dsn_template = '{dialect}:///?odbc_connect=' + quote_plus(';').join([
        quote_plus('DRIVER={FreeTDS}'),
        quote_plus('Server=') + '{host}',
        quote_plus('Port=') + '{port}',
        quote_plus('Database=') + '{db_name}',
        quote_plus('UID=') + '{user}',
        quote_plus('PWD=') + '{passwd}',
        quote_plus('TDS_Version=8.0')
    ])  # {...}s are are left unquoted for future formatting in `get_conn_line`

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return self.execute(DBAdapterQuery('SELECT @@VERSION', db_name=db_ident.db_name)).get_all()[0][0]

    _type_code_to_sa = {
        int: ms_types.INTEGER,
        float: ms_types.FLOAT,
        decimal.Decimal: ms_types.DECIMAL,
        bool: ms_types.BIT,
        str: ms_types.NTEXT,
        datetime.datetime: ms_types.DATETIME,
    }

    MSSQL_LIST_SOURCES_ALL_SCHEMAS_SQL = \
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;"

    def _get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        if schema_ident.schema_name is not None:
            # For a single schema, plug into the common SA code.
            # (might not be ever used)
            return super()._get_tables(schema_ident)

        db_name = schema_ident.db_name
        db_engine = self.get_db_engine(db_name)
        query = self.MSSQL_LIST_SOURCES_ALL_SCHEMAS_SQL
        result = db_engine.execute(sa.text(query))
        return [
            TableIdent(
                db_name=db_name,
                schema_name=schema_name,
                table_name=name,
            )
            for schema_name, name in result
        ]

    def _get_subselect_table_info(self, subquery: SATextTableDefinition) -> RawSchemaInfo:
        """
        For the record, pyodbc's cursor info only contains very approximate type information.
        Test-data example (name, type_code, row value):

            [('number', int, 0),
             ('num_tinyint', int, 0),
             ('num_smallint', int, 0),
             ('num_integer', int, 0),
             ('num_bigint', int, 0),
             ('num_float', float, 0.0),
             ('num_real', float, 0.0),
             ('num_numeric', decimal.Decimal, Decimal('0')),
             ('num_decimal', decimal.Decimal, Decimal('0')),
             ('num_bit', bool, False),
             ('num_char', str, '0                             '),
             ('num_varchar', str, '0'),
             ('num_text', str, '0'),
             ('num_nchar', str, '0                             '),
             ('num_nvarchar', str, '0'),
             ('num_ntext', str, '0'),
             ('num_date', str, '2020-01-01'),
             ('num_datetime', datetime.datetime, datetime.datetime(1900, 1, 1, 0, 0)),
             ('num_datetime2', str, '2020-01-01 00:00:00.0000000'),
             ('num_smalldatetime', datetime.datetime, datetime.datetime(1900, 1, 1, 0, 0)),
             ('num_datetimeoffset', str, '2020-01-01 00:00:00.0000000 +00:00'),
             ('uuid', str, '5F473FA6-E7C3-45E6-9190-A13C7E1558BF')]

        All stuff it returns: name, type, display size (pyodbc does not set
        this value), internal size (in bytes), precision, scale, nullable.

        However, there's a default stored procedure for getting a select schema:
        https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-describe-first-result-set-transact-sql?view=sql-server-ver15#permissions

            exec sp_describe_first_result_set @tsql = N'select * from {subselect}'

        Note that the entire subquery is passed as a string.
        """
        from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery
        # 'select * from (<user_input_text>) as source'
        select_all = sa.select([sa.literal_column('*')]).select_from(subquery.text)
        select_all_str = str(select_all)  # Should be straightforward and safe and reliable. Really.
        sa_query_text = 'exec sp_describe_first_result_set @tsql = :select_all'
        sa_query = sa.sql.text(sa_query_text)
        sa_query = sa_query.bindparams(
            # TODO?: might need a better `type_` value
            sa.sql.bindparam('select_all', select_all_str, type_=sa.types.Unicode),
        )
        dba_query = DBAdapterQuery(query=sa_query)
        query_res = self.execute(dba_query)
        data = [row for chunk in query_res.data_chunks for row in chunk]
        names = query_res.cursor_info['names']
        if data:
            assert len(names) == len(data[0])
        data = [dict(zip(names, row)) for row in data]

        engine = self.get_db_engine(db_name=None)
        dialect = engine.dialect
        ischema_names = dialect.ischema_names

        columns = []
        for column_info in data:
            name = column_info['name']
            if not name:
                LOGGER.warning('Empty name in mssql subselect schema: %r', column_info)
                continue

            type_name = column_info['system_type_name']
            type_name_base = type_name.split('(', 1)[0]

            sa_type = ischema_names.get(type_name_base)
            if sa_type is None:
                LOGGER.warning('Unknown/unsupported type in mssql subselect schema: %r', column_info)
                sa_type = sa.sql.sqltypes.NullType

            # NOTE: it is possible to instantiate the `sa_type` here; but for
            # now, there's no known use for that.

            # Side note: any `cast()` in mssql tends to make the value nullable.
            nullable = column_info['is_nullable']

            native_type = CommonNativeType.normalize_name_and_create(
                conn_type=self.conn_type,
                name=self.normalize_sa_col_type(sa_type),
                nullable=nullable,
            )

            columns.append(RawColumnInfo(
                name=name,
                title=name,
                nullable=native_type.nullable,
                native_type=native_type,
            ))

        return RawSchemaInfo(columns=tuple(columns))

    _EXC_CODE_RE = re.compile(
        r'\(\'[0-9A-Z]+\', [\'\"]\[(?P<state>[0-9A-Z]+)\] '
        r'\[FreeTDS\][^(]+'
        r'\((?P<code>\d+)\)'
    )
    _EXC_CODE_MAP = {
        # [42S22] Invalid column name '.+'. (207)
        207: exc.ColumnDoesNotExist,
        # [42S02] Invalid object name '.+'. (208)
        208: SyncMssqlSourceDoesNotExistError,
        # [22007] Conversion failed when converting date and/or time from character string. (241)
        241: exc.DataParseError,
        # [22012] Divide by zero error encountered. (8134)
        8134: exc.DivisionByZero,
        # [08S01] Read from the server failed (20004)
        20004: exc.SourceConnectError,
        # [08S01] Write to the server failed (20006)
        20006: exc.SourceConnectError,
        # [01000] Unexpected EOF from the server (20017)
        20017: exc.SourceClosedPrematurely,

        # ?
        # [23000] The statement terminated. The maximum recursion 100
        #         has been exhausted before statement completion. (530)
        # [42000] Snapshot isolation transaction failed in database '.+'
        #         because the object accessed by the statement has been modified
        #         by a DDL statement... (3961)
        # [42000] Transaction (Process ID \d+) was deadlocked on lock... (1205)
        # [42000] The batch could not be analyzed because of compile errors. (11501)
    }

    _EXC_STATE_MAP = {
        # [08001] Unable to connect to data source (0)
        '08001': exc.SourceConnectError,
        # [HY000] Could not perform COMMIT or ROLLBACK (0)
        'HY000': exc.CommitOrRollbackFailed,
    }

    EXTRA_EXC_CLS = (pyodbc.Error, sa_exc.DBAPIError)

    @classmethod
    def get_exc_class(cls, err_msg: str) -> Optional[Type[exc.DatabaseQueryError]]:
        err_match = cls._EXC_CODE_RE.match(err_msg)
        if err_match is not None:
            code = int(err_match.group('code'))
            state = err_match.group('state').upper()
            if code in cls._EXC_CODE_MAP:
                return cls._EXC_CODE_MAP[code]
            elif state in cls._EXC_STATE_MAP:
                return cls._EXC_STATE_MAP[state]
            return exc.DatabaseQueryError

        return None

    @classmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
            cls,
            wrapper_exc: Exception,
            orig_exc: Optional[Exception],
            debug_compiled_query: Optional[str]
    ) -> Tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        exc_cls, kw = super().make_exc(wrapper_exc, orig_exc, debug_compiled_query)

        db_msg = kw['db_message']
        specific_exc_cls = cls.get_exc_class(db_msg)  # type: ignore  # TODO: fix
        exc_cls = specific_exc_cls if specific_exc_cls is not None else exc_cls

        return exc_cls, kw
