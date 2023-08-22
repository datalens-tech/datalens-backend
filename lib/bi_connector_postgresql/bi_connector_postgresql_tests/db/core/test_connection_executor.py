from typing import Optional, Sequence

import pytest
import shortuuid
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from bi_constants.enums import BIType

from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.connection_models.common_models import DBIdent, TableIdent

from bi_testing.regulated_test import RegulatedTestParams
from bi_core_testing.database import Db
from bi_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite, DefaultAsyncConnectionExecutorTestSuite,
)

from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL

from bi_connector_postgresql_tests.db.config import CoreConnectionSettings
from bi_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass
from bi_sqlalchemy_postgres.base import CITEXT


class PostgreSQLSyncAsyncConnectionExecutorCheckBase(
        BasePostgreSQLTestClass,
        DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: '',  # TODO: FIXME
        },
    )

    @pytest.fixture(scope='function')
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert '.' in db_version

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[tuple[TypeEngine, BIType]]]:
        return {
            'types_postgres': [
                (sa.Integer(), BIType.integer),
                (sa.Float(), BIType.float),
                (sa.String(length=256), BIType.string),
                (sa.Date(), BIType.date),
                (sa.DateTime(), BIType.genericdatetime),
                (CITEXT(), BIType.string),
            ],
        }

    @pytest.fixture(scope='function')
    def enabled_citext_extension(self, db: Db) -> None:
        db.execute("CREATE EXTENSION IF NOT EXISTS CITEXT;")

    def test_type_recognition(self, db: Db, sync_connection_executor: SyncConnExecutorBase,
                              enabled_citext_extension) -> None:
        for schema_name, type_schema in sorted(self.get_schemas_for_type_recognition().items()):
            columns = [
                sa.Column(name=f'c_{shortuuid.uuid().lower()}', type_=sa_type)
                for sa_type, user_type in type_schema
            ]
            sa_table = db.table_from_columns(columns=columns)
            db.create_table(sa_table)
            table_def = TableIdent(db_name=db.name, schema_name=sa_table.schema, table_name=sa_table.name)
            detected_columns = sync_connection_executor.get_table_schema_info(table_def=table_def).schema
            assert len(detected_columns) == len(type_schema), f'Incorrect number of columns in schema {schema_name}'
            for col_idx, ((sa_type, user_type), detected_col) in enumerate(zip(type_schema, detected_columns)):
                assert detected_col.user_type == user_type, (
                    f'Incorrect user type detected for schema {schema_name} col #{col_idx}: '
                    f'expected {user_type.name}, got {detected_col.user_type.name}'
                )


class TestPostgreSQLSyncConnectionExecutor(
        PostgreSQLSyncAsyncConnectionExecutorCheckBase,
        DefaultSyncConnectionExecutorTestSuite[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: '',  # TODO: FIXME
        },
    )


class TestPostgreSQLAsyncConnectionExecutor(
        PostgreSQLSyncAsyncConnectionExecutorCheckBase,
        DefaultAsyncConnectionExecutorTestSuite[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: 'Not implemented',
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: 'Not implemented',
        },
    )
