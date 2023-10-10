import abc

import pytest
import sqlalchemy as sa

from dl_configs.settings_submodels import S3Settings
from dl_constants.enums import UserDataType
from dl_core.connection_executors import (
    AsyncConnExecutorBase,
    ConnExecutorQuery,
)
from dl_core.connection_models import DBIdent
from dl_core_testing.database import DbTable
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams
from dl_testing.s3_utils import s3_tbl_func_maker

from dl_connector_bundle_chs3_tests.db.base.core.base import (
    FILE_CONN_TV,
    BaseCHS3TestClass,
)


class CHS3SyncAsyncConnectionExecutorTestBase(
    BaseCHS3TestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Assumes it always exists",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        pass


class CHS3SyncConnectionExecutorTestBase(
    CHS3SyncAsyncConnectionExecutorTestBase[FILE_CONN_TV],
    DefaultSyncConnectionExecutorTestSuite[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultSyncConnectionExecutorTestSuite.test_type_recognition: "Not implemented",
            DefaultSyncConnectionExecutorTestSuite.test_get_table_names: "Not implemented",
        },
    )


class CHS3AsyncConnectionExecutorTestBase(
    CHS3SyncAsyncConnectionExecutorTestBase[FILE_CONN_TV],
    DefaultAsyncConnectionExecutorTestSuite[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    async def test_select_data(
        self,
        sample_table: DbTable,
        saved_connection: FILE_CONN_TV,
        async_connection_executor: AsyncConnExecutorBase,
        s3_settings: S3Settings,
        sample_s3_file: str,
    ) -> None:
        schema_line = self._get_s3_func_schema_for_table(sample_table)
        s3_tbl_func = s3_tbl_func_maker(s3_settings)(
            for_="dba",
            conn_dto=saved_connection.get_conn_dto(),
            filename=sample_s3_file,
            file_fmt="Native",
            schema_line=schema_line,
        )
        file_columns = [sa.column(col_desc.split(" ")[0]) for col_desc in schema_line.split(", ")]
        # ^ "col1 String, col2 Int32, col3 Date32" -> [col1, col2, col3]

        n_rows = 3
        result = await async_connection_executor.execute(
            ConnExecutorQuery(
                query=sa.select(columns=file_columns)
                .select_from(sa.text(s3_tbl_func))
                .order_by(file_columns[0])
                .limit(n_rows),
                chunk_size=6,
            )
        )
        rows = await result.get_all()
        assert len(rows) == n_rows

    @pytest.mark.asyncio
    async def test_cast_row_to_output(
        self,
        sample_table: DbTable,
        saved_connection: FILE_CONN_TV,
        async_connection_executor: AsyncConnExecutorBase,
        s3_settings: S3Settings,
        sample_s3_file: str,
    ) -> None:
        schema_line = self._get_s3_func_schema_for_table(sample_table)
        s3_tbl_func = s3_tbl_func_maker(s3_settings)(
            for_="dba",
            conn_dto=saved_connection.get_conn_dto(),
            filename=sample_s3_file,
            file_fmt="Native",
            schema_line=schema_line,
        )

        result = await async_connection_executor.execute(
            ConnExecutorQuery(
                sa.select(columns=[sa.literal(1), sa.literal(2), sa.literal(3)])
                .select_from(sa.text(s3_tbl_func))
                .limit(1),
                user_types=[
                    UserDataType.boolean,
                    UserDataType.float,
                    UserDataType.integer,
                ],
            )
        )
        rows = await result.get_all()
        assert rows == [(True, 2.0, 3)], rows
