from __future__ import annotations

import datetime
from typing import Optional
import uuid

import pytest
import sqlalchemy as sa

from bi_legacy_test_bundle_tests.core.common_ce import (
    ErrorTestSet,
    SelectDataTestSet,
)
from bi_legacy_test_bundle_tests.core.common_ce_ch import BaseClickHouseTestSet
from dl_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO
from dl_connector_bundle_chs3.chs3_gsheets.core.connection_executors import GSheetsFileS3AsyncAdapterConnExecutor
from dl_connector_bundle_chs3.file.core.connection_executors import FileS3AsyncAdapterConnExecutor
from dl_constants.enums import BIType
from dl_core import exc
from dl_core.connection_executors import ConnExecutorQuery
from dl_core_testing.database import (
    C,
    make_table,
)


class BaseTestFileS3AsyncAdapterExecutor(BaseClickHouseTestSet):
    conn_dto_cls = BaseFileS3ConnDTO

    @pytest.fixture()
    def conn_dto(self, db, s3_settings, s3_bucket) -> BaseFileS3ConnDTO:
        return self.conn_dto_cls(
            conn_id=None,
            protocol="http",
            host=db.url.host,
            port=db.url.port,
            username=db.url.username,
            password=db.url.password,
            multihosts=db.get_conn_hosts(),
            s3_endpoint="http://s3-storage:8000",  # because we are communicating with another container (CH <-> S3)
            access_key_id=s3_settings.ACCESS_KEY_ID,
            secret_access_key=s3_settings.SECRET_ACCESS_KEY,
            bucket=s3_bucket,
            replace_secret="SECRET_PLACEHOLDER",
        )

    @pytest.fixture()
    def s3_tbl_func_local(self, s3_tbl_func, conn_dto):
        def table_function(
            for_: str,
            filename: str = "test_data.native",
            file_fmt: str = "Native",
            conn_dto_: Optional[BaseFileS3ConnDTO] = None,
            schema_line="str_val String",
        ) -> str:
            return s3_tbl_func(
                for_=for_,
                conn_dto=conn_dto_ or conn_dto,
                filename=filename,
                file_fmt=file_fmt,
                schema_line=schema_line,
            )

        return table_function

    @pytest.fixture()
    def tbl(self, db):
        tbl = make_table(
            db,
            rows=10,
            columns=[
                C("str_val", BIType.string, vg=lambda rn, **kwargs: str(rn)),
            ],
        )

        yield tbl

        db.drop_table(tbl.table)

    @pytest.fixture()
    async def s3_native_from_ch_table(self, db, tbl, s3_tbl_func_local, s3_client, s3_bucket):
        filename = f"test_data_{uuid.uuid4()}.native"
        s3_tbl_func_for_db = s3_tbl_func_local(for_="db", filename=filename)
        db.execute(f"INSERT INTO FUNCTION {s3_tbl_func_for_db} SELECT * FROM {tbl.db.quote(tbl.name)}")

        yield filename

        await s3_client.delete_object(Bucket=s3_bucket, Key=filename)

    async def test_date32(self, executor, s3_tbl_func_local, s3_native_from_ch_table):
        query = executor.execute(
            query=ConnExecutorQuery(
                query="SELECT toDate32('1931-01-01') " "AS res_0\n " "ORDER BY res_0\n " "LIMIT 0, 100",
                trusted_query=True,
                chunk_size=6,
            ),
        )
        result = await query
        rows = await result.get_all()
        assert rows == [(datetime.date(1931, 1, 1),)]

    @pytest.fixture(params=["sqla", "plain_text", "sqla_const", "plain_text_const"])
    async def select_data_test_set(self, s3_native_from_ch_table, s3_tbl_func_local, request) -> SelectDataTestSet:
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba", filename=s3_native_from_ch_table)
        if request.param == "plain_text":
            return SelectDataTestSet(
                query=ConnExecutorQuery(
                    query=f"SELECT * FROM {s3_tbl_func_for_dba} ORDER BY str_val",
                    trusted_query=True,
                    chunk_size=6,
                ),
                expected_data=[(str(i),) for i in range(10)],
            )
        elif request.param == "sqla":
            return SelectDataTestSet(
                query=ConnExecutorQuery(
                    query=sa.select(columns=[sa.column("str_val")])
                    .select_from(sa.text(s3_tbl_func_for_dba))
                    .order_by(sa.column("str_val")),
                    chunk_size=6,
                ),
                expected_data=[(str(i),) for i in range(10)],
            )
        elif request.param == "plain_text_const":
            return SelectDataTestSet(
                query=ConnExecutorQuery(
                    query="SELECT 'FOO' AS res_0\nGROUP BY res_0\n  ORDER BY res_0 ASC NULLS FIRST\nLIMIT 0, 100",
                    trusted_query=True,
                    chunk_size=6,
                ),
                expected_data=[("FOO",)],
            )
        elif request.param == "sqla_const":
            return SelectDataTestSet(
                query=ConnExecutorQuery(
                    query=sa.select(columns=[sa.literal("FOO")]),
                    chunk_size=6,
                ),
                expected_data=[("FOO",)],
            )
        else:
            raise ValueError(f"Unknown request param: {request.param}")

    @pytest.fixture()
    def error_test_set(self, s3_tbl_func_local) -> ErrorTestSet:
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba", filename="not_existing_file.native")
        return ErrorTestSet(
            query=ConnExecutorQuery(
                f"SELECT * FROM {s3_tbl_func_for_dba} ORDER BY str_val",
                trusted_query=True,
            ),
            expected_err_cls=exc.DatabaseQueryError,
        )

    @pytest.mark.skip()
    async def test_sa_mod(self, executor):
        pass

    @pytest.mark.skip()
    async def test_inf(self, executor):
        pass

    @pytest.mark.skip()
    async def test_cast_row_to_output(self, executor):
        pass

    @pytest.mark.skip()
    def test_type_discovery_sync(self, sync_exec_wrapper, all_supported_types_test_case):
        pass

    @pytest.mark.skip()
    def test_indexes_discovery(self, sync_exec_wrapper, index_test_case):
        pass

    @pytest.mark.skip()
    async def test_get_db_version(self, executor):
        pass

    @pytest.mark.skip()
    def test_get_db_version_sync(self, sync_exec_wrapper):
        pass

    @pytest.mark.skip()
    async def test_get_table_names(self, executor, get_table_names_test_case):
        pass

    @pytest.mark.skip()
    def test_get_table_names_sync(self, sync_exec_wrapper, get_table_names_test_case):
        pass

    @pytest.mark.skip()
    async def test_table_exists(self, executor, is_table_exists_test_case):
        pass

    @pytest.mark.skip()
    def test_table_exists_sync(self, sync_exec_wrapper, is_table_exists_test_case):
        pass


class TestFileS3AsyncAdapterExecutor(BaseTestFileS3AsyncAdapterExecutor):
    executor_cls = FileS3AsyncAdapterConnExecutor


class TestGSheetsFileS3AsyncAdapterExecutor(BaseTestFileS3AsyncAdapterExecutor):
    executor_cls = GSheetsFileS3AsyncAdapterConnExecutor
