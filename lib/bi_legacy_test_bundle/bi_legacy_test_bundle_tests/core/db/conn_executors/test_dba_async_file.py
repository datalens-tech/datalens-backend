from __future__ import annotations

from typing import (
    NamedTuple,
    Optional,
)

from aiohttp import ClientTimeout
import attr
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_connector_bundle_chs3.chs3_base.core.adapter import BaseAsyncFileS3Adapter
from dl_connector_bundle_chs3.chs3_base.core.target_dto import BaseFileS3ConnTargetDTO
from dl_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter
from dl_constants.enums import BIType
from dl_core import exc
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core_testing.database import (
    C,
    DbTable,
    make_table,
)


def get_smaller_session_timeout(self) -> ClientTimeout:
    return ClientTimeout(total=0.2)


class TestAsyncFileS3Adapter:
    class RawSchema(NamedTuple):
        name: str
        ch_type: str

    @pytest.fixture()
    def raw_schema(self) -> list[TestAsyncFileS3Adapter.RawSchema]:
        return [
            TestAsyncFileS3Adapter.RawSchema("str_val", "String"),
            TestAsyncFileS3Adapter.RawSchema("int_val", "Int64"),
        ]

    @pytest.fixture()
    def schema_line(self, raw_schema):
        return ", ".join("{} {}".format(col.name, col.ch_type) for col in raw_schema)

    @pytest.fixture()
    def db(self, clickhouse_db):
        return clickhouse_db

    @pytest.fixture()
    def tbl(self, db, raw_schema) -> DbTable:
        tbl = make_table(
            db,
            rows=1000,
            columns=[
                C(raw_schema[0].name, BIType.string, vg=lambda rn, **kwargs: str(rn)),
                C(raw_schema[1].name, BIType.integer, vg=lambda rn, **kwargs: rn),
            ],
        )
        yield tbl
        db.drop_table(tbl.table)

    @pytest.fixture()
    def conn_target_dto(self, db, s3_settings, s3_bucket):
        return BaseFileS3ConnTargetDTO(
            conn_id=None,
            host=db.url.host,
            port=db.url.port,
            db_name=db.url.database,
            username=db.url.username,
            password=db.url.password,
            protocol="http",
            disable_value_processing=False,
            s3_endpoint="http://s3-storage:8000",  # because we are communicating with another container (CH <-> S3)
            bucket=s3_bucket,
            access_key_id=s3_settings.ACCESS_KEY_ID,
            secret_access_key=s3_settings.SECRET_ACCESS_KEY,
            replace_secret="SECRET_PLACEHOLDER",
        )

    @pytest.fixture()
    def s3_tbl_func_local(self, s3_tbl_func, conn_target_dto, schema_line):
        def table_function(
            for_: str,
            filename: str = "test_data.native",
            file_fmt: str = "Native",
            conn_dto_: Optional[BaseFileS3ConnTargetDTO] = None,
        ) -> str:
            return s3_tbl_func(
                for_=for_,
                conn_dto=conn_dto_ or conn_target_dto,
                filename=filename,
                file_fmt=file_fmt,
                schema_line=schema_line,
            )

        return table_function

    @pytest.fixture()
    async def s3_native_from_ch_table(self, db, tbl, conn_target_dto, s3_client, s3_tbl_func_local) -> str:
        filename = "test_data.native"
        s3_tbl_func_for_db = s3_tbl_func_local(for_="db")
        db.execute(f"INSERT INTO FUNCTION {s3_tbl_func_for_db} SELECT * FROM {tbl.db.quote(tbl.name)}")
        yield filename
        await s3_client.delete_object(Bucket=conn_target_dto.bucket, Key=filename)

    @pytest.fixture()
    async def s3_native_from_s3_csv(self, db, conn_target_dto, s3_client, s3_csv, s3_tbl_func_local) -> str:
        filename = "test_data.native"
        s3_tbl_func_for_db_file = s3_tbl_func_local(for_="db", filename=filename, file_fmt="Native")
        s3_tbl_func_for_db_csv = s3_tbl_func_local(for_="db", filename=s3_csv, file_fmt="CSV")
        db.execute(f"INSERT INTO FUNCTION {s3_tbl_func_for_db_file} SELECT * FROM {s3_tbl_func_for_db_csv}")
        yield filename
        await s3_client.delete_object(Bucket=conn_target_dto.bucket, Key=filename)

    @pytest.mark.asyncio
    async def test_timeout_ch(self, loop, monkeypatch, conn_target_dto, s3_tbl_func_local):
        monkeypatch.setattr(BaseAsyncFileS3Adapter, "get_session_timeout", get_smaller_session_timeout)
        rci = RequestContextInfo.create_empty()
        dba = AsyncFileS3Adapter(
            req_ctx_info=rci,
            target_dto=attr.evolve(conn_target_dto, port="63545"),
            default_chunk_size=10,
        )
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba")
        with pytest.raises(exc.SourceConnectError):
            await dba.execute(
                DBAdapterQuery(
                    query=f"SELECT * FROM {s3_tbl_func_for_dba}",
                    trusted_query=True,
                )
            )

    @pytest.mark.parametrize(
        "pass_db_query_to_user,expected_query",
        (
            (None, None),
            (False, None),
            (True, "select 1"),
        ),
    )
    @pytest.mark.asyncio
    async def test_pass_db_query_to_user(
        self,
        loop,
        conn_target_dto,
        s3_tbl_func_local,
        pass_db_query_to_user,
        expected_query,
    ):
        target_dto = conn_target_dto.clone(port="63545")
        if pass_db_query_to_user is not None:
            target_dto = target_dto.clone(pass_db_query_to_user=pass_db_query_to_user)

        rci = RequestContextInfo.create_empty()
        dba = AsyncFileS3Adapter(
            req_ctx_info=rci,
            target_dto=target_dto,
            default_chunk_size=10,
        )
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba")
        with pytest.raises(exc.SourceConnectError) as exception_info:
            await dba.execute(
                DBAdapterQuery(
                    query=f"SELECT * FROM {s3_tbl_func_for_dba}",
                    trusted_query=True,
                    debug_compiled_query="select 1",
                )
            )

            assert exception_info.value.query == expected_query

    @pytest.mark.asyncio
    async def test_timeout_s3(self, loop, monkeypatch, conn_target_dto, s3_native_from_ch_table, s3_tbl_func_local):
        monkeypatch.setattr(BaseAsyncFileS3Adapter, "get_session_timeout", get_smaller_session_timeout)
        rci = RequestContextInfo.create_empty()
        conn_target_dto = attr.evolve(conn_target_dto, s3_endpoint="http://s3-storage:63545")
        dba = AsyncFileS3Adapter(
            req_ctx_info=rci,
            target_dto=conn_target_dto,
            default_chunk_size=10,
        )
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba", conn_dto_=conn_target_dto, filename=s3_native_from_ch_table)
        with pytest.raises(exc.DatabaseQueryError):  # TODO CONSIDER: narrower exception when s3 is unreachable?
            await dba.execute(
                DBAdapterQuery(
                    query=f"SELECT * FROM {s3_tbl_func_for_dba}",
                    trusted_query=True,
                )
            )

    @pytest.mark.asyncio
    async def test_native_from_clickhouse(self, loop, conn_target_dto, s3_native_from_ch_table, s3_tbl_func_local):
        rci = RequestContextInfo.create_empty()
        dba = AsyncFileS3Adapter(
            req_ctx_info=rci,
            target_dto=conn_target_dto,
            default_chunk_size=10,
        )
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba", filename=s3_native_from_ch_table)
        try:
            res = await dba.execute(
                DBAdapterQuery(
                    query=f"SELECT * FROM {s3_tbl_func_for_dba}",
                    trusted_query=True,
                )
            )

            all_data = []
            async for chunk in res.raw_chunk_generator:
                all_data.extend(chunk)
            assert len(all_data) == 1000
        finally:
            await dba.close()

    @pytest.mark.asyncio
    async def test_native_from_csv(self, loop, conn_target_dto, s3_native_from_s3_csv, s3_tbl_func_local):
        rci = RequestContextInfo.create_empty()
        dba = AsyncFileS3Adapter(
            req_ctx_info=rci,
            target_dto=conn_target_dto,
            default_chunk_size=10,
        )
        s3_tbl_func_for_dba = s3_tbl_func_local(for_="dba", filename=s3_native_from_s3_csv)
        try:
            res = await dba.execute(
                DBAdapterQuery(
                    query=f"SELECT * FROM {s3_tbl_func_for_dba}",
                    trusted_query=True,
                )
            )

            all_data = []
            async for chunk in res.raw_chunk_generator:
                all_data.extend(chunk)
            assert len(all_data) == 3
        finally:
            await dba.close()
