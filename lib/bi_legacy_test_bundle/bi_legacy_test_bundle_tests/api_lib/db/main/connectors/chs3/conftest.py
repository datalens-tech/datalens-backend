from __future__ import annotations

import datetime
import math
from typing import Optional
import uuid

import attr
import clickhouse_sqlalchemy.types as ch_types
import pytest

from bi_legacy_test_bundle_tests.api_lib.utils import make_connection_get_id
from dl_connector_bundle_chs3.chs3_base.core.testing.utils import create_s3_native_from_ch_table
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_constants.enums import (
    BIType,
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core.db import SchemaColumn
from dl_core.services_registry.file_uploader_client_factory import (
    FileSourceDesc,
    FileUploaderClient,
    FileUploaderClientFactory,
    GSheetsFileSourceDesc,
    SourceInternalParams,
    SourcePreview,
)
from dl_core_testing.database import (
    C,
    make_sample_data,
    make_table,
)


@attr.s
class FileUploaderClientMockup(FileUploaderClient):
    # In reality we already receive json-serialized values for the preview from the source,
    # so tune the value generators here as well
    _VALUE_GENERATORS = {
        BIType.date: lambda rn, ts, **kwargs: (ts.date() + datetime.timedelta(days=rn)).isoformat(),
        BIType.datetime: lambda rn, ts, **kwargs: (ts + datetime.timedelta(days=rn / math.pi)).isoformat(),
        BIType.genericdatetime: lambda rn, ts, **kwargs: (ts + datetime.timedelta(days=rn / math.pi)).isoformat(),
    }

    async def get_preview(self, src: FileSourceDesc) -> SourcePreview:
        if src.raw_schema is None:
            return SourcePreview(source_id=src.source_id, preview=[])
        cols = [
            C(sch_col.name, sch_col.user_type, sch_col.nullable, vg=self._VALUE_GENERATORS.get(sch_col.user_type))
            for sch_col in src.raw_schema
        ]
        preview_dicts = make_sample_data(cols, rows=20)
        preview = [[row[c.name] for c in cols] for row in preview_dicts]
        return SourcePreview(source_id=src.source_id, preview=preview)

    async def get_internal_params(self, src: FileSourceDesc) -> SourceInternalParams:
        return SourceInternalParams(
            preview_id=src.preview_id,
            raw_schema=[
                SchemaColumn(title="string_value", name="string_value", user_type=BIType.string),
                SchemaColumn(title="n_string_value", name="n_string_value", user_type=BIType.string),
                SchemaColumn(title="int_value", name="int_value", user_type=BIType.integer),
                SchemaColumn(title="n_int_value", name="n_int_value", user_type=BIType.integer),
                SchemaColumn(title="float_value", name="float_value", user_type=BIType.float),
                SchemaColumn(title="datetime_value", name="datetime_value", user_type=BIType.datetime),
                SchemaColumn(title="n_datetime_value", name="n_datetime_value", user_type=BIType.datetime),
                SchemaColumn(title="date_value", name="date_value", user_type=BIType.date),
                SchemaColumn(title="boolean_value", name="boolean_value", user_type=BIType.boolean),
                SchemaColumn(title="uuid_value", name="uuid_value", user_type=BIType.uuid),
            ],
        )

    async def update_connection_data_internal(
        self,
        conn_id: str,
        sources: list[GSheetsFileSourceDesc],
        authorized: bool,
        tenant_id: Optional[str],
    ) -> None:
        pass  # mocked below


@pytest.fixture(scope="function", autouse=True)
def mockup_file_uploader_client(monkeypatch, default_async_usm_per_test) -> None:
    async def mocked_update_connection_data_internal(
        self,
        conn_id: str,
        sources: list[GSheetsFileSourceDesc],
        authorized: bool,
        tenant_id: Optional[str],
    ) -> None:
        usm = default_async_usm_per_test
        conn: GSheetsFileS3Connection = await usm.get_by_id(conn_id, expected_type=GSheetsFileS3Connection)
        for src in conn.data.sources:
            src.data_updated_at = datetime.datetime.now(datetime.timezone.utc)
        await usm.save(conn)

    monkeypatch.setattr(
        FileUploaderClientMockup,
        "update_connection_data_internal",
        mocked_update_connection_data_internal,
    )
    monkeypatch.setattr(FileUploaderClientFactory, "_file_uploader_client_cls", FileUploaderClientMockup)


@pytest.fixture(scope="function")
def file_connection_params():
    return {
        "name": f"file conn {uuid.uuid4()}",
        "dir_path": "unit_tests/connections",
        "type": "file",
        "sources": [
            {
                "id": "source_1_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 1 - Sheet 1",
                "column_types": [
                    {"name": "field1", "user_type": "string"},
                    {"name": "field2", "user_type": "date"},
                ],
            },
            {
                "id": "source_2_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 1 - Sheet 2",
                "column_types": [
                    {"name": "field1", "user_type": "string"},
                    {"name": "field2", "user_type": "integer"},
                    {"name": "field3", "user_type": "date"},
                ],
            },
            {
                "id": "source_3_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 2 - Sheet 1",
                "column_types": [
                    {"name": "string_value", "user_type": "string"},
                    {"name": "n_string_value", "user_type": "string"},
                    {"name": "int_value", "user_type": "integer"},
                    {"name": "n_int_value", "user_type": "integer"},
                    {"name": "float_value", "user_type": "float"},
                    {"name": "datetime_value", "user_type": "datetime"},
                    {"name": "n_datetime_value", "user_type": "datetime"},
                    {"name": "date_value", "user_type": "date"},
                    {"name": "boolean_value", "user_type": "boolean"},
                    {"name": "uuid_value", "user_type": "uuid"},
                ],
            },
            {
                "id": "source_4_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 3 - Sheet 1",
                "column_types": [
                    {"name": "date32_val_1", "user_type": "date"},
                    {"name": "date32_val_2", "user_type": "date"},
                ],
            },
        ],
    }


@pytest.fixture(scope="function")
def gsheets_v2_connection_params(file_connection_params):
    return {
        "name": f"gsheets_v2 conn {uuid.uuid4()}",
        "dir_path": "unit_tests/connections",
        "type": "gsheets_v2",
        "refresh_enabled": True,
        "sources": [
            {
                "id": "source_1_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 1 - Sheet 1",
            },
            {
                "id": "source_2_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 1 - Sheet 2",
            },
            {
                "id": "source_3_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 2 - Sheet 1",
            },
            {
                "id": "source_4_id",
                "file_id": str(uuid.uuid4()),
                "title": "My File 3 - Sheet 1",
            },
        ],
    }


@pytest.fixture(scope="function")
def file_connection_id(app, client, request, file_connection_params):
    return make_connection_get_id(
        connection_params=file_connection_params,
        client=client,
        request=request,
    )


@pytest.fixture(scope="function")
def gsheets_v2_connection_id(app, client, request, gsheets_v2_connection_params):
    return make_connection_get_id(
        connection_params=gsheets_v2_connection_params,
        client=client,
        request=request,
    )


@pytest.fixture(scope="function")
def clickhouse_table_with_date32(clickhouse_db, request):
    return make_table(
        clickhouse_db,
        columns=[
            C(name="date32_val_1", user_type=BIType.date, nullable=True, sa_type=ch_types.Date32),
            C(name="date32_val_2", user_type=BIType.date, nullable=True, sa_type=ch_types.Date32),
        ],
        data=[
            {"date32_val_1": datetime.date(1963, 4, 2), "date32_val_2": datetime.date(1963, 4, 1)},
            {"date32_val_2": datetime.date(1963, 4, 1), "date32_val_1": datetime.date(1963, 4, 2)},
        ],
    )


@pytest.fixture()
async def s3_native_from_ch_with_date32_table(s3_client, s3_bucket, s3_settings, clickhouse_table_with_date32):
    filename = "with_date32.native"
    tbl_schema = "date32_val_1 Nullable(Date32), date32_val_2 Nullable(Date32)"  # TODO: update DbTable to serve some sort of schema
    create_s3_native_from_ch_table(
        filename, s3_bucket, s3_settings, clickhouse_table_with_date32, tbl_schema, double_data=True
    )

    yield filename

    await s3_client.delete_object(Bucket=s3_bucket, Key=filename)


@pytest.fixture()
async def s3_native_from_ch_table(s3_client, s3_bucket, s3_settings, clickhouse_table):
    filename = "my_file_2_1.native"
    tbl_schema = (
        "string_value Nullable(String), n_string_value Nullable(String), int_value Nullable(Int64), "
        "n_int_value Nullable(Int64), float_value Nullable(Float64), datetime_value Nullable(DateTime64(9)), "
        "n_datetime_value Nullable(DateTime64(9)), date_value Nullable(Date32), boolean_value Nullable(Bool), "
        "uuid_value Nullable(UUID)"
    )  # TODO: update DbTable to serve some sort of schema
    create_s3_native_from_ch_table(filename, s3_bucket, s3_settings, clickhouse_table, tbl_schema, double_data=True)

    yield filename

    await s3_client.delete_object(Bucket=s3_bucket, Key=filename)


def add_raw_schema_to_saved_connection_return_id(us_manager, conn_id):
    conn = us_manager.get_by_id(conn_id, BaseFileS3Connection)
    assert isinstance(conn, BaseFileS3Connection)
    conn.update_data_source(
        id="source_1_id",
        role=DataSourceRole.origin,
        status=FileProcessingStatus.ready,
        raw_schema=[
            SchemaColumn(title="field1", name="string", user_type=BIType.string),
            SchemaColumn(title="field2", name="date", user_type=BIType.date),
        ],
        s3_filename="my_file_1_1.native",
    )
    conn.update_data_source(
        id="source_2_id",
        role=DataSourceRole.origin,
        remove_raw_schema=True,
        status=FileProcessingStatus.in_progress,
        s3_filename="my_file_1_2.native",
    )
    conn.update_data_source(
        id="source_3_id",
        role=DataSourceRole.origin,
        status=FileProcessingStatus.ready,
        raw_schema=[
            SchemaColumn(title="string_value", name="string_value", user_type=BIType.string),
            SchemaColumn(title="n_string_value", name="n_string_value", user_type=BIType.string),
            SchemaColumn(title="int_value", name="int_value", user_type=BIType.integer),
            SchemaColumn(title="n_int_value", name="n_int_value", user_type=BIType.integer),
            SchemaColumn(title="float_value", name="float_value", user_type=BIType.float),
            SchemaColumn(title="datetime_value", name="datetime_value", user_type=BIType.genericdatetime),
            SchemaColumn(title="n_datetime_value", name="n_datetime_value", user_type=BIType.genericdatetime),
            SchemaColumn(title="date_value", name="date_value", user_type=BIType.date),
            SchemaColumn(title="boolean_value", name="boolean_value", user_type=BIType.boolean),
            SchemaColumn(title="uuid_value", name="uuid_value", user_type=BIType.uuid),
        ],
        s3_filename="my_file_2_1.native",
    )
    conn.update_data_source(
        id="source_4_id",
        role=DataSourceRole.origin,
        status=FileProcessingStatus.ready,
        raw_schema=[
            SchemaColumn(title="date32_val_1", name="date32_val_1", user_type=BIType.date),
            SchemaColumn(title="date32_val_2", name="date32_val_2", user_type=BIType.date),
        ],
        s3_filename="with_date32.native",
    )
    us_manager.save(conn)
    return conn.uuid


@pytest.fixture(scope="function")
def file_connection_with_raw_schema_id(default_sync_usm, file_connection_id):
    return add_raw_schema_to_saved_connection_return_id(default_sync_usm, file_connection_id)


@pytest.fixture(scope="function")
def gsheets_v2_connection_with_raw_schema_id(default_sync_usm, gsheets_v2_connection_id):
    return add_raw_schema_to_saved_connection_return_id(default_sync_usm, gsheets_v2_connection_id)
