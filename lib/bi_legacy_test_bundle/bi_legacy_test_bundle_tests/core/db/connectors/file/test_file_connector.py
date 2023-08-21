from __future__ import annotations

import logging

from bi_constants.enums import BIType, FileProcessingStatus

from bi_core.base_models import DefaultConnectionRef
from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE, SOURCE_TYPE_FILE_S3_TABLE
from bi_connector_bundle_chs3.file.core.data_source_spec import FileS3DataSourceSpec
from bi_connector_bundle_chs3.file.core.data_source import FileS3DataSource
from bi_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from bi_core.db import SchemaColumn
from bi_core.db.native_type import ClickHouseNativeType

LOGGER = logging.getLogger(__name__)


def test_file_connection_load_hardcoded_fields(
        default_sync_usm,
        saved_file_connection,
        clickhouse_db,
):
    conn = default_sync_usm.get_by_id(saved_file_connection.uuid)

    hardcoded_dto_fields_list = (
        'host', 'port', 'username', 'password',
        's3_endpoint', 'access_key_id', 'secret_access_key', 'bucket',
    )
    conn_dto = conn.get_conn_dto()
    for f_name in hardcoded_dto_fields_list:
        assert conn._us_resp['data'].get(f_name) is None
        # assert getattr(conn.data, f_name) is None
        assert getattr(conn_dto, f_name) is not None

    hardcoded_properties_list = (
        'allow_public_usage',
    )
    for f_name in hardcoded_properties_list:
        assert getattr(conn, f_name) is not None


def test_build_from_clause(default_sync_usm, saved_file_connection):
    conn: FileS3Connection = default_sync_usm.get_by_id(saved_file_connection.uuid)
    sources = conn.data.sources
    conn_dto = conn.get_conn_dto()
    raw_schema = [
        SchemaColumn(name='c1', native_type=ClickHouseNativeType(conn_type=CONNECTION_TYPE_FILE, name='Int64'), user_type=BIType.integer),
        SchemaColumn(name='c2', native_type=ClickHouseNativeType(conn_type=CONNECTION_TYPE_FILE, name='String'), user_type=BIType.string),
        SchemaColumn(name='c3', native_type=ClickHouseNativeType(conn_type=CONNECTION_TYPE_FILE, name='Int64'), user_type=BIType.integer),
    ]
    default_sync_usm.ensure_entry_preloaded(DefaultConnectionRef(conn_id=saved_file_connection.uuid))
    dsrc = FileS3DataSource(
        us_entry_buffer=default_sync_usm.get_entry_buffer(),
        spec=FileS3DataSourceSpec(
            source_type=SOURCE_TYPE_FILE_S3_TABLE,
            connection_ref=DefaultConnectionRef(conn_id=conn.uuid),
            db_version='1.1.2',
            db_name='some_db',
            table_name=sources[0].s3_filename,
            data_dump_id=None,
            raw_schema=raw_schema,
            s3_endpoint=conn_dto.s3_endpoint,
            bucket=conn_dto.bucket,
            status=FileProcessingStatus.ready,
            origin_source_id=sources[0].id,
        ),
    )

    query_from = dsrc.get_sql_source().compile(compile_kwargs={"literal_binds": True}).string

    replace_secret = conn_dto.replace_secret

    expected = f"s3('{conn_dto.s3_endpoint}/{conn_dto.bucket}/{sources[0].s3_filename}', " \
               f"'key_id_{replace_secret}', 'secret_key_{replace_secret}', 'Native', " \
               f"'c1 Nullable(Int64), c2 Nullable(String), c3 Nullable(Int64)')"
    assert query_from == expected
