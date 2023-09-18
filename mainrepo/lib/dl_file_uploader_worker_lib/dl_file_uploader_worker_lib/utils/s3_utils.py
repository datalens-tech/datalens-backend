import io
import json
from typing import (
    Iterator,
    NamedTuple,
    Optional,
)

import botocore.client
from clickhouse_sqlalchemy.quoting import Quoter

from dl_core.db import (
    SchemaColumn,
    get_type_transformer,
)
from dl_core.raw_data_streaming.stream import SimpleDataStream
from dl_file_uploader_lib.data_sink.json_each_row import S3JsonEachRowFileDataSink
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.models import CSVFileSettings
from dl_file_uploader_lib.redis_model.models.models import (
    CSVFileSourceSettings,
    FileSettings,
    FileSourceSettings,
    SpreadsheetFileSourceSettings,
)
from dl_file_uploader_worker_lib.utils.parsing_utils import get_csv_raw_data_iterator

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import create_column_sql


def make_s3_table_func_sql_source(
    conn: BaseFileS3Connection,
    source_id: str,
    bucket: str,
    filename: str,
    file_fmt: str,
    for_debug: bool = False,
    raw_schema_override: Optional[list[SchemaColumn]] = None,
) -> str:
    source_raw_schema: list[SchemaColumn] = conn.get_file_source_by_id(source_id).raw_schema or []
    q = Quoter().quote_str

    s3_path = q("{}/{}/{}".format(conn.s3_endpoint.strip("/"), bucket.strip("/"), filename))
    file_fmt = q(file_fmt)

    dialect = AsyncFileS3Adapter.get_dialect()
    raw_schema = raw_schema_override if raw_schema_override is not None else source_raw_schema
    type_transformer = get_type_transformer(conn.conn_type)
    schema_str = q(", ".join(create_column_sql(dialect, col, type_transformer) for col in raw_schema))

    if for_debug:
        access_key_id, secret_access_key = "<hidden>", "<hidden>"
    else:
        access_key_id = q(conn.s3_access_key_id)
        secret_access_key = q(conn.s3_secret_access_key)
    return f"s3({s3_path}, {access_key_id}, {secret_access_key}, {file_fmt}, {schema_str})"


class S3Object(NamedTuple):
    bucket: str
    key: str


def copy_from_s3_to_s3(
    s3_sync_cli: botocore.client.BaseClient,
    src_file: S3Object,
    dst_file: S3Object,
    file_type: FileType,
    file_settings: Optional[FileSettings],
    file_source_settings: FileSourceSettings,
    raw_schema: list[SchemaColumn],
) -> None:
    s3_sync_resp = s3_sync_cli.get_object(Bucket=src_file.bucket, Key=src_file.key)
    s3_data_stream = s3_sync_resp["Body"]

    def spreadsheet_data_iter() -> Iterator[dict]:
        fieldnames = tuple(sch.name for sch in raw_schema)
        text_io_wrapper = io.TextIOWrapper(s3_data_stream, encoding="utf-8", newline="")
        assert isinstance(file_source_settings, SpreadsheetFileSourceSettings)
        if file_source_settings.first_line_is_header:
            next(text_io_wrapper)
        for line in text_io_wrapper:
            line_data = json.loads(line)
            yield dict(zip(fieldnames, line_data))

    if file_type == FileType.csv:
        assert isinstance(file_settings, CSVFileSettings)
        assert isinstance(file_source_settings, CSVFileSourceSettings)
        data_iter = get_csv_raw_data_iterator(
            binary_data_stream=s3_data_stream,
            encoding=file_settings.encoding,
            dialect=file_settings.dialect,
            first_line_is_header=file_source_settings.first_line_is_header,
            raw_schema=raw_schema,
        )
    else:
        data_iter = spreadsheet_data_iter()

    data_stream = SimpleDataStream(
        data_iter=data_iter,
        rows_to_copy=None,  # TODO
    )
    with S3JsonEachRowFileDataSink(
        bi_schema=raw_schema,
        s3=s3_sync_cli,
        s3_key=dst_file.key,
        bucket_name=dst_file.bucket,
    ) as data_sink:
        data_sink.dump_data_stream(data_stream)
