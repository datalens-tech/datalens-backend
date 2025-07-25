from __future__ import annotations

import io
import json
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Iterator,
    Optional,
)

from clickhouse_sqlalchemy.quoting import Quoter

from dl_constants.enums import ConnectionType
from dl_core.db import SchemaColumn
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.models import CSVFileSettings
from dl_file_uploader_lib.redis_model.models.models import (
    CSVFileSourceSettings,
    FileSettings,
    FileSourceSettings,
    SpreadsheetFileSourceSettings,
)
from dl_file_uploader_worker_lib.utils.parsing_utils import get_csv_raw_data_iterator
from dl_s3.data_sink import S3FileDataSink
from dl_s3.stream import SimpleDataStream
from dl_s3.utils import S3Object
from dl_type_transformer.type_transformer import get_type_transformer

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import create_column_sql


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as SyncS3Client
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


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


class S3JsonEachRowFileDataSink(S3FileDataSink[SimpleDataStream, dict]):
    conn_type: ConnectionType = CONNECTION_TYPE_FILE

    def __init__(
        self,
        bi_schema: list[SchemaColumn],
        s3: SyncS3Client,
        s3_key: str,
        bucket_name: str,
    ) -> None:
        super().__init__(s3=s3, s3_key=s3_key, bucket_name=bucket_name)

        self._bi_schema = bi_schema
        self._tt = get_type_transformer(self.conn_type)

    def _process_row(self, row_data: dict) -> bytes:
        cast_row_data = [self._tt.cast_for_input(row_data[col.name], user_t=col.user_type) for col in self._bi_schema]
        return json.dumps(cast_row_data).encode("utf-8")


def copy_from_s3_to_s3(
    s3_sync_cli: SyncS3Client,
    src_file: S3Object,
    dst_file: S3Object,
    file_type: FileType,
    file_settings: Optional[FileSettings],
    file_source_settings: FileSourceSettings,
    raw_schema: list[SchemaColumn],
) -> None:
    s3_sync_resp = s3_sync_cli.get_object(Bucket=src_file.bucket, Key=src_file.key)
    s3_data_stream: BinaryIO = s3_sync_resp["Body"]

    def spreadsheet_data_iter() -> Iterator[dict]:
        fieldnames = tuple(sch.name for sch in raw_schema)
        text_io_wrapper = io.TextIOWrapper(s3_data_stream, encoding="utf-8", newline="")
        assert isinstance(file_source_settings, SpreadsheetFileSourceSettings)
        if file_source_settings.first_line_is_header:
            next(text_io_wrapper)
        for line in text_io_wrapper:
            line_data = json.loads(line)
            yield dict(zip(fieldnames, line_data, strict=True))

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


async def delete_prefix_objects(
    s3_client: AsyncS3Client,
    bucket: str,
    prefix: str,
) -> None:
    """Delete every object from bucket by key prefix"""

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    )

    to_delete = []
    async for page in page_iterator:
        contents = page.get("Contents")
        if contents is None:
            assert page.get("KeyCount") == 0, "S3 returned no expected contents"
            continue

        for item in contents:
            s3_key = item.get("Key")
            if s3_key.startswith(prefix):
                to_delete.append(dict(Key=s3_key))

                if len(to_delete) >= 1000:
                    await s3_client.delete_objects(
                        Bucket=bucket,
                        Delete=dict(Objects=to_delete),
                    )
                    to_delete.clear()

        if len(to_delete):
            await s3_client.delete_objects(
                Bucket=bucket,
                Delete=dict(Objects=to_delete),
            )


async def list_prefix_objects(
    s3_client: AsyncS3Client,
    bucket: str,
    prefix: str,
) -> list[str]:
    """Delete every object from bucket by key prefix"""

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    )

    keys = []
    async for page in page_iterator:
        contents = page.get("Contents")
        if contents is None:
            assert page.get("KeyCount") == 0, "S3 returned no expected contents"
            continue

        for item in contents:
            s3_key = item.get("Key")
            if s3_key.startswith(prefix):
                keys.append(s3_key)

    return keys
