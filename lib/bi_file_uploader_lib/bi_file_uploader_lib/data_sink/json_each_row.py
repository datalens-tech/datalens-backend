from __future__ import annotations

import logging
from typing import Optional, ClassVar

import botocore.client
import ujson as json
from aiobotocore.client import AioBaseClient

from bi_constants.enums import ConnectionType
from bi_core.data_sink import DataSink, DataSinkAsync
from bi_core.db import SchemaColumn, get_type_transformer
from bi_core.raw_data_streaming.stream import DataStreamBase, SimpleUntypedDataStream, SimpleUntypedAsyncDataStream

from bi_file_uploader_lib import exc


LOGGER = logging.getLogger(__name__)


class S3JsonEachRowFileDataSink(DataSink):
    conn_type: ConnectionType = ConnectionType.file
    batch_size_in_bytes: int = 30 * 1024 ** 2
    max_batch_size: Optional[int] = None

    _upload_id: Optional[str] = None
    _part_tags: Optional[list[tuple[int, str]]] = None
    _part_number: int = 1
    _multipart_upload_started: bool = False

    def __init__(  # type: ignore
            self,
            bi_schema: list[SchemaColumn],
            s3: botocore.client.BaseClient,
            s3_key: str,
            bucket_name: str = None
    ):
        super().__init__(bi_schema=bi_schema)

        self._s3 = s3
        self._s3_key = s3_key
        self._bucket_name = bucket_name

        self._row_index = 0
        self._rows_saved = 0
        self._tt = get_type_transformer(self.conn_type)
        self._batch_size = None

    def initialize(self) -> None:
        mp_upl_resp = self._s3.create_multipart_upload(
            ACL='private',
            Bucket=self._bucket_name,
            Key=self._s3_key,
        )
        self._multipart_upload_started = True
        self._upload_id = mp_upl_resp['UploadId']
        LOGGER.info(f'Created multipart upload. S3 key: {self._s3_key}, UploadId: {self._upload_id}')
        self._part_tags = []
        self._part_number = 1

    def finalize(self) -> None:
        LOGGER.info(f'Completing S3 multipart upload. {self._part_number - 1} parts were uploaded.')
        if self._multipart_upload_started:
            if self._part_tags:
                self._s3.complete_multipart_upload(
                    Bucket=self._bucket_name,
                    Key=self._s3_key,
                    UploadId=self._upload_id,
                    MultipartUpload={
                        'Parts': [
                            {'ETag': etag, 'PartNumber': pnum} for pnum, etag in self._part_tags
                        ]
                    }
                )
                LOGGER.info('Multipart upload competed.')
                self._multipart_upload_started = False

    def cleanup(self) -> None:
        if self._multipart_upload_started:
            LOGGER.info('Aborting S3 multipart upload,')
            self._s3.abort_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._s3_key,
                UploadId=self._upload_id,
            )
            LOGGER.info('Multipart upload aborted.')
            self._multipart_upload_started = False

    def _prepare_chunk_body(self, batch: list[bytes]) -> bytes:
        return b'\n'.join(batch) + b'\n'

    def _dump_data_batch(self, batch: list[bytes], progress: int) -> None:
        LOGGER.info(f'Dumping {len(batch)} data rows into s3 file {self._s3_key}.')
        part_resp = self._s3.upload_part(
            Bucket=self._bucket_name,
            Key=self._s3_key,
            UploadId=self._upload_id,
            PartNumber=self._part_number,
            Body=self._prepare_chunk_body(batch),
        )
        LOGGER.info(f'Part number {self._part_number} uploaded.')
        assert self._part_tags is not None
        self._part_tags.append((self._part_number, part_resp['ETag']))
        self._part_number += 1

        self._rows_saved += len(batch)
        LOGGER.info(f'Copied: {self._rows_saved} rows ({progress}%%) to S3')

    def _process_row(self, row_data: dict) -> bytes:
        cast_row_data = [
            self._tt.cast_for_input(row_data[col.name], user_t=col.user_type)
            for col in self._bi_schema
        ]
        return json.dumps(cast_row_data).encode('utf-8')

    def _should_dump_batch(self, batch: list[bytes], batch_size: int) -> bool:
        assert self.batch_size_in_bytes
        if not batch:
            return False
        return batch_size >= self.batch_size_in_bytes

    def _dump_data_stream_base(self, data_stream: DataStreamBase) -> None:
        batch: list[bytes] = []
        batch_size = 0
        for row_data in data_stream:
            processed_row = self._process_row(row_data)

            if self._should_dump_batch(batch, batch_size=batch_size):
                self._dump_data_batch(batch, progress=data_stream.get_progress_percent())
                batch.clear()
                batch_size = 0

            batch.append(processed_row)
            batch_size += len(processed_row)
            self._row_index += 1

        if batch:
            self._dump_data_batch(batch, progress=data_stream.get_progress_percent())

    def dump_data_stream(self, data_stream: DataStreamBase) -> None:
        self._dump_data_stream_base(data_stream)


class S3JsonEachRowUntypedFileDataSink(S3JsonEachRowFileDataSink):
    def __init__(  # type: ignore
        self,
        s3: botocore.client.BaseClient,
        s3_key: str,
        bucket_name: str = None
    ):
        super().__init__(
            bi_schema=[],  # ignore bi_schema
            s3=s3,
            s3_key=s3_key,
            bucket_name=bucket_name,
        )

    def _process_row(self, row_data: list) -> bytes:  # type: ignore
        # skip type casts
        return json.dumps(row_data).encode('utf-8')

    def dump_data_stream(self, data_stream: SimpleUntypedDataStream) -> None:  # type: ignore
        self._dump_data_stream_base(data_stream)


class S3JsonEachRowUntypedFileAsyncDataSink(DataSinkAsync[SimpleUntypedAsyncDataStream]):
    conn_type: ConnectionType = ConnectionType.file
    batch_size_in_bytes: int = 30 * 1024 ** 2
    max_batch_size: Optional[int] = None
    max_file_size_bytes: ClassVar[int] = 200 * 1024 ** 2

    _upload_id: Optional[str] = None
    _part_tags: Optional[list[tuple[int, str]]] = None
    _part_number: int = 1
    _multipart_upload_started: bool = False

    def __init__(self, s3: AioBaseClient, s3_key: str, bucket_name: str = None):
        self._s3 = s3
        self._s3_key = s3_key
        self._bucket_name = bucket_name

        self._row_index = 0
        self._rows_saved = 0
        self._bytes_saved = 0
        self._batch_size = None

    async def initialize(self) -> None:
        mp_upl_resp = await self._s3.create_multipart_upload(
            ACL='private',
            Bucket=self._bucket_name,
            Key=self._s3_key,
        )
        self._multipart_upload_started = True
        self._upload_id = mp_upl_resp['UploadId']
        LOGGER.info(f'Created multipart upload. S3 key: {self._s3_key}, UploadId: {self._upload_id}')
        self._part_tags = []
        self._part_number = 1

    async def finalize(self) -> None:
        if self._multipart_upload_started:
            LOGGER.info(f'Completing S3 multipart upload. {self._part_number - 1} parts were uploaded.')
            await self._s3.complete_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._s3_key,
                UploadId=self._upload_id,
                MultipartUpload={
                    'Parts': [
                        {'ETag': etag, 'PartNumber': pnum} for pnum, etag in (self._part_tags or [])
                    ]
                }
            )
            LOGGER.info('Multipart upload competed.')
            self._multipart_upload_started = False

    async def cleanup(self) -> None:
        if self._multipart_upload_started:
            LOGGER.exception('Aborting S3 multipart upload,')
            await self._s3.abort_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._s3_key,
                UploadId=self._upload_id,
            )
            LOGGER.info('Multipart upload aborted.')
            self._multipart_upload_started = False

    def _prepare_chunk_body(self, batch: list[bytes]) -> bytes:
        return b'\n'.join(batch) + b'\n'

    async def _dump_data_batch(self, batch: list[bytes], progress: int) -> None:
        LOGGER.info(f'Dumping {len(batch)} data rows into s3 file {self._s3_key}.')
        batch_to_write = self._prepare_chunk_body(batch)
        part_resp = await self._s3.upload_part(
            Bucket=self._bucket_name,
            Key=self._s3_key,
            UploadId=self._upload_id,
            PartNumber=self._part_number,
            Body=batch_to_write,
        )
        LOGGER.info(f'Part number {self._part_number} uploaded.')
        assert isinstance(self._part_tags, list)
        self._part_tags.append((self._part_number, part_resp['ETag']))
        self._part_number += 1

        self._rows_saved += len(batch)
        self._bytes_saved += len(batch_to_write)
        LOGGER.info(f'Copied: {self._rows_saved} rows ({progress}%%) to S3')

    def _process_row(self, row_data: list) -> bytes:
        return json.dumps(row_data).encode('utf-8')

    def _should_dump_batch(self, batch: list[bytes], batch_size: int) -> bool:
        assert self.batch_size_in_bytes
        if not batch:
            return False
        return batch_size >= self.batch_size_in_bytes

    async def _dump_data_stream_base(self, data_stream: SimpleUntypedAsyncDataStream) -> None:
        batch: list[bytes] = []
        batch_size = 0
        async for row_data in data_stream:
            processed_row = self._process_row(row_data)

            if self._bytes_saved > self.max_file_size_bytes:
                raise exc.FileLimitError

            if self._should_dump_batch(batch, batch_size=batch_size):
                await self._dump_data_batch(batch, progress=data_stream.get_progress_percent())
                batch.clear()
                batch_size = 0

            batch.append(processed_row)
            batch_size += len(processed_row)
            self._row_index += 1

        if batch:
            actual_size = self._bytes_saved + len(batch)
            if actual_size > self.max_file_size_bytes:
                raise exc.FileLimitError
            await self._dump_data_batch(batch, progress=data_stream.get_progress_percent())

    async def dump_data_stream(self, data_stream: SimpleUntypedAsyncDataStream) -> None:
        await self._dump_data_stream_base(data_stream)
