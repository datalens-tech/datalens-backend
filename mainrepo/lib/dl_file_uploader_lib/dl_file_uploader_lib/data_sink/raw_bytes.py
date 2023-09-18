from __future__ import annotations

import logging
from typing import (
    AsyncIterator,
    ClassVar,
    Optional,
)

from aiobotocore.client import AioBaseClient

from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_constants.enums import ConnectionType
from dl_core.data_sink import DataSinkAsync
from dl_core.raw_data_streaming.stream import AsyncDataStreamBase
from dl_file_uploader_lib import exc

LOGGER = logging.getLogger(__name__)


class RawBytesAsyncDataStream(AsyncDataStreamBase[bytes]):
    def __init__(self, data_iter: AsyncIterator[bytes], bytes_to_copy: Optional[int] = None):
        self._data_iter = data_iter
        self.bytes_to_copy = bytes_to_copy

        self._bytes_read = 0

    def get_progress_percent(self) -> int:
        if self.bytes_to_copy is None:
            return 0
        if self.bytes_to_copy == 0:
            return 100
        return min(self.max_percent, int((self._bytes_read / self.bytes_to_copy) * 100))

    async def __anext__(self) -> bytes:
        chunk = await self._data_iter.__anext__()
        self._bytes_read += len(chunk)
        return chunk


class S3RawFileAsyncDataSink(DataSinkAsync[RawBytesAsyncDataStream]):
    conn_type: ConnectionType = CONNECTION_TYPE_FILE
    batch_size_in_bytes: int = 10 * 1024**2
    max_batch_size: Optional[int] = None
    max_file_size_bytes: ClassVar[int] = 200 * 1024**2

    _upload_id: Optional[str] = None
    _part_tags: Optional[list[tuple[int, str]]] = None
    _part_number: int = 1
    _multipart_upload_started: bool = False
    _chunks_saved: int = 0
    _bytes_saved: int = 0

    def __init__(self, s3: AioBaseClient, s3_key: str, bucket_name: str):
        self._s3 = s3
        self._s3_key = s3_key
        self._bucket_name = bucket_name

        self._chunks_saved = 0
        self._bytes_saved = 0

    async def initialize(self) -> None:
        mp_upl_resp = await self._s3.create_multipart_upload(
            ACL="private",
            Bucket=self._bucket_name,
            Key=self._s3_key,
        )
        self._multipart_upload_started = True
        self._upload_id = mp_upl_resp["UploadId"]
        LOGGER.info(f"Created multipart upload. S3 key: {self._s3_key}, UploadId: {self._upload_id}")
        self._part_tags = []
        self._part_number = 1

    async def finalize(self) -> None:
        if self._multipart_upload_started:
            LOGGER.info(f"Completing S3 multipart upload. {self._part_number - 1} parts were uploaded.")
            await self._s3.complete_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._s3_key,
                UploadId=self._upload_id,
                MultipartUpload={
                    "Parts": [{"ETag": etag, "PartNumber": pnum} for pnum, etag in (self._part_tags or [])]
                },
            )
            LOGGER.info("Multipart upload competed.")
            self._multipart_upload_started = False

    async def cleanup(self) -> None:
        if self._multipart_upload_started:
            LOGGER.exception("Aborting S3 multipart upload,")
            await self._s3.abort_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._s3_key,
                UploadId=self._upload_id,
            )
            LOGGER.info("Multipart upload aborted.")
            self._multipart_upload_started = False

    async def _dump_data_batch(self, batch: bytes, progress: int) -> None:
        LOGGER.info(f"Dumping {len(batch)} data rows into s3 file {self._s3_key}.")
        part_resp = await self._s3.upload_part(
            Bucket=self._bucket_name,
            Key=self._s3_key,
            UploadId=self._upload_id,
            PartNumber=self._part_number,
            Body=batch,
        )
        LOGGER.info(f"Part number {self._part_number} uploaded.")
        assert isinstance(self._part_tags, list)
        self._part_tags.append((self._part_number, part_resp["ETag"]))
        self._part_number += 1

        self._bytes_saved += len(batch)
        LOGGER.info(f"Copied: {self._bytes_saved} bytes in {self._chunks_saved} chunks ({progress}%%) to S3")

    def _should_dump_batch(self, batch: bytes) -> bool:
        assert self.batch_size_in_bytes
        if not batch:
            return False
        return len(batch) >= self.batch_size_in_bytes

    async def _dump_data_stream_base(self, data_stream: RawBytesAsyncDataStream) -> None:
        batch = b""
        async for chunk in data_stream:
            if self._bytes_saved > self.max_file_size_bytes:
                raise exc.FileLimitError

            if self._should_dump_batch(batch):
                await self._dump_data_batch(batch, progress=data_stream.get_progress_percent())
                batch = b""

            batch += chunk
            self._chunks_saved += 1

        if batch:
            actual_size = self._bytes_saved + len(batch)
            if actual_size > self.max_file_size_bytes:
                raise exc.FileLimitError
            await self._dump_data_batch(batch, progress=data_stream.get_progress_percent())

    async def dump_data_stream(self, data_stream: RawBytesAsyncDataStream) -> None:
        await self._dump_data_stream_base(data_stream)
