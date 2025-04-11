from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
)


if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

from dl_constants.exc import DLBaseException
from dl_s3.data_sink.base import DataSinkAsync
from dl_s3.stream import RawBytesAsyncDataStream


LOGGER = logging.getLogger(__name__)


class S3RawFileAsyncDataSink(DataSinkAsync[RawBytesAsyncDataStream]):
    batch_size_in_bytes: int = 10 * 1024**2
    max_batch_size: Optional[int] = None
    max_file_size_bytes: ClassVar[int] = 200 * 1024**2

    _upload_id: Optional[str] = None
    _part_tags: Optional[list[tuple[int, str]]] = None
    _part_number: int = 1
    _multipart_upload_started: bool = False
    _chunks_saved: int = 0
    _bytes_saved: int = 0

    def __init__(self, s3: S3Client, s3_key: str, bucket_name: str, max_file_size_exc: type[DLBaseException]) -> None:
        self._s3 = s3
        self._s3_key = s3_key
        self._bucket_name = bucket_name
        self._max_file_size_exc = max_file_size_exc

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
            assert self._upload_id is not None
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
            assert self._upload_id is not None
            await self._s3.abort_multipart_upload(
                Bucket=self._bucket_name,
                Key=self._s3_key,
                UploadId=self._upload_id,
            )
            LOGGER.info("Multipart upload aborted.")
            self._multipart_upload_started = False

    async def _dump_data_batch(self, batch: bytes, progress: int) -> None:
        if self._bytes_saved + len(batch) > self.max_file_size_bytes:
            raise self._max_file_size_exc
        LOGGER.info(f"Dumping {len(batch)} data rows into s3 file {self._s3_key}.")
        assert self._upload_id is not None
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
            if self._should_dump_batch(batch):
                await self._dump_data_batch(batch, progress=data_stream.get_progress_percent())
                batch = b""

            batch += chunk
            self._chunks_saved += 1

        if batch:
            await self._dump_data_batch(batch, progress=data_stream.get_progress_percent())

    async def dump_data_stream(self, data_stream: RawBytesAsyncDataStream) -> None:
        await self._dump_data_stream_base(data_stream)
