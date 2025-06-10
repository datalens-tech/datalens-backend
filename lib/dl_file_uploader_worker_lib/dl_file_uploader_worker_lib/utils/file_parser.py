import abc
import asyncio
import concurrent.futures
import csv
import logging
from typing import (
    Any,
    Optional,
)

import attr
from botocore.response import StreamingBody

from dl_constants.enums import FileProcessingStatus
from dl_core.db import SchemaColumn
from dl_file_uploader_lib.enums import (
    CSVDelimiter,
    CSVEncoding,
)
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    CSVFileSettings,
    CSVFileSourceSettings,
    DataFile,
    DataSource,
    DataSourcePreview,
    FileSettings,
    FileSourceSettings,
    SpreadsheetFileSourceSettings,
)
from dl_file_uploader_lib.s3_model.base import S3ModelManager
from dl_file_uploader_lib.s3_model.models import S3DataSourcePreview
from dl_file_uploader_worker_lib.utils.parsing_utils import (
    detect_dialect,
    detect_encoding,
    guess_header_and_schema,
    make_upcropped_text_sample,
    prepare_preview,
    prepare_preview_from_json_each_row,
    reguess_header_and_schema_spreadsheet,
)
from dl_s3.s3_service import S3Service
from dl_utils.aio import ContextVarExecutor


LOGGER = logging.getLogger(__name__)


class FileParser(metaclass=abc.ABCMeta):
    def __init__(
        self,
        dfile: DataFile,
        s3: S3Service,
        tpe: ContextVarExecutor,
        file_settings: Optional[dict[str, Any]] = None,
        sample_size: int = 1 * 1024 * 1024,
        source_id: Optional[str] = None,
    ):
        self.dfile = dfile
        self.s3 = s3
        self.tpe = tpe
        self.file_settings = file_settings or {}
        self.sample_size = sample_size
        self.source_id = source_id

    def _get_sync_s3_data_stream(self, key: str) -> StreamingBody:
        s3_sync_cli = self.s3.get_sync_client()
        s3_sync_resp = s3_sync_cli.get_object(Bucket=self.s3.tmp_bucket_name, Key=key)
        return s3_sync_resp["Body"]

    @abc.abstractmethod
    def ensure_sources_and_return(self) -> list[DataSource]:
        raise NotImplementedError

    @abc.abstractmethod
    async def guess_header_and_schema(
        self,
        dsrc: DataSource,
    ) -> tuple[bool, list[SchemaColumn], Optional[FileSettings], FileSourceSettings]:
        raise NotImplementedError

    @abc.abstractmethod
    async def prepare_preview(self, dsrc: DataSource, s3mm: S3ModelManager) -> S3DataSourcePreview:
        raise NotImplementedError

    # TODO(catsona): Remove after release, support for old behavior
    @abc.abstractmethod
    async def prepare_preview_legacy(self, dsrc: DataSource, rmm: RedisModelManager) -> DataSourcePreview:
        raise NotImplementedError


class CSVFileParser(FileParser):
    def ensure_sources_and_return(self) -> list[DataSource]:
        if self.source_id is not None:
            return [self.dfile.get_source_by_id(self.source_id)]

        self.dfile.sources = [
            DataSource(
                title=self.dfile.filename or self.dfile.id,
                raw_schema=[],
                status=FileProcessingStatus.in_progress,
            )
        ]

        return self.dfile.sources

    async def _make_sample_text(self) -> str:
        loop = asyncio.get_running_loop()

        s3_resp = await self.s3.client.get_object(
            Bucket=self.s3.tmp_bucket_name,
            Key=self.dfile.s3_key_old,
            Range=f"bytes=0-{self.sample_size}",
        )
        sample_bytes = await s3_resp["Body"].read()
        LOGGER.info(f"Downloaded sample from s3. Sample length: {len(sample_bytes)} bytes.")

        if self.file_settings.get("encoding") is not None:
            encoding = getattr(CSVEncoding, self.file_settings["encoding"])
            LOGGER.info(f"Overriding encoding with user defined: {encoding}")
        else:
            encoding = await loop.run_in_executor(self.tpe, detect_encoding, sample_bytes)
            LOGGER.info(f"Detected encoding: {encoding}")

        sample_text = await loop.run_in_executor(
            self.tpe,
            make_upcropped_text_sample,
            sample_bytes,
            self.sample_size,
            encoding,
        )

        self.encoding = encoding
        self.sample_text = sample_text
        return sample_text

    async def ensure_sample_text_loaded(self) -> None:
        if not hasattr(self, "sample_text") or not hasattr(self, "encoding"):
            await self._make_sample_text()

    async def guess_header_and_schema(
        self,
        dsrc: DataSource,
    ) -> tuple[bool, list[SchemaColumn], Optional[FileSettings], FileSourceSettings]:
        loop = asyncio.get_running_loop()
        await self.ensure_sample_text_loaded()

        dialect_detection_retries = 50
        dialect_detection_retry_timeout_sec = 0.2
        with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(detect_dialect, self.sample_text)

            while dialect_detection_retries > 0:
                dialect_detection_retries -= 1
                await asyncio.sleep(dialect_detection_retry_timeout_sec)

                if not future.running():
                    dialect = future.result()
                    break
            else:
                dialect = csv.excel()
                executor.shutdown(wait=False, cancel_futures=True)

        LOGGER.info(f"Detected dialect: {dialect}")
        if self.file_settings.get("delimiter") is not None:
            delimiter = getattr(CSVDelimiter, self.file_settings["delimiter"])
            dialect.delimiter = delimiter.value
            LOGGER.info(f"Overriding dialect delimiter with user defined: {delimiter}")

        has_header = None
        if self.file_settings.get("first_line_is_header") is not None:
            has_header = self.file_settings["first_line_is_header"]
            LOGGER.info(f"Overriding `has_header` with user defined: has_header={has_header}")

        data_stream = await loop.run_in_executor(self.tpe, self._get_sync_s3_data_stream, self.dfile.s3_key_old)
        has_header, raw_schema = await loop.run_in_executor(
            self.tpe,
            guess_header_and_schema,
            data_stream,
            self.encoding,
            dialect,
            has_header,
        )
        LOGGER.info(f"has_header: {has_header}, raw_schema: {raw_schema}")

        file_settings = CSVFileSettings(
            encoding=self.encoding,
            dialect=dialect,
        )
        file_source_settings = CSVFileSourceSettings(
            first_line_is_header=has_header,
        )

        return has_header, raw_schema, file_settings, file_source_settings

    async def _prepare_preview_data(self, dsrc: DataSource) -> Any:
        loop = asyncio.get_running_loop()

        await self.ensure_sample_text_loaded()

        file_settings = self.dfile.file_settings
        assert isinstance(file_settings, CSVFileSettings)

        file_source_settings = dsrc.file_source_settings
        assert isinstance(file_source_settings, CSVFileSourceSettings)

        return await loop.run_in_executor(
            self.tpe,
            prepare_preview,
            self.sample_text,
            file_settings.dialect,
            file_source_settings.first_line_is_header,
        )

    async def prepare_preview(self, dsrc: DataSource, s3mm: S3ModelManager) -> S3DataSourcePreview:
        return S3DataSourcePreview(manager=s3mm, preview_data=await self._prepare_preview_data(dsrc))

    @abc.abstractmethod
    async def prepare_preview_legacy(self, dsrc: DataSource, rmm: RedisModelManager) -> DataSourcePreview:
        return RedisModelManager(manager=rmm, preview_data=await self._prepare_preview_data(dsrc))


class SpreadsheetFileParser(FileParser):
    def ensure_sources_and_return(self) -> list[DataSource]:
        if self.source_id is not None:
            return [self.dfile.get_source_by_id(self.source_id)]

        return self.dfile.sources or []

    async def guess_header_and_schema(
        self,
        dsrc: DataSource,
    ) -> tuple[bool, list[SchemaColumn], Optional[FileSettings], FileSourceSettings]:
        loop = asyncio.get_running_loop()

        file_source_settings = dsrc.file_source_settings
        assert isinstance(file_source_settings, SpreadsheetFileSourceSettings)

        has_header = file_source_settings.first_line_is_header
        if self.file_settings.get("first_line_is_header") is not None:
            has_header = self.file_settings["first_line_is_header"]
            LOGGER.info(f"Overriding `has_header` with user defined: has_header={has_header}")

        raw_schema_header = file_source_settings.raw_schema_header
        raw_schema_body = file_source_settings.raw_schema_body
        file_type = file_source_settings.file_type
        has_header, raw_schema = await loop.run_in_executor(
            self.tpe, reguess_header_and_schema_spreadsheet, raw_schema_header, raw_schema_body, has_header, file_type
        )
        LOGGER.info(f"Re-guessed string columns. {has_header=}, {raw_schema=}")

        file_settings = None
        file_source_settings = attr.evolve(file_source_settings, first_line_is_header=has_header)

        return has_header, raw_schema, file_settings, file_source_settings

    async def _prepare_preview_data(self, dsrc: DataSource) -> Any:
        loop = asyncio.get_running_loop()

        s3_resp = await self.s3.client.get_object(
            Bucket=self.s3.tmp_bucket_name,
            Key=dsrc.s3_key,
            Range=f"bytes=0-{self.sample_size}",
        )
        sample_bytes = await s3_resp["Body"].read()
        LOGGER.info(f"Downloaded sample from s3. Sample length: {len(sample_bytes)} bytes.")

        file_source_settings = dsrc.file_source_settings
        assert isinstance(file_source_settings, SpreadsheetFileSourceSettings)
        has_header = file_source_settings.first_line_is_header

        return await loop.run_in_executor(self.tpe, prepare_preview_from_json_each_row, sample_bytes, has_header)

    async def prepare_preview(self, dsrc: DataSource, s3mm: S3ModelManager) -> S3DataSourcePreview:
        return S3DataSourcePreview(manager=s3mm, preview_data=await self._prepare_preview_data(dsrc))

    async def prepare_preview_legacy(self, dsrc: DataSource, rmm: RedisModelManager) -> DataSourcePreview:
        return RedisModelManager(manager=rmm, preview_data=await self._prepare_preview_data(dsrc))
