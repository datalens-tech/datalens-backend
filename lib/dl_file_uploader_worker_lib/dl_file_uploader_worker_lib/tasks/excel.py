import asyncio
from collections import defaultdict
import itertools
import logging
import ssl
from typing import (
    Any,
    Iterable,
    Iterator,
    Optional,
)
import urllib.parse

import aiohttp
import attr

from dl_constants.enums import FileProcessingStatus
from dl_core.db import SchemaColumn
from dl_s3.stream import SimpleUntypedDataStream
from dl_file_uploader_lib import exc
from dl_s3.data_sink import S3JsonEachRowUntypedFileDataSink
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    ExcelFileSourceSettings,
    FileProcessingError,
    FileSourceSettings,
    YaDocsFileSourceSettings,
    YaDocsUserSourceDataSourceProperties,
)
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_file_uploader_task_interface.tasks import TaskExecutionMode
from dl_file_uploader_worker_lib.utils.connection_error_tracker import FileConnectionDataSourceErrorTracker
from dl_file_uploader_worker_lib.utils.parsing_utils import guess_header_and_schema_excel
from dl_task_processor.task import (
    BaseExecutorTask,
    Fail,
    Retry,
    Success,
    TaskResult,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class ProcessExcelTask(BaseExecutorTask[task_interface.ProcessExcelTask, FileUploaderTaskContext]):
    """Loads a xlsx file from s3, runs secure-reader, schedules ParseFileTask"""

    cls_meta = task_interface.ProcessExcelTask

    async def run(self) -> TaskResult:
        dfile: Optional[DataFile] = None
        sources_to_update_by_sheet_id: dict[int, list[DataSource]] = defaultdict(list)
        usm = self._ctx.get_async_usm()
        task_processor = self._ctx.make_task_processor(self._request_id)
        redis = self._ctx.redis_service.get_redis()
        connection_error_tracker = FileConnectionDataSourceErrorTracker(usm, task_processor, redis, self._request_id)

        try:
            LOGGER.info(f"ProcessExcelTask. File: {self.meta.file_id}")
            loop = asyncio.get_running_loop()

            redis = self._ctx.redis_service.get_redis()
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)
            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)
            assert dfile is not None

            s3 = self._ctx.s3_service

            if self.meta.exec_mode == TaskExecutionMode.BASIC:
                dfile.sources = []
            assert dfile.sources is not None

            s3_resp = await s3.client.get_object(
                Bucket=s3.tmp_bucket_name,
                Key=dfile.s3_key,
            )
            file_obj = await s3_resp["Body"].read()

            conn: aiohttp.BaseConnector
            if self._ctx.secure_reader_settings.endpoint is not None:
                secure_reader_endpoint = self._ctx.secure_reader_settings.endpoint
                # TODO: after migration to aiohttp 3.9.4+ replace with
                # ssl_context: ssl.SSLContext | True = True
                ssl_context: Optional[ssl.SSLContext] = None
                if self._ctx.secure_reader_settings.cafile is not None:
                    ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
                    ssl_context.load_verify_locations(cafile=self._ctx.secure_reader_settings.cafile)
                # TODO: after migration to aiohttp 3.9.4+ replace condition with
                # aiohttp.TCPConnector(ssl=ssl_context)
                if ssl_context is not None:
                    conn = aiohttp.TCPConnector(ssl=ssl_context)
                else:
                    conn = aiohttp.TCPConnector()
            else:
                socket_path = self._ctx.secure_reader_settings.socket
                secure_reader_endpoint = f"http+unix://{urllib.parse.quote_plus(socket_path)}"
                conn = aiohttp.UnixConnector(path=socket_path)

            async with aiohttp.ClientSession(connector=conn, loop=loop) as session:
                with aiohttp.MultipartWriter() as mpwriter:
                    part = mpwriter.append(file_obj)
                    part.set_content_disposition("form-data", name="file")
                    async with session.post(
                        url=f"{secure_reader_endpoint}/reader/excel",
                        data=mpwriter,
                    ) as resp:
                        if resp.status != 200:
                            raise exc.InvalidExcel()
                        file_data = await resp.json()

            for src in dfile.sources:
                sources_to_update_by_sheet_id[src.user_source_dsrc_properties.sheet_id].append(src)  # type: ignore  # 2024-01-24 # TODO: Item "UserSourceDataSourceProperties" of "UserSourceDataSourceProperties | None" has no attribute "sheet_id"  [union-attr]

            for spreadsheet in file_data:
                sheetname = spreadsheet["sheetname"]
                sheetdata = spreadsheet["data"]
                source_status = FileProcessingStatus.in_progress
                has_header: Optional[bool] = None
                raw_schema: list[SchemaColumn] = []
                raw_schema_header: list[SchemaColumn] = []
                raw_schema_body: list[SchemaColumn] = []

                if self.meta.exec_mode == TaskExecutionMode.BASIC:
                    source_title = f"{dfile.filename} – {sheetname}"
                    if dfile.file_type == FileType.yadocs:
                        sheet_data_sources = [
                            DataSource(
                                title=source_title,
                                raw_schema=raw_schema,
                                status=source_status,
                                user_source_dsrc_properties=YaDocsUserSourceDataSourceProperties(sheet_id=sheetname),
                                error=None,
                            )
                        ]
                    else:
                        sheet_data_sources = [
                            DataSource(
                                title=source_title,
                                raw_schema=raw_schema,
                                status=source_status,
                                error=None,
                            )
                        ]
                    dfile.sources.extend(sheet_data_sources)
                else:
                    if sheetname not in sources_to_update_by_sheet_id:
                        continue
                    sheet_data_sources = sources_to_update_by_sheet_id.pop(sheetname)

                if not sheetdata:
                    for src in sheet_data_sources:
                        src.error = FileProcessingError.from_exception(exc.EmptyDocument())
                        src.status = FileProcessingStatus.failed
                        connection_error_tracker.add_error(src.id, src.error)
                else:
                    try:
                        has_header, raw_schema, raw_schema_header, raw_schema_body = guess_header_and_schema_excel(
                            sheetdata
                        )
                    except Exception as ex:
                        for src in sheet_data_sources:
                            src.status = FileProcessingStatus.failed
                            exc_to_save = ex if isinstance(ex, exc.DLFileUploaderBaseError) else exc.ParseFailed()
                            src.error = FileProcessingError.from_exception(exc_to_save)
                            connection_error_tracker.add_error(src.id, src.error)
                sheet_settings: Optional[FileSourceSettings] = None

                for src in sheet_data_sources:
                    if src.is_applicable:
                        if self.meta.exec_mode != TaskExecutionMode.BASIC:
                            assert src.file_source_settings is not None
                            has_header = src.file_source_settings.first_line_is_header
                        assert has_header is not None

                        def data_iter(row_iter: Iterator[Iterable[dict[str, Any]]]) -> Iterator[list]:
                            for row in row_iter:
                                values = [cell["value"] for cell in row]
                                yield values

                        data_stream = SimpleUntypedDataStream(
                            data_iter=data_iter(iter(sheetdata)),
                            rows_to_copy=None,  # TODO
                        )
                        with S3JsonEachRowUntypedFileDataSink(
                            s3=s3.get_sync_client(),
                            s3_key=src.s3_key,
                            bucket_name=s3.tmp_bucket_name,
                        ) as data_sink:
                            data_sink.dump_data_stream(data_stream)

                        assert has_header is not None
                        if dfile.file_type == FileType.yadocs:
                            sheet_settings = YaDocsFileSourceSettings(
                                first_line_is_header=has_header,
                                raw_schema_header=raw_schema_header,
                                raw_schema_body=raw_schema_body,
                            )
                        else:
                            sheet_settings = ExcelFileSourceSettings(
                                first_line_is_header=has_header,
                                raw_schema_header=raw_schema_header,
                                raw_schema_body=raw_schema_body,
                            )
                    src.raw_schema = raw_schema
                    src.file_source_settings = sheet_settings

            not_found_sources: Iterable[DataSource] = itertools.chain(*sources_to_update_by_sheet_id.values())
            for src in not_found_sources:
                src.error = FileProcessingError.from_exception(exc.DocumentNotFound())
                src.status = FileProcessingStatus.failed
                connection_error_tracker.add_error(src.id, src.error)

            await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "finalize" of "FileConnectionDataSourceErrorTracker" has incompatible type "TaskExecutionMode | None"; expected "TaskExecutionMode"  [arg-type]
            await dfile.save()
            LOGGER.info("DataFile object saved.")

            task_processor = self._ctx.make_task_processor(self._request_id)
            parse_file_task = task_interface.ParseFileTask(
                file_id=dfile.id,
                tenant_id=self.meta.tenant_id,
                connection_id=self.meta.connection_id,
                exec_mode=self.meta.exec_mode,  # type: ignore  # 2024-01-24 # TODO: Argument "exec_mode" to "ParseFileTask" has incompatible type "TaskExecutionMode | None"; expected "TaskExecutionMode"  [arg-type]
            )
            await task_processor.schedule(parse_file_task)
            LOGGER.info(f"Scheduled ParseFileTask for file_id {dfile.id}")

        except Exception as ex:
            LOGGER.exception(ex)
            if dfile is None:
                return Retry(attempts=3)
            else:
                dfile.status = FileProcessingStatus.failed
                exc_to_save = ex if isinstance(ex, exc.DLFileUploaderBaseError) else exc.ParseFailed()
                dfile.error = FileProcessingError.from_exception(exc_to_save)

                for src in dfile.sources or ():
                    src.status = FileProcessingStatus.failed
                    src.error = dfile.error
                    connection_error_tracker.add_error(src.id, src.error)
                await dfile.save()
                await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "finalize" of "FileConnectionDataSourceErrorTracker" has incompatible type "TaskExecutionMode | None"; expected "TaskExecutionMode"  [arg-type]

                return Fail()
        finally:
            await usm.close()
        return Success()
