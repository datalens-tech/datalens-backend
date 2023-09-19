import asyncio
import logging
import ssl
from typing import (
    Iterator,
    Optional,
)
import urllib.parse

import aiohttp
import attr

from dl_constants.enums import FileProcessingStatus
from dl_core.db import SchemaColumn
from dl_core.raw_data_streaming.stream import SimpleUntypedDataStream
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.data_sink.json_each_row import S3JsonEachRowUntypedFileDataSink
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    ExcelFileSourceSettings,
    FileProcessingError,
)
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
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
        try:
            LOGGER.info(f"ProcessExcelTask. File: {self.meta.file_id}")
            loop = asyncio.get_running_loop()

            redis = self._ctx.redis_service.get_redis()
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)
            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)
            assert dfile is not None

            s3 = self._ctx.s3_service

            dfile.sources = []

            s3_resp = await s3.client.get_object(
                Bucket=s3.tmp_bucket_name,
                Key=dfile.s3_key,
            )
            file_obj = await s3_resp["Body"].read()

            conn: aiohttp.BaseConnector
            if self._ctx.secure_reader_settings.endpoint is not None:
                secure_reader_endpoint = self._ctx.secure_reader_settings.endpoint
                ssl_context: Optional[ssl.SSLContext] = None
                if self._ctx.secure_reader_settings.cafile is not None:
                    ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
                    ssl_context.load_verify_locations(cafile=self._ctx.secure_reader_settings.cafile)
                conn = aiohttp.TCPConnector(ssl=ssl_context)
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
                        file_data = await resp.json()

            for spreadsheet in file_data:
                sheetname = spreadsheet["sheetname"]
                sheetdata = spreadsheet["data"]
                source_status = FileProcessingStatus.in_progress
                has_header: Optional[bool] = None
                raw_schema: list[SchemaColumn] = []
                raw_schema_header: list[SchemaColumn] = []
                raw_schema_body: list[SchemaColumn] = []
                source_title = f"{dfile.filename} – {sheetname}"
                sheet_data_source = DataSource(
                    title=source_title,
                    raw_schema=raw_schema,
                    status=source_status,
                    error=None,
                )
                dfile.sources.append(sheet_data_source)

                if not sheetdata:
                    sheet_data_source.error = FileProcessingError.from_exception(exc.EmptyDocument())
                    sheet_data_source.status = FileProcessingStatus.failed
                else:
                    has_header, raw_schema, raw_schema_header, raw_schema_body = guess_header_and_schema_excel(
                        sheetdata
                    )
                sheet_settings = None

                if sheet_data_source.is_applicable:

                    def data_iter() -> Iterator[list]:
                        row_iter = iter(sheetdata)
                        for row in row_iter:
                            values = [cell["value"] for cell in row]
                            yield values

                    data_stream = SimpleUntypedDataStream(
                        data_iter=data_iter(),
                        rows_to_copy=None,  # TODO
                    )
                    with S3JsonEachRowUntypedFileDataSink(
                        s3=s3.get_sync_client(),
                        s3_key=sheet_data_source.s3_key,
                        bucket_name=s3.tmp_bucket_name,
                    ) as data_sink:
                        data_sink.dump_data_stream(data_stream)

                    assert has_header is not None
                    sheet_settings = ExcelFileSourceSettings(
                        first_line_is_header=has_header,
                        raw_schema_header=raw_schema_header,
                        raw_schema_body=raw_schema_body,
                    )

                sheet_data_source.raw_schema = raw_schema
                sheet_data_source.file_source_settings = sheet_settings

            await dfile.save()
            LOGGER.info("DataFile object saved.")

            task_processor = self._ctx.make_task_processor(self._request_id)
            parse_file_task = task_interface.ParseFileTask(file_id=dfile.id)
            await task_processor.schedule(parse_file_task)
            LOGGER.info(f"Scheduled ParseFileTask for file_id {dfile.id}")

        except Exception as ex:
            LOGGER.exception(ex)
            if dfile is None:
                return Retry(attempts=3)
            else:
                return Fail()
        return Success()
