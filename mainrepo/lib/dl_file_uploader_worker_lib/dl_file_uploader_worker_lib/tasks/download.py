from __future__ import annotations

import asyncio
from collections import defaultdict
import itertools
import logging
from typing import (
    AsyncIterator,
    Iterable,
    Optional,
)

import aiogoogle
import attr

from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_constants.enums import (
    BIType,
    FileProcessingStatus,
)
from dl_core.aio.web_app_services.gsheets import (
    Range,
    Sheet,
)
from dl_core.db import SchemaColumn
from dl_core.raw_data_streaming.stream import SimpleUntypedAsyncDataStream
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.data_sink.json_each_row import S3JsonEachRowUntypedFileAsyncDataSink
from dl_file_uploader_lib.gsheets_client import (
    GSheetsClient,
    GSheetsOAuth2,
    google_api_error_to_file_uploader_exception,
)
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    FileProcessingError,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
)
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_file_uploader_task_interface.tasks import TaskExecutionMode
from dl_file_uploader_worker_lib.utils.connection_error_tracker import FileConnectionDataSourceErrorTracker
from dl_file_uploader_worker_lib.utils.parsing_utils import guess_header_and_schema_gsheet
from dl_task_processor.task import (
    BaseExecutorTask,
    Fail,
    Retry,
    Success,
    TaskResult,
)

LOGGER = logging.getLogger(__name__)


class NoToken(Exception):
    pass


async def _get_gsheets_auth(
    dfile_token: Optional[str],
    conn_id: Optional[str],
    usm: AsyncUSManager,
) -> Optional[GSheetsOAuth2]:
    if dfile_token is not None:  # if there is a token in dfile, then use it
        refresh_token = dfile_token
    elif conn_id is not None:  # otherwise, use the one from the connection
        conn: GSheetsFileS3Connection = await usm.get_by_id(conn_id, GSheetsFileS3Connection)
        if conn.data.refresh_token is None:
            raise NoToken()
        refresh_token = conn.data.refresh_token
    else:
        raise NoToken()

    return GSheetsOAuth2(access_token=None, refresh_token=refresh_token) if refresh_token is not None else None


async def _values_data_iter(
    sheets_client: GSheetsClient,
    spreadsheet_id: str,
    sheet_sample: Sheet,
    user_types: list[BIType | str],
    raw_schema_body: list[SchemaColumn],
) -> AsyncIterator[list]:
    """
    Trying to convert values to the originally guessed schema
    Fallback to strings after all data is read, for which
    raw_schema_body is updated inplace on each portion
    """

    acceptable_resp_size_bytes = 4e6
    acceptable_deviation = 0.25
    max_acceptable_resp_size = acceptable_resp_size_bytes * (1 + acceptable_deviation)

    # dump the first row separately not to bother about its types later
    assert sheet_sample.data is not None
    yield [cell.value for cell in sheet_sample.data[0]]

    current_range = Range(
        sheet_title=sheet_sample.title,
        row_from=2,  # start reading from the second line, because the first one is dumped above
        col_from=1,
        row_to=sheet_sample.batch_size_rows,
        col_to=sheet_sample.column_count,
    )
    body_rows_total = 0
    while True:
        sheet_portion_values, new_user_types = await sheets_client.get_single_values_range(
            spreadsheet_id=spreadsheet_id,
            range=current_range,
            user_types=user_types,
        )
        body_rows_total += len(sheet_portion_values)
        if not sheet_portion_values:
            # no data received, we assume that we are done
            if body_rows_total == 0:
                raise exc.TooFewRowsError()
            return

        resp_size = sheets_client.last_response_size_bytes
        if resp_size > max_acceptable_resp_size:
            prev_batch_size_rows = sheet_sample.batch_size_rows
            adjust_rows_coeff = acceptable_resp_size_bytes / resp_size
            sheet_sample.batch_size_rows = int(sheet_sample.batch_size_rows * adjust_rows_coeff)
            LOGGER.info(
                f"Got a response with size {resp_size}, which is too large, going to adjust"
                f" batch size rows from {prev_batch_size_rows}"
                f" to {sheet_sample.batch_size_rows}"
            )

        for idx, (col, new_user_type) in enumerate(zip(raw_schema_body, new_user_types)):
            # fall back to string
            if new_user_type == "time":
                new_user_type = BIType.string
            if new_user_type != col.user_type and new_user_type == BIType.string:
                raw_schema_body[idx] = raw_schema_body[idx].clone(user_type=new_user_type)

        for row in sheet_portion_values:
            yield row

        current_range.row_from = current_range.row_to + 1
        current_range.row_to = min(current_range.row_to + sheet_sample.batch_size_rows, sheet_sample.row_count)
        if current_range.row_from > sheet_sample.row_count:
            if body_rows_total == 0:
                raise exc.TooFewRowsError()
            return


@attr.s
class DownloadGSheetTask(BaseExecutorTask[task_interface.DownloadGSheetTask, FileUploaderTaskContext]):
    """Loads a spreadsheet into s3, building raw_schema based solely on gsheet cell types, schedules ParseFileTask"""

    cls_meta = task_interface.DownloadGSheetTask

    async def run(self) -> TaskResult:
        dfile: Optional[DataFile] = None
        sources_to_update_by_sheet_id: dict[int, list[DataSource]] = defaultdict(list)
        usm = self._ctx.get_async_usm()
        task_processor = self._ctx.make_task_processor(self._request_id)
        redis = self._ctx.redis_service.get_redis()
        connection_error_tracker = FileConnectionDataSourceErrorTracker(usm, task_processor, redis, self._request_id)
        try:
            LOGGER.info(f"DownloadGSheetTask. Mode: {self.meta.exec_mode.name}. File: {self.meta.file_id}")
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)
            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)
            assert dfile is not None

            assert isinstance(dfile.user_source_properties, GSheetsUserSourceProperties)
            spreadsheet_id = dfile.user_source_properties.spreadsheet_id

            if self.meta.exec_mode == TaskExecutionMode.BASIC:
                dfile.sources = []
            assert dfile.sources is not None

            auth: Optional[GSheetsOAuth2] = None
            if self.meta.authorized:
                try:
                    dfile_token = dfile.user_source_properties.refresh_token
                    auth = await _get_gsheets_auth(dfile_token, self.meta.connection_id, usm)
                except NoToken:
                    LOGGER.error("Authorized call but no token found in either DataFile or connection, failing task")
                    return Fail()

            async with GSheetsClient(self._ctx.gsheets_settings, self._ctx.tpe, auth) as sheets_client:
                try:
                    spreadsheet_sample = await sheets_client.get_spreadsheet_sample(spreadsheet_id)
                except (aiogoogle.HTTPError, exc.DLFileUploaderBaseError) as e:
                    if isinstance(e, aiogoogle.HTTPError):
                        e = google_api_error_to_file_uploader_exception(e)
                    download_error = FileProcessingError.from_exception(e)
                    dfile.status = FileProcessingStatus.failed
                    dfile.error = download_error

                    for src in dfile.sources:
                        src.status = FileProcessingStatus.failed
                        src.error = download_error
                        connection_error_tracker.add_error(src.id, src.error)
                    await dfile.save()

                    await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)
                    return Success()

                s3 = self._ctx.s3_service

                dfile.filename = spreadsheet_sample.title
                for src in dfile.sources:
                    assert isinstance(src.user_source_dsrc_properties, GSheetsUserSourceDataSourceProperties)
                    sources_to_update_by_sheet_id[src.user_source_dsrc_properties.sheet_id].append(src)
                for sheet_sample in spreadsheet_sample.sheets:
                    source_status = FileProcessingStatus.in_progress
                    has_header: Optional[bool] = None
                    raw_schema: list[SchemaColumn] = []
                    raw_schema_header: list[SchemaColumn] = []
                    raw_schema_body: list[SchemaColumn] = []
                    sheet_properties = GSheetsUserSourceDataSourceProperties(sheet_id=sheet_sample.id)

                    if self.meta.exec_mode == TaskExecutionMode.BASIC:
                        source_title = spreadsheet_sample.title + " – " + sheet_sample.title
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
                        if sheet_sample.id not in sources_to_update_by_sheet_id:
                            continue
                        sheet_data_sources = sources_to_update_by_sheet_id.pop(sheet_sample.id)

                    loop = asyncio.get_running_loop()
                    if not sheet_sample.data:
                        for src in sheet_data_sources:
                            src.error = FileProcessingError.from_exception(exc.EmptyDocument())
                            src.status = FileProcessingStatus.failed
                            connection_error_tracker.add_error(src.id, src.error)
                    else:
                        try:
                            has_header, raw_schema, raw_schema_header, raw_schema_body = await loop.run_in_executor(
                                self._ctx.tpe, guess_header_and_schema_gsheet, sheet_sample
                            )
                        except exc.DLFileUploaderBaseError as e:
                            for src in sheet_data_sources:
                                src.status = FileProcessingStatus.failed
                                src.error = FileProcessingError.from_exception(e)
                                connection_error_tracker.add_error(src.id, src.error)

                    sheet_settings = None

                    for src in sheet_data_sources:
                        if src.is_applicable:
                            if self.meta.exec_mode != TaskExecutionMode.BASIC:
                                assert src.file_source_settings is not None
                                has_header = src.file_source_settings.first_line_is_header
                            assert has_header is not None

                            orig_user_types: list[BIType | str] = []
                            for idx, col in enumerate(raw_schema_body):
                                if col.user_type == BIType.string and sheet_sample.col_is_time(idx, has_header):
                                    orig_user_types.append("time")
                                else:
                                    orig_user_types.append(col.user_type)

                            data_stream = SimpleUntypedAsyncDataStream(
                                data_iter=_values_data_iter(
                                    sheets_client,
                                    spreadsheet_id,
                                    sheet_sample,
                                    orig_user_types,
                                    raw_schema_body,
                                ),
                                rows_to_copy=None,  # TODO
                            )
                            try:
                                async with S3JsonEachRowUntypedFileAsyncDataSink(
                                    s3=s3.get_client(),
                                    s3_key=src.s3_key,
                                    bucket_name=s3.tmp_bucket_name,
                                ) as data_sink:
                                    await data_sink.dump_data_stream(data_stream)
                            except exc.DLFileUploaderBaseError as e:
                                src.status = FileProcessingStatus.failed
                                src.error = FileProcessingError.from_exception(e)
                                connection_error_tracker.add_error(src.id, src.error)

                            sheet_settings = GSheetsFileSourceSettings(
                                first_line_is_header=has_header,
                                raw_schema_header=raw_schema_header,
                                raw_schema_body=raw_schema_body,
                            )

                        src.raw_schema = raw_schema
                        src.file_source_settings = sheet_settings
                        src.user_source_dsrc_properties = sheet_properties

            not_found_sources: Iterable[DataSource] = itertools.chain(*sources_to_update_by_sheet_id.values())
            for src in not_found_sources:
                src.error = FileProcessingError.from_exception(exc.DocumentNotFound())
                src.status = FileProcessingStatus.failed
                connection_error_tracker.add_error(src.id, src.error)

            await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)
            await dfile.save()
            LOGGER.info("DataFile object saved.")

            if self.meta.schedule_parsing:
                parse_file_task = task_interface.ParseFileTask(
                    file_id=dfile.id,
                    tenant_id=self.meta.tenant_id,
                    connection_id=self.meta.connection_id,
                    exec_mode=self.meta.exec_mode,
                )
                await task_processor.schedule(parse_file_task)
                LOGGER.info(f"Scheduled ParseFileTask for file_id {dfile.id}")
            else:
                LOGGER.info(f"Skipping ParseFileTask for file_id {dfile.id} because {self.meta.schedule_parsing=}")
        except Exception as ex:
            LOGGER.exception(ex)
            if dfile is None:
                return Retry(attempts=3)
            else:
                dfile.status = FileProcessingStatus.failed
                exc_to_save = ex if isinstance(ex, exc.DLFileUploaderBaseError) else exc.DownloadFailed()
                dfile.error = FileProcessingError.from_exception(exc_to_save)
                await dfile.save()

                for src in dfile.sources or ():
                    connection_error_tracker.add_error(src.id, dfile.error)
                await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)

                return Fail()
        finally:
            await usm.close()
        return Success()
