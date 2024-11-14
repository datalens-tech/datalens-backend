from __future__ import annotations

import logging
from typing import (
    AsyncGenerator,
    Optional,
)

import aiohttp
import attr

from dl_constants.enums import FileProcessingStatus
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    FileProcessingError,
    YaDocsUserSourceProperties,
)
from dl_file_uploader_lib.yadocs_client import (
    YaDocsClient,
    yadocs_error_to_file_uploader_exception,
)
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_file_uploader_task_interface.tasks import TaskExecutionMode
from dl_file_uploader_worker_lib.utils.connection_error_tracker import FileConnectionDataSourceErrorTracker
from dl_s3.data_sink import (
    RawBytesAsyncDataStream,
    S3RawFileAsyncDataSink,
)
from dl_task_processor.task import (
    BaseExecutorTask,
    Fail,
    Retry,
    Success,
    TaskResult,
)

from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


LOGGER = logging.getLogger(__name__)


class NoToken(Exception):
    pass


async def _get_yadocs_oauth_token(
    dfile_token: Optional[str],
    conn_id: Optional[str],
    usm: AsyncUSManager,
) -> Optional[str]:
    if dfile_token is not None:  # if there is a token in dfile, then use it
        oauth_token = dfile_token
    elif conn_id is not None:  # otherwise, use the one from the connection
        conn: YaDocsFileS3Connection = await usm.get_by_id(conn_id, YaDocsFileS3Connection)  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "USEntry", variable has type "YaDocsFileS3Connection")  [assignment]
        if conn.data.oauth_token is None:
            raise NoToken()
        oauth_token = conn.data.oauth_token
    else:
        raise NoToken()

    return oauth_token


@attr.s
class DownloadYaDocsTask(BaseExecutorTask[task_interface.DownloadYaDocsTask, FileUploaderTaskContext]):
    cls_meta = task_interface.DownloadYaDocsTask

    async def run(self) -> TaskResult:
        dfile: Optional[DataFile] = None
        redis = self._ctx.redis_service.get_redis()
        task_processor = self._ctx.make_task_processor(self._request_id)
        usm = self._ctx.get_async_usm()
        connection_error_tracker = FileConnectionDataSourceErrorTracker(usm, task_processor, redis, self._request_id)
        try:
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)
            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)
            assert dfile is not None

            assert isinstance(dfile.user_source_properties, YaDocsUserSourceProperties)

            oauth_token: Optional[str] = None
            if self.meta.authorized:
                try:
                    dfile_token = dfile.user_source_properties.oauth_token
                    oauth_token = await _get_yadocs_oauth_token(dfile_token, self.meta.connection_id, usm)
                except NoToken:
                    LOGGER.error("Authorized call but no token found in either DataFile or connection, failing task")
                    return Fail()

            async with aiohttp.ClientSession() as session:
                yadocs_client = YaDocsClient(session)
                try:
                    if dfile.user_source_properties.public_link is not None:
                        spreadsheet_ref = await yadocs_client.get_spreadsheet_public_ref(
                            link=dfile.user_source_properties.public_link
                        )
                        spreadsheet_meta = await yadocs_client.get_spreadsheet_public_meta(
                            link=dfile.user_source_properties.public_link
                        )

                    elif dfile.user_source_properties.private_path is not None and oauth_token is not None:
                        spreadsheet_ref = await yadocs_client.get_spreadsheet_private_ref(
                            path=dfile.user_source_properties.private_path,
                            token=oauth_token,
                        )
                        spreadsheet_meta = await yadocs_client.get_spreadsheet_private_meta(
                            path=dfile.user_source_properties.private_path,
                            token=oauth_token,
                        )
                    else:
                        raise exc.DLFileUploaderBaseError()
                except exc.DLFileUploaderBaseError as e:
                    LOGGER.exception(e)
                    download_error = FileProcessingError.from_exception(e)
                    dfile.status = FileProcessingStatus.failed
                    dfile.error = download_error
                    if self.meta.exec_mode != TaskExecutionMode.BASIC:
                        for src in dfile.sources or []:
                            src.status = FileProcessingStatus.failed
                            src.error = download_error
                            connection_error_tracker.add_error(src.id, src.error)
                    await dfile.save()

                    await connection_error_tracker.finalize(self.meta.exec_mode, self.meta.connection_id)
                    return Success()

            filename: str = spreadsheet_meta["name"]
            xlsx_suffix = ".xlsx"
            if not filename.endswith(xlsx_suffix):
                raise exc.UnsupportedDocument(
                    details={"reason": f"Supported file extensions are: '.xlsx'. Got {filename}"}
                )

            dfile.filename = filename
            s3 = self._ctx.s3_service

            async def _chunk_iter(chunk_size: int = 10 * 1024 * 1024) -> AsyncGenerator[bytes, None]:
                async with aiohttp.ClientSession() as session:
                    async with session.get(spreadsheet_ref) as resp:
                        if resp.status != 200:
                            raise yadocs_error_to_file_uploader_exception(resp.status, await resp.json())
                        while True:
                            chunk = await resp.content.read(chunk_size)
                            if chunk:
                                LOGGER.debug(f"Received chunk of {len(chunk)} bytes.")
                                yield chunk
                            else:
                                LOGGER.info("Empty chunk received.")
                                break

            data_stream = RawBytesAsyncDataStream(data_iter=_chunk_iter())
            async with S3RawFileAsyncDataSink(
                s3=s3.client,
                s3_key=dfile.s3_key,
                bucket_name=s3.tmp_bucket_name,
            ) as data_sink:
                await data_sink.dump_data_stream(data_stream)

            await dfile.save()
            LOGGER.info(f'Uploaded file "{dfile.filename}".')

            await task_processor.schedule(
                task_interface.ProcessExcelTask(
                    file_id=dfile.id,
                    tenant_id=self.meta.tenant_id,
                    connection_id=self.meta.connection_id,
                    exec_mode=self.meta.exec_mode,
                )
            )
            LOGGER.info(f"Scheduled ProcessExcelTask for file_id {dfile.id}")

        except Exception as ex:
            LOGGER.exception(ex)
            if dfile is None:
                return Retry(attempts=3)
            else:
                dfile.status = FileProcessingStatus.failed
                exc_to_save = ex if isinstance(ex, exc.DLFileUploaderBaseError) else exc.DownloadFailed()
                dfile.error = FileProcessingError.from_exception(exc_to_save)
                await dfile.save()

                return Fail()
        return Success()
