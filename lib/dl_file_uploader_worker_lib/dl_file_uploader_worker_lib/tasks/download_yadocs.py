from __future__ import annotations


import logging
from typing import Optional

import aiohttp
import attr

from dl_constants.enums import FileProcessingStatus

from dl_file_uploader_lib import exc
from dl_file_uploader_lib.data_sink.raw_bytes import (
    RawBytesAsyncDataStream,
    S3RawFileAsyncDataSink,
)

from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    FileProcessingError,
    YaDocsUserSourceProperties,
)
from dl_file_uploader_lib.yadocs_client import YaDocsClient, yadocs_error_to_file_uploader_exception
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_task_processor.task import (
    BaseExecutorTask,
    Fail,
    Retry,
    Success,
    TaskResult,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class DownloadYaDocsTask(BaseExecutorTask[task_interface.DownloadYaDocsTask, FileUploaderTaskContext]):
    cls_meta = task_interface.DownloadYaDocsTask

    async def run(self) -> TaskResult:
        dfile: Optional[DataFile] = None
        redis = self._ctx.redis_service.get_redis()

        try:
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)
            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)
            assert dfile is not None

            assert isinstance(dfile.user_source_properties, YaDocsUserSourceProperties)

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

                    elif (
                        dfile.user_source_properties.private_path is not None
                        and dfile.user_source_properties.oauth_token is not None
                    ):
                        spreadsheet_ref = await yadocs_client.get_spreadsheet_private_ref(
                            path=dfile.user_source_properties.private_path,
                            token=dfile.user_source_properties.oauth_token,
                        )
                        spreadsheet_meta = await yadocs_client.get_spreadsheet_private_meta(
                            path=dfile.user_source_properties.private_path,
                            token=dfile.user_source_properties.oauth_token,
                        )
                    else:
                        raise exc.DLFileUploaderBaseError()
                except exc.DLFileUploaderBaseError as e:
                    LOGGER.exception(e)
                    download_error = FileProcessingError.from_exception(e)
                    dfile.status = FileProcessingStatus.failed
                    dfile.error = download_error
                    await dfile.save()
                    return Success()

            dfile.filename = spreadsheet_meta["name"]
            s3 = self._ctx.s3_service

            async def _chunk_iter(chunk_size: int = 10 * 1024 * 1024):
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
