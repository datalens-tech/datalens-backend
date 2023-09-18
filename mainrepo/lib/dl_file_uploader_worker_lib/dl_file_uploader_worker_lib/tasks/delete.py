import logging

import attr
from botocore.exceptions import ClientError

from dl_file_uploader_lib.redis_model.base import (
    RedisModelManager,
    RedisModelNotFound,
)
from dl_file_uploader_lib.redis_model.models import DataSourcePreview
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_task_processor.task import (
    BaseExecutorTask,
    Retry,
    Success,
    TaskResult,
)

LOGGER = logging.getLogger(__name__)


@attr.s
class DeleteFileTask(BaseExecutorTask[task_interface.DeleteFileTask, FileUploaderTaskContext]):
    cls_meta = task_interface.DeleteFileTask

    async def run(self) -> TaskResult:
        try:
            s3_filename = self.meta.s3_filename
            preview_id = self.meta.preview_id
            LOGGER.info(f"DeleteFileTask. Filename: {s3_filename}")

            if preview_id is None:
                LOGGER.info(f"Unable to delete preview for file {s3_filename} since {preview_id=}")
            else:
                rmm = RedisModelManager(
                    redis=self._ctx.redis_service.get_redis(),
                    crypto_keys_config=self._ctx.crypto_keys_config,
                )
                try:
                    preview = await DataSourcePreview.get(manager=rmm, obj_id=preview_id)
                    await preview.delete()
                    LOGGER.info(f"Successfully deleted preview id={preview_id} for file {s3_filename}")
                except RedisModelNotFound:
                    LOGGER.info(f"Preview id={preview_id} not found for file {s3_filename}")

            s3_service = self._ctx.s3_service
            s3_client = s3_service.get_client()
            try:
                await s3_client.delete_object(Bucket=s3_service.persistent_bucket_name, Key=s3_filename)
                LOGGER.info(f"Successfully deleted file {s3_filename} from the persistent bucket")
            except ClientError as ex:
                if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                    LOGGER.info(f"File {s3_filename} was not found in the persistent bucket.")
                else:
                    raise

        except Exception as ex:
            LOGGER.exception(ex)
            return Retry(attempts=5)
        return Success()
