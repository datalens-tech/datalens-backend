import logging

import attr
from botocore.exceptions import ClientError

from dl_file_uploader_lib.redis_model.base import (
    RedisModelManager,
    RedisModelNotFound,
)
from dl_file_uploader_lib.redis_model.models import DataSourcePreview
from dl_file_uploader_lib.s3_model.base import (
    S3ModelManager,
    S3ModelNotFound,
)
from dl_file_uploader_lib.s3_model.models import S3DataSourcePreview
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
                tenant_id = self.meta.tenant_id

                # Delete from S3
                s3mm = S3ModelManager(
                    s3_service=self._ctx.s3_service,
                    crypto_keys_config=self._ctx.crypto_keys_config,
                    tenant_id=tenant_id,
                )

                try:
                    s3_preview = await S3DataSourcePreview.get(manager=s3mm, obj_id=preview_id)
                    assert isinstance(s3_preview, S3DataSourcePreview)

                    await s3_preview.delete()

                    LOGGER.info(f"Successfully deleted preview from S3 id={preview_id} for tenant {tenant_id}")
                except S3ModelNotFound:
                    LOGGER.info(f"Preview id={preview_id} not found in S3 for tenant {tenant_id}")

                # Delete from redis (legacy)
                rmm = RedisModelManager(
                    redis=self._ctx.redis_service.get_redis(),
                    crypto_keys_config=self._ctx.crypto_keys_config,
                )

                try:
                    redis_preview = await DataSourcePreview.get(manager=rmm, obj_id=preview_id)
                    assert isinstance(redis_preview, DataSourcePreview)

                    await redis_preview.delete()

                    LOGGER.info(f"Successfully deleted preview from redis id={preview_id} for tenant {tenant_id}")
                except RedisModelNotFound:
                    LOGGER.info(f"Preview id={preview_id} not found in redis for tenant {tenant_id}")

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
