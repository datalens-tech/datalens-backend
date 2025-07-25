import logging

import attr
from redis.asyncio.lock import Lock as RedisLock

from dl_file_uploader_lib.redis_model.base import (
    RedisModelManager,
    RedisModelNotFound,
)
from dl_file_uploader_lib.redis_model.models import (
    DataSourcePreview,
    PreviewSet,
)
from dl_file_uploader_lib.s3_model.base import S3ModelManager
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
class MigratePreviewRedisToS3Task(
    BaseExecutorTask[task_interface.MigratePreviewRedisToS3Task, FileUploaderTaskContext]
):
    cls_meta = task_interface.MigratePreviewRedisToS3Task

    async def run(self) -> TaskResult:
        try:
            source_lock_key = f"MigratePreviewRedisToS3Task/{tenant_id}/{preview_id}"
            LOGGER.info(f"Acquiring redis lock {source_lock_key}")
            async with RedisLock(redis, name=source_lock_key, timeout=120, blocking_timeout=120):
                LOGGER.info(f"Lock {source_lock_key} acquired")

                tenant_id = self.meta.tenant_id
                preview_id = self.meta.preview_id

                if tenant_id is None:
                    LOGGER.info(f"Unable to migrate preview since {tenant_id=}")
                    raise ValueError("tenant_id is None")

                if preview_id is None:
                    LOGGER.info(f"Unable to migrate preview since {preview_id=}")
                    raise ValueError("preview_id is None")

                redis = self._ctx.redis_service.get_redis()
                rmm = RedisModelManager(
                    redis=redis,
                    crypto_keys_config=self._ctx.crypto_keys_config,
                )
                s3mm = S3ModelManager(
                    s3_service=self._ctx.s3_service,
                    crypto_keys_config=self._ctx.crypto_keys_config,
                    tenant_id=tenant_id,
                )

                try:
                    redis_preview = await DataSourcePreview.get(manager=rmm, obj_id=preview_id)
                    assert isinstance(redis_preview, DataSourcePreview)

                    s3_preview = S3DataSourcePreview(
                        manager=s3mm, preview_data=redis_preview.preview_data, id=redis_preview.id
                    )
                    await s3_preview.save(persistent=await redis_preview.is_persistent())

                    await redis_preview.delete()

                    preview_set = PreviewSet(redis=redis, id=tenant_id)
                    await preview_set.remove(preview_id)

                    LOGGER.info(
                        f"Successfully migrated preview id={preview_id} for tenant {tenant_id} from redis to s3"
                    )
                except RedisModelNotFound:
                    LOGGER.info(f"Preview id={preview_id} not found in redis for tenant {tenant_id}")

        except Exception as ex:
            LOGGER.exception(ex)
            return Retry(attempts=5)
        return Success()
