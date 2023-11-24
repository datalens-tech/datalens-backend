from datetime import (
    datetime,
    timedelta,
)
import logging
from typing import (
    Any,
    Optional,
)

import attr
from botocore.exceptions import ClientError
from redis.asyncio.lock import Lock as RedisLock

from dl_constants.enums import DataSourceRole
from dl_core.us_entry import USMigrationEntry
from dl_file_uploader_lib.enums import RenameTenantStatus
from dl_file_uploader_lib.redis_model.base import (
    RedisModelManager,
    RedisModelNotFound,
)
from dl_file_uploader_lib.redis_model.models import (
    DataSourcePreview,
    PreviewSet,
    RenameTenantStatusModel,
)
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_file_uploader_worker_lib.tasks.save import make_source_s3_filename_suffix
from dl_task_processor.task import (
    BaseExecutorTask,
    Retry,
    Success,
    TaskResult,
)

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_DOCS
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE


LOGGER = logging.getLogger(__name__)


def make_s3_bucket_lifecycle_update_lock_key(bucket_name: str) -> str:
    return f"s3_bucket_lifecycle_update/{bucket_name}"


@attr.s
class CleanupTenantTask(BaseExecutorTask[task_interface.CleanupTenantTask, FileUploaderTaskContext]):
    cls_meta = task_interface.CleanupTenantTask

    async def run(self) -> TaskResult:
        try:
            tenant_id = self.meta.tenant_id
            obj_prefix = f"{tenant_id}_"
            LOGGER.info(f"CleanupTenantTask. tenant ID: {tenant_id}")

            s3_service = self._ctx.s3_service
            s3_client = s3_service.get_client()

            redis = self._ctx.redis_service.get_redis()
            lock_key = make_s3_bucket_lifecycle_update_lock_key(s3_service.persistent_bucket_name)
            LOGGER.info(f"Acquiring redis lock {lock_key}")
            async with RedisLock(redis, name=lock_key, timeout=120, blocking_timeout=120):
                LOGGER.info(f"Lock {lock_key} acquired")
                # lc == lifecycle
                try:
                    lc_config = await s3_client.get_bucket_lifecycle_configuration(
                        Bucket=s3_service.persistent_bucket_name,
                    )
                except ClientError as ex:
                    if ex.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                        lc_config = {"Rules": []}
                    else:
                        raise
                lc_rules = lc_config["Rules"]

                new_rule = {
                    "ID": obj_prefix,  # note: currently unused
                    "Expiration": {
                        "Date": datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2),
                    },
                    "Filter": {
                        "Prefix": obj_prefix,
                    },
                    "Status": "Enabled",
                }
                await s3_client.put_bucket_lifecycle_configuration(
                    Bucket=s3_service.persistent_bucket_name,
                    LifecycleConfiguration=dict(
                        Rules=lc_rules + [new_rule],
                    ),
                )

                LOGGER.info(f'Updated LC rules to remove objects with prefix "{obj_prefix}"')

            LOGGER.info(f"Released lock {lock_key}")

        except Exception as ex:
            LOGGER.exception(ex)
            return Retry(attempts=5)
        return Success()


@attr.s
class CleanS3LifecycleRulesTask(BaseExecutorTask[task_interface.CleanS3LifecycleRulesTask, FileUploaderTaskContext]):
    cls_meta = task_interface.CleanS3LifecycleRulesTask

    async def run(self) -> TaskResult:
        try:
            LOGGER.info("CleanS3LifecycleRulesTask")

            s3_service = self._ctx.s3_service
            s3_client = s3_service.get_client()

            async def prefix_exists(prefix: str) -> bool:
                list_resp = await s3_client.list_objects_v2(
                    Bucket=s3_service.persistent_bucket_name,
                    Prefix=prefix,
                    MaxKeys=1,
                )
                return bool(list_resp.get("Contents"))

            async def remove_old_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
                indices_to_remove = set()
                for idx, rule in enumerate(rules):
                    rule_prefix = rule["Filter"]["Prefix"]
                    if not await prefix_exists(rule_prefix):
                        LOGGER.info(f"Going to remove lifecycle rule for prefix {rule_prefix}")
                        indices_to_remove.add(idx)
                return [rule for idx, rule in enumerate(rules) if idx not in indices_to_remove]

            redis = self._ctx.redis_service.get_redis()
            lock_key = make_s3_bucket_lifecycle_update_lock_key(s3_service.persistent_bucket_name)
            LOGGER.info(f"Acquiring redis lock {lock_key}")
            async with RedisLock(redis, name=lock_key, timeout=120, blocking_timeout=120):
                LOGGER.info(f"Lock {lock_key} acquired")
                # lc == lifecycle
                try:
                    lc_config = await s3_client.get_bucket_lifecycle_configuration(
                        Bucket=s3_service.persistent_bucket_name
                    )
                except ClientError as ex:
                    if ex.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                        lc_config = {"Rules": []}
                    else:
                        raise
                lc_rules = lc_config["Rules"]

                new_lc_rules = await remove_old_rules(lc_rules)

                if len(new_lc_rules) > 750:
                    LOGGER.warning(f"Number of LC rules: {len(new_lc_rules)}")

                if new_lc_rules:
                    if len(new_lc_rules) < len(lc_rules):
                        await s3_client.put_bucket_lifecycle_configuration(
                            Bucket=s3_service.persistent_bucket_name,
                            LifecycleConfiguration=dict(
                                Rules=new_lc_rules,
                            ),
                        )
                else:
                    await s3_client.delete_bucket_lifecycle(Bucket=s3_service.persistent_bucket_name)

                LOGGER.info(f"Removed {len(lc_rules) - len(new_lc_rules)} LC rule(s)")

            LOGGER.info(f"Released lock {lock_key}")

        except Exception as ex:
            LOGGER.exception(ex)
            return Retry(attempts=5)
        return Success()


@attr.s
class CleanupTenantFilePreviewsTask(
    BaseExecutorTask[task_interface.CleanupTenantFilePreviewsTask, FileUploaderTaskContext]
):
    cls_meta = task_interface.CleanupTenantFilePreviewsTask

    async def run(self) -> TaskResult:
        try:
            tenant_id = self.meta.tenant_id
            LOGGER.info(f"CleanupTenantFilePreviewsTask. tenant ID: {tenant_id}")

            redis = self._ctx.redis_service.get_redis()
            rmm = RedisModelManager(
                redis=self._ctx.redis_service.get_redis(),
                crypto_keys_config=self._ctx.crypto_keys_config,
            )
            preview_set = PreviewSet(redis=redis, id=tenant_id)
            async for preview_id in preview_set.sscan_iter():
                try:
                    preview = await DataSourcePreview.get(manager=rmm, obj_id=preview_id)
                    await preview.delete()
                    LOGGER.info(f"Successfully deleted preview id={preview_id} for tenant {tenant_id}")
                except RedisModelNotFound:
                    LOGGER.info(f"Preview id={preview_id} not found for tenant {tenant_id}")

            LOGGER.info(f"Done with previews for tenant {tenant_id}, going to delete Redis Set {preview_set.key}")
            await preview_set.delete()

        except Exception as ex:
            LOGGER.exception(ex)
            return Retry(attempts=5)
        return Success()


@attr.s
class RenameTenantFilesTask(BaseExecutorTask[task_interface.RenameTenantFilesTask, FileUploaderTaskContext]):
    cls_meta = task_interface.RenameTenantFilesTask

    async def run(self) -> TaskResult:
        tenant_id = self.meta.tenant_id
        old_tenant_id = self.meta.old_tenant_id
        LOGGER.info(f"RenameTenantFilesTask. Moving to {tenant_id} (old_tenant_id = {old_tenant_id})")

        rmm = RedisModelManager(
            redis=self._ctx.redis_service.get_redis(),
            crypto_keys_config=self._ctx.crypto_keys_config,
        )
        await RenameTenantStatusModel(manager=rmm, id=tenant_id, status=RenameTenantStatus.started).save()
        try:
            usm = self._ctx.get_async_usm()
            usm.set_tenant_override(self._ctx.tenant_resolver.resolve_tenant_def_by_tenant_id(self.meta.tenant_id))
            s3_service = self._ctx.s3_service
            s3_client = s3_service.get_client()

            s3_file_based_conn_types = (
                CONNECTION_TYPE_FILE,
                CONNECTION_TYPE_GSHEETS_V2,
                CONNECTION_TYPE_DOCS,
            )

            redis = self._ctx.redis_service.get_redis()
            lock_key = f"rename_tenant_files_task/{tenant_id}"
            LOGGER.info(f"Acquiring redis lock {lock_key}")
            async with RedisLock(redis, name=lock_key, timeout=120, blocking_timeout=120):
                LOGGER.info(f"Lock {lock_key} acquired")

                old_tenants: set[str] = set()

                async with usm:
                    for conn_type in s3_file_based_conn_types:
                        conn_iter = usm.get_collection(
                            entry_cls=USMigrationEntry,
                            entry_type=conn_type.name,
                            entry_scope="connection",
                            raise_on_broken_entry=True,
                        )
                        async for conn_placeholder in conn_iter:
                            async with usm.locked_entry_cm(
                                conn_placeholder.uuid,
                                expected_type=BaseFileS3Connection,
                                wait_timeout_sec=10,
                                duration_sec=300,
                                force=True,
                            ) as conn:
                                assert isinstance(conn, BaseFileS3Connection)
                                conn_changed = False
                                for source in conn.data.sources:
                                    if source.s3_filename is None and source.s3_filename_suffix is None:
                                        LOGGER.info(
                                            f"Cannot rename file for source_id {source.id} - s3_filename not set"
                                        )
                                        continue

                                    old_s3_filename: str
                                    if source.s3_filename:
                                        old_s3_filename = source.s3_filename
                                    else:
                                        assert old_tenant_id
                                        old_s3_filename = "_".join(
                                            (old_tenant_id, conn.uuid, source.s3_filename_suffix)
                                        )

                                    if not old_tenant_id:
                                        old_fname_parts = old_s3_filename.split("_")
                                        if len(old_fname_parts) >= 2 and all(part for part in old_fname_parts):
                                            # assume that first part is old tenant id
                                            old_tenant_id = old_fname_parts[0]
                                    old_tenants.add(old_tenant_id)

                                    s3_filename_suffix = (
                                        source.s3_filename_suffix
                                        if source.s3_filename_suffix is not None
                                        else make_source_s3_filename_suffix()
                                    )
                                    new_s3_filename = conn.get_full_s3_filename(s3_filename_suffix)
                                    if old_s3_filename == new_s3_filename:
                                        LOGGER.info(f"The file already has the correct name: {new_s3_filename}")
                                        continue

                                    await s3_client.copy_object(
                                        CopySource=dict(
                                            Bucket=s3_service.persistent_bucket_name,
                                            Key=old_s3_filename,
                                        ),
                                        Bucket=s3_service.persistent_bucket_name,
                                        Key=new_s3_filename,
                                    )
                                    await s3_client.delete_object(
                                        Bucket=s3_service.persistent_bucket_name,
                                        Key=old_s3_filename,
                                    )
                                    LOGGER.info(f"Moved s3 file {old_s3_filename} -> {new_s3_filename}")

                                    conn_changed = True
                                    conn.update_data_source(
                                        source.id,
                                        role=DataSourceRole.origin,
                                        s3_filename=new_s3_filename,
                                        s3_filename_suffix=s3_filename_suffix,
                                    )

                                if conn_changed:
                                    await usm.save(conn)
                if old_tenants:
                    LOGGER.info(f"Moving redis old preview set[s]. From {old_tenants} to {tenant_id}")
                    await PreviewSet(redis=redis, id=tenant_id).sunion_by_id(*old_tenants)
                    for t_id in old_tenants:
                        await PreviewSet(redis=redis, id=t_id).delete()
        except Exception as ex:
            LOGGER.exception(ex)
            await RenameTenantStatusModel(manager=rmm, id=tenant_id, status=RenameTenantStatus.error).save()
            return Retry(attempts=10)

        await RenameTenantStatusModel(manager=rmm, id=tenant_id, status=RenameTenantStatus.success).save()
        return Success()
