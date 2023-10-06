import asyncio
import datetime
import logging
from typing import (
    Any,
    Optional,
)

import attr
from redis.asyncio.lock import Lock as RedisLock
import shortuuid

from dl_constants.enums import (
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core import exc as core_exc
from dl_core.aio.web_app_services.s3 import S3Service
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.db import SchemaColumn
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_file_uploader_lib.common_locks import release_source_update_locks
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    DataSourcePreview,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
    PreviewSet,
)
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
import dl_file_uploader_task_interface.tasks as task_interface
from dl_file_uploader_task_interface.tasks import TaskExecutionMode
from dl_file_uploader_worker_lib.utils.s3_utils import (
    S3Object,
    copy_from_s3_to_s3,
    make_s3_table_func_sql_source,
)
from dl_task_processor.task import (
    BaseExecutorTask,
    Fail,
    Retry,
    Success,
    TaskResult,
)
from dl_utils.aio import ContextVarExecutor

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection


LOGGER = logging.getLogger(__name__)


def _make_source_s3_filename(tenant_id: str) -> str:
    return f"{tenant_id}_{shortuuid.uuid()}"


def _make_tmp_source_s3_filename(source_id: str) -> str:
    return f"{source_id}-{shortuuid.uuid()}.json"


async def _prepare_file(
    s3_service: S3Service,
    tpe: ContextVarExecutor,
    dfile: DataFile,
    src_source: DataSource,
    dst_source_id: str,
    conn_raw_schema: list[SchemaColumn],
) -> str:
    src_filename = dfile.s3_key if dfile.file_type == FileType.csv else src_source.s3_key

    tmp_s3_filename = _make_tmp_source_s3_filename(dst_source_id)

    def do_s3_copy_sync() -> None:
        assert src_source.file_source_settings is not None and dfile.file_type is not None
        copy_from_s3_to_s3(
            s3_sync_cli=s3_service.get_sync_client(),
            src_file=S3Object(s3_service.tmp_bucket_name, src_filename),
            dst_file=S3Object(s3_service.tmp_bucket_name, tmp_s3_filename),
            file_type=dfile.file_type,
            file_settings=dfile.file_settings,
            file_source_settings=src_source.file_source_settings,
            raw_schema=conn_raw_schema,
        )

    loop = asyncio.get_running_loop()
    LOGGER.info("Copying data from user file to tmp s3 file (in JSONCompactEachRow format)")
    await loop.run_in_executor(tpe, do_s3_copy_sync)

    return tmp_s3_filename


def _get_conn_specific_dsrc_params(dfile: DataFile, src: DataSource) -> dict[str, Any]:
    kwargs = {}
    if dfile.file_type == FileType.gsheets:
        file_source_settings = src.file_source_settings
        assert isinstance(file_source_settings, GSheetsFileSourceSettings)
        user_source_properties = dfile.user_source_properties
        assert isinstance(user_source_properties, GSheetsUserSourceProperties)
        user_source_dsrc_properties = src.user_source_dsrc_properties
        assert isinstance(user_source_dsrc_properties, GSheetsUserSourceDataSourceProperties)

        kwargs.update(
            dict(
                spreadsheet_id=user_source_properties.spreadsheet_id,
                sheet_id=user_source_dsrc_properties.sheet_id,
                first_line_is_header=file_source_settings.first_line_is_header,
            )
        )
    return kwargs


@attr.s
class SaveSourceTask(BaseExecutorTask[task_interface.SaveSourceTask, FileUploaderTaskContext]):
    cls_meta = task_interface.SaveSourceTask

    async def run(self) -> TaskResult:
        try:
            LOGGER.info(f"SaveSourceTask. Mode: {self.meta.exec_mode.name}. File: {self.meta.file_id}")
            if self.meta.exec_mode == TaskExecutionMode.UPDATE_NO_SAVE:
                LOGGER.warning(f"Cannot run SaveSourceTask in {self.meta.exec_mode.name} mode, failing the task")
                return Fail()

            redis = self._ctx.redis_service.get_redis()
            rmm = RedisModelManager(redis=redis, crypto_keys_config=self._ctx.crypto_keys_config)
            dfile = await DataFile.get(manager=rmm, obj_id=self.meta.file_id)

            src_source_id, dst_source_id = self.meta.src_source_id, self.meta.dst_source_id
            src_source = dfile.get_source_by_id(src_source_id)

            # TODO: init all this stuff in a proper place, not in task
            rci = self._ctx.get_rci()
            usm = self._ctx.get_async_usm(rci=rci)
            service_registry = self._ctx.get_service_registry(rci=rci)
            release_update_source_lock_flag = False
            async with usm:
                source_lock_key = f"SaveSourceTask/{dst_source_id}"
                LOGGER.info(f"Acquiring redis lock {source_lock_key}")
                async with RedisLock(redis, name=source_lock_key, timeout=120, blocking_timeout=120):
                    LOGGER.info(f"Lock {source_lock_key} acquired")
                    assert isinstance(usm, AsyncUSManager)
                    conn = await usm.get_by_id(self.meta.connection_id, expected_type=BaseFileS3Connection)
                    assert isinstance(conn, BaseFileS3Connection)

                    preview = await DataSourcePreview.get(manager=rmm, obj_id=str(src_source.preview_id))
                    await preview.save(ttl=None)  # now that the source is saved the preview can be saved without ttl
                    preview_set = PreviewSet(redis=redis, id=self.meta.tenant_id)
                    await preview_set.add(preview.id)

                    try:
                        conn_file_source = conn.get_file_source_by_id(dst_source_id)
                        if (
                            conn_file_source.status == FileProcessingStatus.ready
                            and self.meta.exec_mode != TaskExecutionMode.UPDATE_AND_SAVE
                        ):
                            LOGGER.info(
                                f'Source {dst_source_id} in connection {conn.uuid} has status "ready". Finishing task.'
                            )
                            return Success()
                        conn_raw_schema: list[SchemaColumn] = conn_file_source.raw_schema or []
                        orig_file_id = conn_file_source.file_id
                        orig_s3_filename = conn_file_source.s3_filename
                        orig_preview_id = conn_file_source.preview_id
                    except core_exc.SourceDoesNotExist:
                        LOGGER.info(f"Source {dst_source_id} not present in connection {conn.uuid}. Finishing task.")
                        return Success()

                    s3_service = self._ctx.s3_service

                    raw_schema_override: Optional[list[SchemaColumn]]
                    if self.meta.exec_mode == TaskExecutionMode.UPDATE_AND_SAVE:
                        raw_schema_override = src_source.raw_schema
                    else:
                        raw_schema_override = None

                    tmp_s3_filename = await _prepare_file(
                        s3_service,
                        self._ctx.tpe,
                        dfile,
                        src_source,
                        dst_source_id,
                        raw_schema_override if raw_schema_override is not None else conn_raw_schema,
                    )

                    new_s3_filename = _make_source_s3_filename(tenant_id=self.meta.tenant_id)

                    def _construct_insert_from_select_query(for_debug: bool = False) -> str:
                        src_sql = make_s3_table_func_sql_source(
                            conn=conn,
                            source_id=dst_source_id,
                            bucket=s3_service.tmp_bucket_name,
                            filename=tmp_s3_filename,
                            file_fmt="JSONCompactEachRow",
                            for_debug=for_debug,
                            raw_schema_override=raw_schema_override,
                        )
                        dst_sql = make_s3_table_func_sql_source(
                            conn=conn,
                            source_id=dst_source_id,
                            bucket=s3_service.persistent_bucket_name,
                            filename=new_s3_filename,
                            file_fmt="Native",
                            for_debug=for_debug,
                            raw_schema_override=raw_schema_override,
                        )
                        return f"insert into FUNCTION {dst_sql} select * from {src_sql}"

                    ce_query = ConnExecutorQuery(
                        query=_construct_insert_from_select_query(),
                        debug_compiled_query=_construct_insert_from_select_query(for_debug=True),
                        trusted_query=True,
                        is_ddl_dml_query=True,
                    )

                    LOGGER.info(
                        "Running INSERT FROM SELECT clickhouse query " "to transform S3 data to CH Native format."
                    )
                    conn_executor_factory = service_registry.get_conn_executor_factory()
                    async_conn_executor = conn_executor_factory.get_async_conn_executor(conn)
                    exec_result = await async_conn_executor.execute(ce_query)
                    LOGGER.info(exec_result)

                    async with usm.locked_entry_cm(
                        self.meta.connection_id,
                        expected_type=BaseFileS3Connection,
                        wait_timeout_sec=60,
                        duration_sec=10,
                    ) as conn:
                        assert isinstance(conn, BaseFileS3Connection)
                        try:
                            conn.get_file_source_by_id(id=dst_source_id)
                            conn_file_source = conn.get_file_source_by_id(id=dst_source_id)
                        except core_exc.SourceDoesNotExist:
                            LOGGER.info(
                                f"Source {dst_source_id} not present in connection {conn.uuid}. Finishing task."
                            )
                            conn.data.component_errors.remove_errors(id=dst_source_id)
                            return Success()

                        if (
                            conn_file_source.status == FileProcessingStatus.ready
                            and self.meta.exec_mode != TaskExecutionMode.UPDATE_AND_SAVE
                        ):
                            LOGGER.info(
                                f'Source {dst_source_id} in connection {conn.uuid} already has status "ready". '
                                "Something strange, considering present lock. "
                                "Failing task."
                            )
                            return Fail()

                        extra_dsrc_params = _get_conn_specific_dsrc_params(dfile, src_source)
                        if self.meta.exec_mode == TaskExecutionMode.UPDATE_AND_SAVE:
                            extra_dsrc_params["data_updated_at"] = datetime.datetime.now(datetime.timezone.utc)
                            release_update_source_lock_flag = True
                            extra_dsrc_params["raw_schema"] = raw_schema_override
                            extra_dsrc_params["file_id"] = dfile.id

                        conn.update_data_source(
                            dst_source_id,
                            role=DataSourceRole.origin,
                            s3_filename=new_s3_filename,
                            status=FileProcessingStatus.ready,
                            preview_id=preview.id,
                            **extra_dsrc_params,
                        )
                        conn.data.component_errors.remove_errors(id=dst_source_id)
                        await usm.save(conn)

                    # sync source id with the connection to enable consistent work with dfile (e.g. polling)
                    src_source.id = dst_source_id
                    await dfile.save()

                    if orig_file_id != self.meta.file_id and orig_s3_filename is not None:
                        if self.meta.exec_mode == TaskExecutionMode.UPDATE_AND_SAVE:
                            delete_file_task = task_interface.DeleteFileTask(
                                s3_filename=orig_s3_filename,
                                preview_id=orig_preview_id,
                            )
                            task_processor = self._ctx.make_task_processor(self._request_id)
                            await task_processor.schedule(delete_file_task)
                        else:
                            LOGGER.warning(
                                f"file_id in connection source ({orig_file_id}) and passed file_id"
                                f" ({self.meta.file_id}) are different and execution mode is"
                                f" {self.meta.exec_mode.name}, expected it to be"
                                f" {TaskExecutionMode.UPDATE_AND_SAVE.name}"
                            )

                LOGGER.info(f"Released lock {source_lock_key}")

                if release_update_source_lock_flag:
                    await release_source_update_locks(redis, dst_source_id)

        except Exception as ex:
            LOGGER.exception(ex)
            return Retry(attempts=5)
        return Success()
