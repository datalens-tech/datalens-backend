import datetime
import logging
from typing import Optional

import attr
import redis.asyncio

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_constants.enums import (
    ComponentErrorLevel,
    ComponentType,
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core import exc as core_exc
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_file_uploader_lib.common_locks import release_source_update_locks
from dl_file_uploader_lib.redis_model.models import FileProcessingError
from dl_file_uploader_task_interface.tasks import (
    DeleteFileTask,
    TaskExecutionMode,
)
from dl_task_processor.processor import TaskProcessor

LOGGER = logging.getLogger(__name__)


@attr.s
class FileConnectionDataSourceErrorTracker:
    """
    Stores errors that need to be dumped into connection, fails connection sources on `finalize`
    """

    _usm: AsyncUSManager = attr.ib()
    _task_processor: TaskProcessor = attr.ib()
    _redis: redis.asyncio.Redis = attr.ib()
    _request_id: Optional[str] = attr.ib(default=None)

    _error_registry: dict[str, FileProcessingError] = attr.ib(init=False, factory=dict)

    def add_error(self, source_id: str, err: FileProcessingError) -> None:
        """
        :param source_id: id of a source in a connection
        :param err: error to store, if an error for this source is already stored, do nothing
        """

        if (existing_error := self._error_registry.get(source_id)) is not None:
            LOGGER.warning(
                f"Attempt to overwrite existing error for source {source_id}"
                f"({existing_error.code}) with a new one: {err.code}"
            )
            return
        err.details["request-id"] = self._request_id
        self._error_registry[source_id] = err

    def clear(self) -> None:
        self._error_registry.clear()

    async def _release_source_update_locks(self) -> None:
        await release_source_update_locks(self._redis, *self._error_registry.keys())

    async def _fail_connection_sources(self, connection_id: str) -> None:
        delete_tasks: list[DeleteFileTask] = []

        async with self._usm.locked_entry_cm(
            connection_id,
            expected_type=BaseFileS3Connection,
            wait_timeout_sec=60,
            duration_sec=10,
        ) as conn:
            assert isinstance(conn, BaseFileS3Connection)
            for source_id, error in self._error_registry.items():
                reason = ".".join(error.code)
                try:
                    err_level = ComponentErrorLevel(error.level.name)
                except ValueError:
                    err_level = ComponentErrorLevel.error

                try:
                    src = conn.get_file_source_by_id(source_id)
                except core_exc.SourceDoesNotExist:
                    LOGGER.info(f"Source {source_id} not present in connection {conn.uuid}. No need to fail source.")
                    conn.data.component_errors.remove_errors(id=source_id)
                    continue

                LOGGER.info(
                    f"Going to fail source id={source_id} in connection id={connection_id}"
                    f' due to reason "{reason}" with level "{err_level}"'
                )

                if err_level == ComponentErrorLevel.error:
                    if src.s3_filename is not None:
                        delete_tasks.append(
                            DeleteFileTask(
                                s3_filename=src.s3_filename,
                                preview_id=src.preview_id,
                            )
                        )

                    conn.update_data_source(
                        source_id,
                        role=DataSourceRole.origin,
                        s3_filename=None,
                        status=FileProcessingStatus.failed,
                        preview_id=None,
                        data_updated_at=datetime.datetime.now(datetime.timezone.utc),
                    )
                else:
                    conn.update_data_source(
                        source_id,
                        role=DataSourceRole.origin,
                        status=FileProcessingStatus.ready,
                        data_updated_at=datetime.datetime.now(datetime.timezone.utc),
                    )

                conn.data.component_errors.remove_errors(id=source_id)  # there can only be one error at a time
                conn.data.component_errors.add_error(
                    id=source_id,
                    type=ComponentType.data_source,
                    message=error.message,
                    code=error.code,
                    level=err_level,
                    details=error.details,
                )

            await self._usm.save(conn)

        for delete_file_task in delete_tasks:
            await self._task_processor.schedule(delete_file_task)
            LOGGER.info(f"Scheduled task DeleteFileTask for source_id {src.id}, filename {src.s3_filename}")

    async def finalize(self, mode: TaskExecutionMode, connection_id: Optional[str] = None) -> None:
        """
        Deals with errors according to `mode`:
            - BASIC - do nothing
            - UPDATE_NO_SAVE - release locks (does not require connection_id)
            - UPDATE_AND_SAVE - update connection with new errors and release locks
        """

        if mode not in (TaskExecutionMode.UPDATE_AND_SAVE, TaskExecutionMode.UPDATE_NO_SAVE):
            LOGGER.info(f"Nothing to do with errors for mode {mode}")
            return
        if mode == TaskExecutionMode.UPDATE_AND_SAVE:
            assert connection_id is not None, "No connection_id to fail sources in"
            await self._fail_connection_sources(connection_id)
        await self._release_source_update_locks()
        self.clear()
