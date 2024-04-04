import logging
from typing import (
    Iterable,
    Optional,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connectors.base.lifecycle import ConnectionLifecycleManager
from dl_file_uploader_task_interface.tasks import (
    DeleteFileTask,
    SaveSourceTask,
)
from dl_task_processor.processor import TaskProcessor
from dl_utils.aio import await_sync

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection


LOGGER = logging.getLogger(__name__)


@attr.s
class FileConnTaskScheduler:
    _task_processor: TaskProcessor = attr.ib(kw_only=True)
    _rci: RequestContextInfo = attr.ib(kw_only=True)

    def schedule_sources_delete(
        self, conn: BaseFileS3Connection, source_to_del: Optional[Iterable[str]] = None
    ) -> None:
        """Removes all _saved_sources if `sources_to_del` is not specified"""

        if source_to_del is None:
            source_to_del = (src.id for src in conn._saved_sources or [])

        LOGGER.info("Going to schedule DeleteFileTask for sources %s", source_to_del)
        for src_id in source_to_del:
            source = conn.get_saved_source_by_id(src_id)

            s3_filename: str | None
            if source.s3_filename_suffix is not None:
                s3_filename = conn.get_full_s3_filename(source.s3_filename_suffix)
            else:
                # TODO: Remove this fallback after old connections migration to s3_filename_suffix
                s3_filename = source.s3_filename

            if s3_filename is None:
                LOGGER.warning(f"Cannot schedule file deletion for source_id {source.id} - s3_filename not set")
                continue
            task = DeleteFileTask(
                s3_filename=s3_filename,
                preview_id=source.preview_id,
            )
            task_instance = await_sync(self._task_processor.schedule(task))
            LOGGER.info(
                f"Scheduled task DeleteFileTask for source_id {source.id}, filename {s3_filename}. "
                f"instance_id: {task_instance.instance_id}"
            )

    def schedule_sources_update(self, conn: BaseFileS3Connection) -> None:
        saved_sources = set(src.id for src in (conn._saved_sources or []))
        current_sources = set(src.id for src in conn.data.sources)
        sources_to_add = current_sources - saved_sources
        sources_to_del = saved_sources - current_sources
        sources_to_replace = {
            upd_src["old_source_id"]: upd_src["new_source_id"] for upd_src in conn.data.replace_sources
        }
        assert self._rci.tenant is not None

        LOGGER.info("Going to schedule SaveSourceTask for sources %s", sources_to_add)
        for src_id in sources_to_add:
            source = conn.get_file_source_by_id(src_id)
            src_source_id = sources_to_replace.get(source.id, source.id)
            task = SaveSourceTask(
                tenant_id=self._rci.tenant.get_tenant_id(),
                file_id=source.file_id,
                src_source_id=src_source_id,
                dst_source_id=source.id,
                connection_id=conn.uuid,  # type: ignore  # 2024-01-30 # TODO: Argument "connection_id" to "SaveSourceTask" has incompatible type "str | None"; expected "str"  [arg-type]
            )
            task_instance = await_sync(self._task_processor.schedule(task))
            LOGGER.info(
                f"Scheduled task SaveSourceTask for source_id {source.id}. instance_id: {task_instance.instance_id}"
            )
        self.schedule_sources_delete(conn, sources_to_del)


class BaseFileS3ConnectionLifecycleManager(ConnectionLifecycleManager[BaseFileS3Connection]):
    ENTRY_CLS = BaseFileS3Connection

    def get_task_processor(self, req_id: Optional[str] = None) -> TaskProcessor:
        task_processor_factory = self._service_registry.get_task_processor_factory()
        task_processor = task_processor_factory.make(req_id)
        return task_processor

    def post_save_hook(self) -> None:
        super().post_save_hook()

        rci = self._us_manager.bi_context
        task_processor = self.get_task_processor(rci.request_id)
        scheduler = FileConnTaskScheduler(task_processor=task_processor, rci=rci)
        scheduler.schedule_sources_update(self.entry)

    def post_delete_hook(self) -> None:
        super().post_delete_hook()

        rci = self._us_manager.bi_context
        task_processor = self.get_task_processor(rci.request_id)
        scheduler = FileConnTaskScheduler(task_processor=task_processor, rci=rci)
        scheduler.schedule_sources_delete(self.entry)

    async def post_init_async_hook(self) -> None:
        await super().post_init_async_hook()

        self.entry._saved_sources = self.entry.data.sources
