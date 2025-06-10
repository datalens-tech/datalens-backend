import enum
from typing import Optional

import attr

from dl_task_processor.task import (
    BaseTaskMeta,
    TaskName,
)


class TaskExecutionMode(enum.Enum):
    BASIC = enum.auto()
    UPDATE_NO_SAVE = enum.auto()
    UPDATE_AND_SAVE = enum.auto()


@attr.s
class DownloadGSheetTask(BaseTaskMeta):
    name = TaskName("download_gsheet")

    file_id: str = attr.ib()
    authorized: bool = attr.ib()

    tenant_id: Optional[str] = attr.ib(default=None)
    connection_id: Optional[str] = attr.ib(default=None)
    exec_mode: TaskExecutionMode = attr.ib(default=TaskExecutionMode.BASIC)
    schedule_parsing: bool = attr.ib(default=True)


@attr.s
class DownloadYaDocsTask(BaseTaskMeta):
    name = TaskName("download_yadocs")

    file_id: str = attr.ib()
    authorized: bool = attr.ib()

    tenant_id: Optional[str] = attr.ib(default=None)
    connection_id: Optional[str] = attr.ib(default=None)
    exec_mode: TaskExecutionMode = attr.ib(default=TaskExecutionMode.BASIC)


@attr.s
class ParseFileTask(BaseTaskMeta):
    name = TaskName("parse_file")

    file_id: str = attr.ib()

    tenant_id: Optional[str] = attr.ib(default=None)
    source_id: Optional[str] = attr.ib(default=None)
    connection_id: Optional[str] = attr.ib(default=None)
    exec_mode: TaskExecutionMode = attr.ib(default=TaskExecutionMode.BASIC)
    file_settings: Optional[dict] = attr.ib(factory=dict)
    source_settings: Optional[dict] = attr.ib(factory=dict)


@attr.s
class ProcessExcelTask(BaseTaskMeta):
    name = TaskName("process_excel")

    file_id: str = attr.ib()
    tenant_id: Optional[str] = attr.ib(default=None)
    connection_id: Optional[str] = attr.ib(default=None)
    exec_mode: Optional[TaskExecutionMode] = attr.ib(default=TaskExecutionMode.BASIC)


@attr.s
class SaveSourceTask(BaseTaskMeta):
    """
    src_source_id -- the one that is supposed to be found in the DataFile.
    dst_source_id -- the one that is supposed to be saved in the Connection.

    This plays its role when replacing file sources since at this point (on save)
    source IDs in the DataFile and in the Connection are not the same for the same file.

    src_source_id == dst_source_id when the source is being saved for the 1st time
    """

    name = TaskName("save_source")

    tenant_id: str = attr.ib()
    file_id: str = attr.ib()
    src_source_id: str = attr.ib()
    dst_source_id: str = attr.ib()
    connection_id: str = attr.ib()

    exec_mode: TaskExecutionMode = attr.ib(default=TaskExecutionMode.BASIC)


@attr.s
class DeleteFileTask(BaseTaskMeta):
    name = TaskName("delete_file")

    s3_filename: str = attr.ib()
    tenant_id: Optional[str] = attr.ib(default=None)
    preview_id: Optional[str] = attr.ib(default=None)


@attr.s
class MigratePreviewRedisToS3Task(BaseTaskMeta):
    name = TaskName("migrate_preview")

    tenant_id: Optional[str] = attr.ib(default=None)
    preview_id: Optional[str] = attr.ib()


@attr.s
class CleanupTenantTask(BaseTaskMeta):
    name = TaskName("cleanup")

    tenant_id: str = attr.ib()


@attr.s
class CleanS3LifecycleRulesTask(BaseTaskMeta):
    name = TaskName("regular_bucket_lifecycle_cleanup")


@attr.s
class CleanupTenantFilePreviewsTask(BaseTaskMeta):
    name = TaskName("cleanup_previews")

    tenant_id: str = attr.ib()


@attr.s(kw_only=True)
class RenameTenantFilesTask(BaseTaskMeta):
    name = TaskName("rename_tenant_files_task")

    old_tenant_id: Optional[str] = attr.ib(default=None)
    tenant_id: str = attr.ib()
