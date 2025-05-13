from dl_task_processor.task import TaskRegistry

from .cleanup import (
    CleanS3LifecycleRulesTask,
    CleanupTenantFilePreviewsTask,
    CleanupTenantTask,
    RenameTenantFilesTask,
)
from .delete import DeleteFileTask
from .download_gsheets import DownloadGSheetTask
from .download_yadocs import DownloadYaDocsTask
from .excel import ProcessExcelTask
from .migrate_preview import MigratePreviewRedisToS3Task
from .parse import ParseFileTask
from .save import SaveSourceTask


REGISTRY: TaskRegistry = TaskRegistry.create(
    [
        DownloadGSheetTask,
        DownloadYaDocsTask,
        ParseFileTask,
        ProcessExcelTask,
        SaveSourceTask,
        DeleteFileTask,
        CleanupTenantTask,
        CleanS3LifecycleRulesTask,
        CleanupTenantFilePreviewsTask,
        RenameTenantFilesTask,
        MigratePreviewRedisToS3Task,
    ]
)

__all__ = ("REGISTRY",)
