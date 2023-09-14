from bi_task_processor.task import TaskRegistry

from .cleanup import (
    CleanS3LifecycleRulesTask,
    CleanupTenantFilePreviewsTask,
    CleanupTenantTask,
    RenameTenantFilesTask,
)
from .delete import DeleteFileTask
from .download import DownloadGSheetTask
from .excel import ProcessExcelTask
from .parse import ParseFileTask
from .save import SaveSourceTask

REGISTRY: TaskRegistry = TaskRegistry.create(
    [
        DownloadGSheetTask,
        ParseFileTask,
        ProcessExcelTask,
        SaveSourceTask,
        DeleteFileTask,
        CleanupTenantTask,
        CleanS3LifecycleRulesTask,
        CleanupTenantFilePreviewsTask,
        RenameTenantFilesTask,
    ]
)

__all__ = ("REGISTRY",)
