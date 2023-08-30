from bi_task_processor.task import TaskRegistry

from .download import DownloadGSheetTask
from .parse import ParseFileTask
from .excel import ProcessExcelTask
from .save import SaveSourceTask
from .delete import DeleteFileTask
from .cleanup import (
    CleanupTenantTask,
    CleanupTenantFilePreviewsTask,
    CleanS3LifecycleRulesTask,
    RenameTenantFilesTask,
)


REGISTRY: TaskRegistry = TaskRegistry.create([
    DownloadGSheetTask,
    ParseFileTask,
    ProcessExcelTask,
    SaveSourceTask,
    DeleteFileTask,
    CleanupTenantTask,
    CleanS3LifecycleRulesTask,
    CleanupTenantFilePreviewsTask,
    RenameTenantFilesTask,
])

__all__ = (
    'REGISTRY',
)
