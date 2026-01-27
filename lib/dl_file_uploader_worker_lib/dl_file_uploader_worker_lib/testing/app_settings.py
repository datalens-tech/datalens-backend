import pydantic

from dl_core.connectors.settings.base import ConnectorSettings
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings
import dl_settings


class TestingFileUploaderWorkerSettings(FileUploaderWorkerSettings):
    # Override CONNECTORS field without AfterValidator for testing purposes
    CONNECTORS: dl_settings.TypedDictWithTypeKeyAnnotation[ConnectorSettings] = pydantic.Field(default_factory=dict)
