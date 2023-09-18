from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.core.data_source import BaseFileS3DataSource
from dl_connector_bundle_chs3.file.core.constants import (
    CONNECTION_TYPE_FILE,
    SOURCE_TYPE_FILE_S3_TABLE,
)
from dl_constants.enums import CreateDSFrom


class FileS3DataSource(BaseFileS3DataSource):
    conn_type = CONNECTION_TYPE_FILE

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_FILE_S3_TABLE,
        }
