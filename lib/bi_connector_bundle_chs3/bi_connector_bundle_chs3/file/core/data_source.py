from __future__ import annotations

from bi_constants.enums import CreateDSFrom, ConnectionType

from bi_connector_bundle_chs3.chs3_base.core.data_source import BaseFileS3DataSource


class FileS3DataSource(BaseFileS3DataSource):
    conn_type = ConnectionType.file

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            CreateDSFrom.FILE_S3_TABLE,
        }
