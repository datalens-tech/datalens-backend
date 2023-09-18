from __future__ import annotations

import logging

import attr

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.file.core.constants import SOURCE_TYPE_FILE_S3_TABLE
from dl_constants.enums import DataSourceRole
from dl_core.services_registry.file_uploader_client_factory import FileSourceDesc

LOGGER = logging.getLogger(__name__)


class FileS3Connection(BaseFileS3Connection):
    source_type = SOURCE_TYPE_FILE_S3_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_FILE_S3_TABLE,))

    @attr.s(eq=False, kw_only=True)
    class FileDataSource(BaseFileS3Connection.FileDataSource):
        column_types: list[dict] = attr.ib(factory=list)  # should not be saved in US

        def get_desc(self) -> FileSourceDesc:
            return FileSourceDesc(
                file_id=self.file_id,
                source_id=self.id,
                title=self.title,
                raw_schema=self.column_types or self.raw_schema,
                preview_id=self.preview_id,
            )

    def restore_source_params_from_orig(self, src_id: str, original_version: BaseFileS3Connection) -> None:
        orig_src = original_version.get_file_source_by_id(src_id)
        assert isinstance(orig_src, FileS3Connection.FileDataSource)
        self.update_data_source(
            src_id,
            role=DataSourceRole.origin,
            raw_schema=orig_src.raw_schema,
            file_id=orig_src.file_id,
            s3_filename=orig_src.s3_filename,
            status=orig_src.status,
            preview_id=orig_src.preview_id,
        )

    @property
    def allow_public_usage(self) -> bool:
        return True
