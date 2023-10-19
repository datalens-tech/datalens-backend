from __future__ import annotations

import datetime
import logging
from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_constants.enums import (
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core.services_registry.file_uploader_client_factory import GSheetsFileSourceDesc
from dl_utils.utils import DataKey

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import SOURCE_TYPE_GSHEETS_V2


LOGGER = logging.getLogger(__name__)


class GSheetsFileS3Connection(BaseFileS3Connection):
    source_type = SOURCE_TYPE_GSHEETS_V2
    allowed_source_types = frozenset((SOURCE_TYPE_GSHEETS_V2,))

    editable_data_source_parameters: ClassVar[
        tuple[str, ...]
    ] = BaseFileS3Connection.editable_data_source_parameters + (
        "spreadsheet_id",
        "sheet_id",
        "first_line_is_header",
        "data_updated_at",
    )

    @attr.s(eq=False, kw_only=True)
    class FileDataSource(BaseFileS3Connection.FileDataSource):
        spreadsheet_id: Optional[str] = attr.ib(default=None)
        sheet_id: Optional[int] = attr.ib(default=None)
        first_line_is_header: Optional[bool] = attr.ib(default=None)
        data_updated_at: datetime.datetime = attr.ib(factory=lambda: datetime.datetime.now(datetime.timezone.utc))

        def __hash__(self) -> int:
            raw_schema = tuple(self.raw_schema) if self.raw_schema is not None else tuple()
            return hash(
                (
                    self.id,
                    self.file_id,
                    self.title,
                    self.s3_filename,
                    self.s3_filename_suffix,
                    raw_schema,
                    self.status,
                    self.sheet_id,
                    self.spreadsheet_id,
                )
            )

        def str_for_hash(self) -> str:
            return ",".join(
                [
                    super().str_for_hash(),
                    str(self.spreadsheet_id),
                    str(self.sheet_id),
                ]
            )

        def get_desc(self) -> GSheetsFileSourceDesc:
            return GSheetsFileSourceDesc(
                file_id=self.file_id,
                source_id=self.id,
                title=self.title,
                raw_schema=self.raw_schema,
                preview_id=self.preview_id,
                spreadsheet_id=self.spreadsheet_id,
                sheet_id=self.sheet_id,
                first_line_is_header=self.first_line_is_header,
            )

    @attr.s(eq=False, kw_only=True)
    class DataModel(BaseFileS3Connection.DataModel):
        sources: list["GSheetsFileS3Connection.FileDataSource"] = attr.ib()  # type: ignore

        refresh_token: Optional[str] = attr.ib(default=None, repr=False)
        refresh_enabled: bool = attr.ib(default=False)

        def oldest_data_update_time(
            self, exclude_statuses: Optional[set[FileProcessingStatus]] = None
        ) -> Optional[datetime.datetime]:
            if exclude_statuses is None:
                exclude_statuses = set()

            data_updated_list = [src.data_updated_at for src in self.sources if src.status not in exclude_statuses]
            if not data_updated_list:
                return None

            return min(data_updated_list)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {DataKey(parts=("refresh_token",))}

    data: DataModel

    @property
    def authorized(self) -> bool:
        return self.data.refresh_token is not None

    def restore_source_params_from_orig(self, src_id: str, original_version: BaseFileS3Connection) -> None:
        orig_src = original_version.get_file_source_by_id(src_id)
        assert isinstance(orig_src, GSheetsFileS3Connection.FileDataSource)
        self.update_data_source(
            src_id,
            role=DataSourceRole.origin,
            raw_schema=orig_src.raw_schema,
            file_id=orig_src.file_id,
            s3_filename=orig_src.s3_filename,
            s3_filename_suffix=orig_src.s3_filename_suffix,
            status=orig_src.status,
            preview_id=orig_src.preview_id,
            first_line_is_header=orig_src.first_line_is_header,
            spreadsheet_id=orig_src.spreadsheet_id,
            sheet_id=orig_src.sheet_id,
        )

    @property
    def allow_public_usage(self) -> bool:
        return True
