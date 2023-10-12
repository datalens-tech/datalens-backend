from __future__ import annotations

import csv
import time
from typing import (
    ClassVar,
    Optional,
)
import uuid

import attr

from dl_constants.enums import FileProcessingStatus
from dl_core.db.elements import SchemaColumn
from dl_file_uploader_lib.enums import (
    CSVDelimiter,
    CSVEncoding,
    ErrorLevel,
    FileType,
    RenameTenantStatus,
)
from dl_file_uploader_lib.exc import DLFileUploaderBaseError
from dl_file_uploader_lib.redis_model.base import (
    RedisModel,
    RedisModelUserIdAuth,
    RedisSetManager,
    SecretContainingMixin,
)
from dl_utils.utils import DataKey


class DataFileError(Exception):
    pass


class SourceNotFoundError(DataFileError):
    pass


class EmptySourcesError(DataFileError):
    pass


@attr.s(init=True, kw_only=True)
class FileSettings:
    file_type: FileType = attr.ib()


@attr.s(init=True, kw_only=True)
class CSVFileSettings(FileSettings):
    file_type: FileType = attr.ib(default=FileType.csv)

    encoding: CSVEncoding = attr.ib()
    dialect: csv.Dialect = attr.ib()

    @property
    def delimiter(self) -> CSVDelimiter:
        return CSVDelimiter(self.dialect.delimiter)


@attr.s(init=True, kw_only=True)
class FileSourceSettings:
    file_type: FileType = attr.ib()

    first_line_is_header: bool = attr.ib()


@attr.s(init=True, kw_only=True)
class CSVFileSourceSettings(FileSourceSettings):
    file_type: FileType = attr.ib(default=FileType.csv)


@attr.s(init=True, kw_only=True)
class SpreadsheetFileSourceSettings(FileSourceSettings):
    raw_schema_header: list[SchemaColumn] = attr.ib()
    raw_schema_body: list[SchemaColumn] = attr.ib()


@attr.s(init=True, kw_only=True)
class GSheetsFileSourceSettings(SpreadsheetFileSourceSettings):
    file_type: FileType = attr.ib(default=FileType.gsheets)


@attr.s(init=True, kw_only=True)
class ExcelFileSourceSettings(SpreadsheetFileSourceSettings):
    file_type: FileType = attr.ib(default=FileType.xlsx)


@attr.s(init=True, kw_only=True)
class UserSourceProperties(SecretContainingMixin):
    file_type: FileType = attr.ib()


@attr.s(init=True, kw_only=True)
class GSheetsUserSourceProperties(UserSourceProperties):
    file_type: FileType = attr.ib(default=FileType.gsheets)

    spreadsheet_id: str = attr.ib()
    refresh_token: Optional[str] = attr.ib(default=None, repr=False)

    def get_secret_keys(self) -> set[DataKey]:
        return {DataKey(parts=("refresh_token",))}


@attr.s(init=True, kw_only=True)
class YaDocsUserSourceProperties(UserSourceProperties):
    file_type: FileType = attr.ib(default=FileType.yadocs)

    private_path: Optional[str] = attr.ib(default=None)
    public_link: Optional[str] = attr.ib(default=None)

    oauth_token: Optional[str] = attr.ib(default=None, repr=False)

    def get_secret_keys(self) -> set[DataKey]:
        return {DataKey(parts=("oauth_token",))}


@attr.s(init=True, kw_only=True)
class UserSourceDataSourceProperties:
    file_type: FileType = attr.ib()


@attr.s(init=True, kw_only=True)
class GSheetsUserSourceDataSourceProperties(UserSourceDataSourceProperties):
    file_type: FileType = attr.ib(default=FileType.gsheets)

    sheet_id: int = attr.ib()


@attr.s(init=True, kw_only=True)
class ParsingError:
    code: str = attr.ib()


@attr.s
class FileProcessingError:
    level: ErrorLevel = attr.ib()
    message: str = attr.ib()
    code: list[str] = attr.ib(factory=list)
    details: dict = attr.ib(factory=dict)

    @classmethod
    def from_exception(cls, exc: DLFileUploaderBaseError, level: Optional[ErrorLevel] = None) -> FileProcessingError:
        return cls(
            level=level or exc.default_level,
            message=exc.message,
            code=exc.err_code,
            details=exc.details,
        )


@attr.s(init=True, kw_only=True)
class DataSource:
    id: str = attr.ib(factory=lambda: str(uuid.uuid4()))
    s3_key: str = attr.ib(
        default=attr.Factory(
            lambda self: self.id + "_" + str(uuid.uuid4())[:8],
            takes_self=True,
        )
    )
    preview_id: Optional[str] = attr.ib(default=None)
    title: str = attr.ib()
    raw_schema: list[SchemaColumn] = attr.ib()
    file_source_settings: Optional[FileSourceSettings] = attr.ib(default=None)
    user_source_dsrc_properties: Optional[UserSourceDataSourceProperties] = attr.ib(default=None)
    status: FileProcessingStatus = attr.ib()
    error: Optional[FileProcessingError] = attr.ib(default=None)

    @property
    def is_applicable(self) -> bool:
        return not (self.error is not None or self.status == FileProcessingStatus.failed)


@attr.s(init=True, kw_only=True)
class DataFile(RedisModelUserIdAuth):
    filename: Optional[str] = attr.ib()
    file_type: Optional[FileType] = attr.ib(default=None)
    file_settings: Optional[FileSettings] = attr.ib(default=None)
    user_source_properties: Optional[UserSourceProperties] = attr.ib(default=None)
    size: Optional[int] = attr.ib(default=None)
    sources: Optional[list[DataSource]] = attr.ib(default=None)
    status: FileProcessingStatus = attr.ib()
    error: Optional[FileProcessingError] = attr.ib(default=None)

    KEY_PREFIX: ClassVar[str] = "df"
    DEFAULT_TTL: ClassVar[Optional[int]] = 12 * 60 * 60  # 12 hours

    @property
    def s3_key(self) -> str:
        return self.id

    def get_secret_keys(self) -> set[DataKey]:
        if self.user_source_properties is None:
            return set()

        lower_level_keys = self.user_source_properties.get_secret_keys()
        return {DataKey(parts=("user_source_properties",) + ll_key.parts) for ll_key in lower_level_keys}

    def get_source_by_id(self, source_id: str) -> DataSource:
        if not self.sources:
            raise EmptySourcesError("There are no sources for the file.")
        for src in self.sources:
            if src.id == source_id:
                return src
        raise SourceNotFoundError(f"Source with id {source_id} has not been found.")


@attr.s(init=True, kw_only=True)
class DataSourcePreview(RedisModel):
    preview_data: list[list[Optional[str]]] = attr.ib()

    KEY_PREFIX: ClassVar[str] = "df_preview"


@attr.s(init=True, kw_only=True)
class RenameTenantStatusModel(RedisModel):  # object id is tenant_id
    status: RenameTenantStatus = attr.ib()
    mtime: float = attr.ib(factory=time.time)

    KEY_PREFIX = "rename_tenant_status"


@attr.s(init=True, kw_only=True)
class PreviewSet(RedisSetManager):
    KEY_PREFIX = "tenant_previews"
