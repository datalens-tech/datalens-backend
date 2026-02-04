import attr

from dl_constants.enums import FileProcessingStatus
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec


@attr.s
class BaseFileS3DataSourceSpec(StandardSQLDataSourceSpec):
    s3_endpoint: str | None = attr.ib(kw_only=True, default=None)
    bucket: str | None = attr.ib(kw_only=True, default=None)
    origin_source_id: str | None = attr.ib(kw_only=True, default=None)
    status: FileProcessingStatus = attr.ib(kw_only=True, default=FileProcessingStatus.in_progress)
