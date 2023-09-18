from typing import Optional

import attr

from dl_constants.enums import FileProcessingStatus
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec


@attr.s
class BaseFileS3DataSourceSpec(StandardSQLDataSourceSpec):
    s3_endpoint: Optional[str] = attr.ib(kw_only=True, default=None)
    bucket: Optional[str] = attr.ib(kw_only=True, default=None)
    origin_source_id: Optional[str] = attr.ib(kw_only=True, default=None)
    status: FileProcessingStatus = attr.ib(kw_only=True, default=FileProcessingStatus.in_progress)
