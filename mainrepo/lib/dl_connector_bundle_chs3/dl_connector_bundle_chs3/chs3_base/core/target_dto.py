from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core.utils import secrepr


@attr.s
class BaseFileS3ConnTargetDTO(BaseSQLConnTargetDTO):
    protocol: str = attr.ib(kw_only=True)
    disable_value_processing: bool = attr.ib(kw_only=True)

    endpoint: Optional[str] = attr.ib(kw_only=True, default=None)
    max_execution_time: Optional[int] = attr.ib(kw_only=True, default=None)
    connect_timeout: Optional[int] = attr.ib(kw_only=True, default=None)
    total_timeout: Optional[int] = attr.ib(kw_only=True, default=None)

    s3_endpoint: str = attr.ib(kw_only=True)
    bucket: str = attr.ib(kw_only=True)
    access_key_id: str = attr.ib(kw_only=True, repr=secrepr)
    secret_access_key: str = attr.ib(kw_only=True, repr=secrepr)
    replace_secret: str = attr.ib(kw_only=True)
