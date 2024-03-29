from typing import Optional

import attr

from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO


@attr.s(frozen=True)
class BaseFileS3ConnDTO(ClickHouseConnDTO):
    s3_endpoint: str = attr.ib(kw_only=True)
    access_key_id: str = attr.ib(kw_only=True, repr=False)
    secret_access_key: str = attr.ib(kw_only=True, repr=False)
    bucket: str = attr.ib(kw_only=True)
    replace_secret: str = attr.ib(kw_only=True)

    # Set default=None
    db_name: Optional[str] = attr.ib(kw_only=True, default=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "str | None", base class "ClickHouseConnDTO" defined the type as "str")  [assignment]
    cluster_name: Optional[str] = attr.ib(kw_only=True, default=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "str | None", base class "ClickHouseConnDTO" defined the type as "str")  [assignment]
    endpoint: Optional[str] = attr.ib(kw_only=True, default=None)
