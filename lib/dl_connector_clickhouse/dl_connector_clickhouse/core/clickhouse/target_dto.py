import attr

from dl_connector_clickhouse.core.clickhouse_base.target_dto import ClickHouseConnTargetDTO


@attr.s(frozen=True)
class DLClickHouseConnTargetDTO(ClickHouseConnTargetDTO):
    readonly: int = attr.ib(kw_only=True, default=2)
