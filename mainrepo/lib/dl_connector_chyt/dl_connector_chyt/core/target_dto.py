import attr

from dl_connector_clickhouse.core.clickhouse_base.target_dto import BaseClickHouseConnTargetDTO


@attr.s(frozen=True)
class BaseCHYTConnTargetDTO(BaseClickHouseConnTargetDTO):
    pass


@attr.s(frozen=True)
class CHYTConnTargetDTO(BaseCHYTConnTargetDTO):
    pass
