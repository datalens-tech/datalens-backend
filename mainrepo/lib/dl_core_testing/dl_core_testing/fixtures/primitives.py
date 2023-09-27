import attr

from dl_constants.enums import UserDataType


@attr.s(frozen=True)
class FixtureTableSpec:
    csv_name: str = attr.ib(kw_only=True)
    table_schema: tuple[tuple[str, UserDataType], ...] = attr.ib(kw_only=True)
    nullable: bool = attr.ib(kw_only=True, default=True)
