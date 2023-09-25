import attr

from dl_constants.enums import BIType


@attr.s(frozen=True)
class FixtureTableSpec:
    csv_name: str = attr.ib(kw_only=True)
    table_schema: tuple[tuple[str, BIType], ...] = attr.ib(kw_only=True)
