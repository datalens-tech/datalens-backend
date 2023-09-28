import attr

from dl_constants.enums import UserDataType


@attr.s(frozen=True)
class FixtureTableSpec:
    csv_name: str = attr.ib(kw_only=True)
    table_schema: tuple[tuple[str, UserDataType], ...] = attr.ib(kw_only=True)
    nullable: bool = attr.ib(kw_only=True, default=True)

    def get_user_type_for_col(self, col_name: str) -> UserDataType:
        return next(tbl_col[1] for tbl_col in self.table_schema if tbl_col[0] == col_name)
