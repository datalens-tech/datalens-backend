import attr

from dl_constants.enums import DataSourceType
from dl_core.base_models import ConnectionRef
from dl_core.db.elements import SchemaColumn


@attr.s
class DataSourceSpec:
    source_type: DataSourceType = attr.ib(kw_only=True)
    connection_ref: ConnectionRef | None = attr.ib(kw_only=True, default=None)
    raw_schema: list[SchemaColumn] | None = attr.ib(kw_only=True, default=None)
    data_dump_id: str | None = attr.ib(kw_only=True, default=None)

    @property
    def is_configured(self) -> bool:
        return True

    @classmethod
    def get_public_fields(cls) -> list[str]:
        return [field.name for field in attr.fields(cls) if not field.name.startswith("_")]
