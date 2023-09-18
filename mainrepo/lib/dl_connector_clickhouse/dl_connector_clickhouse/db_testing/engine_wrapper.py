from typing import (
    Optional,
    Sequence,
)

import attr
from clickhouse_sqlalchemy import Table as CHTable
from clickhouse_sqlalchemy.engines import Log
import shortuuid
import sqlalchemy as sa

from dl_db_testing.database.engine_wrapper import (
    DbEngineConfig,
    EngineWrapperBase,
)


@attr.s(frozen=True)
class ClickhouseDbEngineConfig(DbEngineConfig):
    cluster: Optional[str] = attr.ib(kw_only=True, default=None)


class ClickHouseEngineWrapperBase(EngineWrapperBase):
    CONFIG_CLS = ClickhouseDbEngineConfig

    @property
    def config(self) -> ClickhouseDbEngineConfig:
        assert isinstance(self._config, ClickhouseDbEngineConfig)
        return self._config

    def get_conn_credentials(self, full: bool = False) -> dict:
        creds = super().get_conn_credentials(full)
        if not full:
            creds.pop("db_name")
        return creds

    def load_table(self, table_name: str, schema: Optional[str] = None) -> sa.Table:
        return CHTable(table_name, sa.MetaData(bind=self.engine), autoload=True)

    def drop_table(self, db_name: str, table: sa.Table) -> None:
        assert isinstance(table, CHTable)
        if self.config.cluster:
            self.execute(
                sa.text(f"DROP TABLE IF EXISTS `{db_name}`.`{table.name}` " f"ON CLUSTER `{self.config.cluster}`")
            )
        else:
            table.drop(bind=self.engine, if_exists=True)

    def table_from_columns(
        self,
        columns: Sequence[sa.Column],
        *,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> sa.Table:
        table_name = table_name or f"test_table_{shortuuid.uuid()[:10]}"
        table = CHTable(table_name, sa.MetaData(), *columns)
        table.engine = Log()
        return table


class ClickHouseEngineWrapper(ClickHouseEngineWrapperBase):
    URL_PREFIX = "clickhouse"


class BiClickHouseEngineWrapper(ClickHouseEngineWrapperBase):
    URL_PREFIX = "bi_clickhouse"

    def _db_connect_params(self) -> dict:
        return {"format": "JSONCompact"}
