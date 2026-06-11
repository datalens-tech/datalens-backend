from collections.abc import Sequence

import attr
import sqlalchemy as sa

from dl_db_testing.database.engine_wrapper import (
    DbEngineConfig,
    EngineWrapperBase,
)


@attr.s(frozen=True)
class BigQueryDbEngineConfig(DbEngineConfig):
    default_dataset_name: str = attr.ib(kw_only=True)


class BigQueryEngineWrapper(EngineWrapperBase):
    URL_PREFIX = "bigquery"
    TABLE_AVAILABILITY_TIMEOUT = 30.0

    @property
    def config(self) -> BigQueryDbEngineConfig:
        assert isinstance(self._config, BigQueryDbEngineConfig)
        return self._config

    def get_conn_credentials(self, full: bool = False) -> dict:
        return dict(
            db_name=self.url.database,
        )

    def table_from_columns(
        self,
        columns: Sequence[sa.Column],
        *,
        schema: str | None = None,
        table_name: str | None = None,
    ) -> sa.Table:
        return super().table_from_columns(
            columns=columns,
            schema=schema or self.config.default_dataset_name,
            table_name=table_name,
        )
