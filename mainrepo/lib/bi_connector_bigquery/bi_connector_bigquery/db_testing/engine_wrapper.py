from typing import (
    Optional,
    Sequence,
)

import attr
import sqlalchemy as sa

from bi_db_testing.database.engine_wrapper import (
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
        creds = dict(
            db_name=self.url.database,
        )
        return creds

    def table_from_columns(
        self,
        columns: Sequence[sa.Column],
        *,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> sa.Table:
        table = super().table_from_columns(
            columns=columns,
            schema=schema or self.config.default_dataset_name,
            table_name=table_name,
        )
        return table
