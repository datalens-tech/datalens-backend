from typing import Optional

import attr
import sqlalchemy as sa

from bi_db_testing.database.engine_wrapper import DbEngineConfig, EngineWrapperBase


@attr.s(frozen=True)
class SnowFlakeDbEngineConfig(DbEngineConfig):
    db_name: str = attr.ib(kw_only=True)


class SnowFlakeEngineWrapper(EngineWrapperBase):
    CONFIG_CLS = SnowFlakeDbEngineConfig
    URL_PREFIX = 'snowflake'

    @property
    def config(self) -> SnowFlakeDbEngineConfig:
        assert isinstance(self._config, SnowFlakeDbEngineConfig)
        return self._config

    @property
    def name(self) -> str:
        name = self.execute('SELECT CURRENT_DATABASE()').scalar()
        assert isinstance(name, str)
        return name

    def get_version(self) -> Optional[str]:
        return self.name

    def drop_table(self, db_name: str, table: sa.Table) -> None:
        table.drop(bind=self.engine, checkfirst=True)

    def get_conn_credentials(self, full: bool = False) -> dict:
        return dict(
            db_name=self.config.db_name,
        )
