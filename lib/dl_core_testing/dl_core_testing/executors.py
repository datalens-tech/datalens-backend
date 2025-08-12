from typing import (
    Any,
)

import attr

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core_testing.database import Db


@attr.s
class ExecutorFactoryBase:
    db: Db = attr.ib()

    def get_dto_class(self) -> type[BaseSQLConnTargetDTO]:
        raise NotImplementedError

    def get_dba_class(self) -> type[CommonBaseDirectAdapter]:
        return CommonBaseDirectAdapter

    def get_dto_kwargs(self) -> dict[str, Any]:
        return dict(
            conn_id=None,
            host=self.db.url.host,
            port=self.db.url.port,
            db_name=self.db.url.database,
            username=self.db.url.username,
            password=self.db.url.password,
        )

    def make_dto(self) -> BaseSQLConnTargetDTO:
        dto_cls = self.get_dto_class()
        return dto_cls(**self.get_dto_kwargs())
