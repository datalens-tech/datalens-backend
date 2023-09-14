from typing import Sequence

import sqlalchemy as sa

from bi_db_testing.database.engine_wrapper import EngineWrapperBase


class CHYTEngineWrapperBase(EngineWrapperBase):
    URL_PREFIX = "bi_chyt"

    def get_conn_credentials(self, full: bool = False) -> dict:
        return {}  # FIXME

    def drop_table(self, db_name: str, table: sa.Table) -> None:
        raise NotImplementedError

    def create_table(self, table: sa.Table) -> None:
        raise NotImplementedError

    def insert_into_table(self, table: sa.Table, data: Sequence[dict]) -> None:
        raise NotImplementedError

    def test(self) -> bool:
        return True
