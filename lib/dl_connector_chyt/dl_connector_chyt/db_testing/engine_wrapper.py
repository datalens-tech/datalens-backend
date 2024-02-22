from typing import (
    Optional,
    Sequence,
)

from clickhouse_sqlalchemy import Table as CHTable
import sqlalchemy as sa

from dl_db_testing.database.engine_wrapper import EngineWrapperBase
from dl_sqlalchemy_chyt.table_engine import YtTable


class CHYTEngineWrapperBase(EngineWrapperBase):
    URL_PREFIX = "bi_chyt"

    def get_conn_credentials(self, full: bool = False) -> dict:
        return {"db_name": ""}  # db_name is required by `drop_table`

    def table_from_columns(
        self,
        columns: Sequence[sa.Column],
        *,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> sa.Table:
        assert table_name is not None, "Table name is required"
        table = CHTable(table_name, sa.MetaData(), *columns)
        table.engine = YtTable()
        return table

    def test(self) -> bool:
        return True

    def drop_table(self, db_name: str, table: sa.Table) -> None:
        if table.exists(bind=self.engine):
            super().drop_table(db_name=db_name, table=table)
