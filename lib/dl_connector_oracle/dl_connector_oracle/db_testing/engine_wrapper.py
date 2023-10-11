import re
from typing import (
    Any,
    Optional,
    Sequence,
)

import sqlalchemy as sa

from dl_db_testing.database.engine_wrapper import EngineWrapperBase


class OracleEngineWrapper(EngineWrapperBase):
    URL_PREFIX = "oracle"  # Not using the bi_* version because we only need basic functionality here

    def execute(self, query: Any, *multiparams: Any, **params: Any):  # type: ignore  # TODO: fix
        # FIXME: Note the following problem:
        #  https://stackoverflow.com/questions/54709396/incorrect-type-conversion-with-cx-oracle-and-sqlalchemy-queries

        return super().execute(query, *multiparams, **params)

    def count_sql_sessions(self) -> int:
        # noinspection SqlDialectInspection
        cur = self.execute("SELECT * FROM sys.V_$SESSION")
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()

    def get_conn_credentials(self, full: bool = False) -> dict:
        cred_prefix, oracle_dsn = str(self.url).split("@")
        username, password = cred_prefix.split("//")[1].split(":")
        match = re.search(
            r"HOST=(?P<host>[^)]+)\).*PORT=(?P<port>\d+)\).*(SERVICE_NAME|SID)=(?P<db_name>[^)]+)\)", oracle_dsn
        )
        return dict(
            host=match.group("host"),  # type: ignore  # TODO: fix
            port=int(match.group("port")),  # type: ignore  # TODO: fix
            username=username,
            password=password,
            db_name=match.group("db_name"),  # type: ignore  # TODO: fix
        )

    def insert_into_table(self, table: sa.Table, data: Sequence[dict]) -> None:
        # Multi-row insert doesn't work correctly
        for row in data:
            self.execute(table.insert(row))

    def create_schema(self, schema_name: str) -> None:
        self.execute(f"CREATE USER {self.quote(schema_name)} IDENTIFIED BY qwerty")
        self.execute(f"GRANT ALL PRIVILEGES TO {self.quote(schema_name)}")

    def get_version(self) -> Optional[str]:
        return self.execute("SELECT * FROM V$VERSION").scalar()
