from dl_db_testing.database.engine_wrapper import EngineWrapperBase


class TrinoEngineWrapper(EngineWrapperBase):
    URL_PREFIX = "trino"

    def count_sql_sessions(self) -> int:
        cur = self.execute("SELECT * FROM system.runtime.queries")
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()
