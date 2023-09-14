from bi_db_testing.database.engine_wrapper import EngineWrapperBase


class PGEngineWrapperBase(EngineWrapperBase):
    def count_sql_sessions(self) -> int:
        # noinspection SqlDialectInspection
        cur = self.execute("select * from pg_stat_activity")
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()


class PGEngineWrapper(PGEngineWrapperBase):
    URL_PREFIX = "postgresql"


class BiPGEngineWrapper(PGEngineWrapperBase):
    URL_PREFIX = "bi_postgresql"
