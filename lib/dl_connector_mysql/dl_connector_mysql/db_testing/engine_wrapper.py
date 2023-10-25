from dl_db_testing.database.engine_wrapper import EngineWrapperBase


class MySQLEngineWrapperBase(EngineWrapperBase):
    def count_sql_sessions(self) -> int:
        cur = self.execute("SHOW PROCESSLIST")
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()


class DLMYSQLEngineWrapper(MySQLEngineWrapperBase):
    URL_PREFIX = "dl_mysql"
