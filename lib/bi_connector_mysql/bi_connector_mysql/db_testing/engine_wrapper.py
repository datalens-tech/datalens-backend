from bi_db_testing.database.engine_wrapper import EngineWrapperBase


class MySQLEngineWrapperBase(EngineWrapperBase):
    def count_sql_sessions(self) -> int:
        cur = self.execute('SHOW PROCESSLIST').fetchall()
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()


class MySQLEngineWrapper(MySQLEngineWrapperBase):
    URL_PREFIX = 'mysql'


class BiMySQLEngineWrapper(MySQLEngineWrapperBase):
    URL_PREFIX = 'bi_mysql'
