from dl_db_testing.database.engine_wrapper import EngineWrapperBase


class StarRocksEngineWrapperBase(EngineWrapperBase):
    def count_sql_sessions(self) -> int:
        cur = self.execute("SHOW PROCESSLIST")
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()


class DLStarRocksEngineWrapper(StarRocksEngineWrapperBase):
    URL_PREFIX = "dl_mysql"


class BiStarRocksEngineWrapper(StarRocksEngineWrapperBase):
    URL_PREFIX = "bi_starrocks"
