from __future__ import annotations

from typing import Optional
import urllib.parse

from sqlalchemy.engine.url import URL

from dl_db_testing.database.engine_wrapper import EngineWrapperBase


class MSSQLEngineWrapper(EngineWrapperBase):
    URL_PREFIX = 'mssql'  # Not using the bi_* version because we only need basic functionality here

    def get_conn_credentials(self, full: bool = False) -> dict:
        url = self.url
        if isinstance(url, URL):
            odbc_dsn = url.query['odbc_connect']
        else:
            urldata = urllib.parse.urlparse(str(url))
            query = dict(urllib.parse.parse_qsl(urldata.query))
            odbc_dsn = query['odbc_connect']

        assert isinstance(odbc_dsn, str)
        odbc_props = {pair.split('=')[0]: pair.split('=')[1] for pair in odbc_dsn.split(';')}
        return dict(
            host=odbc_props['Server'],
            port=int(odbc_props['Port']),
            username=odbc_props['UID'],
            password=odbc_props['PWD'],
            db_name=odbc_props['Database'],
        )

    def count_sql_sessions(self) -> int:
        cur = self.execute('exec sp_who')
        try:
            lines = cur.fetchall()
            return len(lines)
        finally:
            cur.close()

    def get_version(self) -> Optional[str]:
        return self.execute('SELECT @@VERSION').scalar()
