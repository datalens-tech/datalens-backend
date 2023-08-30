
import sqlalchemy as sa
from ...util.compat import string_types
from ..base import ClickHouseDialect, ClickHouseExecutionContextBase
from . import connector


# Export connector version
VERSION = (0, 0, 2, None)


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):

    def _add_statement_format(self):
        # TODO: refactor
        if not self.isinsert and not self.isddl:
            self.statement = '{} FORMAT {}'.format(
                self.statement,
                self.dialect.format_string(self.root_connection))

    def pre_exec(self):
        self._add_statement_format()
        super(ClickHouseExecutionContext, self).pre_exec()


class ClickHouseDialect_http(ClickHouseDialect):
    driver = 'http'
    execution_ctx_cls = ClickHouseExecutionContext

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        kwargs = {}
        protocol = url.query.get('protocol', 'http')
        port = url.port or 8123
        db_name = url.database or 'default'
        endpoint = url.query.get('endpoint', '')

        kwargs.update(url.query)

        db_url = '%s://%s:%d/%s' % (protocol, url.host, port, endpoint)

        return (db_url, db_name, url.username, url.password), kwargs

    def _execute(self, connection, sql):
        if isinstance(sql, string_types):
            # Makes sure the query will go through the
            # `ClickHouseExecutionContext` logic.
            sql = sa.sql.elements.TextClause(sql.replace(':', r'\:'))
        return connection.execute(sql)

    def _query_server_version_string(self, connection):
        query = 'select version()'
        return connection.scalar(query)


dialect = ClickHouseDialect_http
