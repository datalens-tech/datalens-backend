import re

from sqlalchemy import create_engine

from clickhouse_sqlalchemy import make_session

from .config import (
    http_uri, native_uri, system_native_uri, system_http_uri,
)

http_session = make_session(create_engine(http_uri))
http_stream_session = make_session(create_engine(http_uri + '?stream=1'))
http_tsv_session = make_session(create_engine(
    http_uri + '?format=TabSeparatedWithNamesAndTypes'))
http_jc_session = make_session(create_engine(
    http_uri + '?format=JSONCompact&stream=1'))

native_session = make_session(create_engine(native_uri))

system_native_session = make_session(create_engine(system_native_uri))
system_http_session = make_session(create_engine(system_http_uri))


class MockedEngine(object):

    prev_do_execute = None
    prev_do_executemany = None
    prev_query_server_version_string = None

    def __init__(self, session=None):
        self._buffer = []

        if session is None:
            session = make_session(create_engine(http_uri))

        self.session = session
        self.dialect_cls = session.bind.dialect.__class__

    @property
    def history(self):
        return [re.sub(r'[\n\t]', '', str(ssql)) for ssql in self._buffer]

    def __enter__(self):
        self.prev_do_execute = self.dialect_cls.do_execute
        self.prev_do_executemany = self.dialect_cls.do_executemany
        self.prev_query_server_version_string = \
            self.dialect_cls._query_server_version_string

        def do_executemany(
                instance, cursor, statement, parameters, context=None):
            self._buffer.append(statement)

        def do_execute(instance, cursor, statement, parameters, context=None):
            self._buffer.append(statement)

        def query_server_version_string(*args, **kwargs):
            return '19.16.2.2'

        self.dialect_cls.do_execute = do_execute
        self.dialect_cls.do_executemany = do_executemany
        self.dialect_cls._query_server_version_string = \
            query_server_version_string

        return self

    def __exit__(self, *exc_info):
        self.dialect_cls.do_execute = self.prev_do_execute
        self.dialect_cls.do_executemany = self.prev_do_executemany
        self.dialect_cls._query_server_version_string = \
            self.prev_query_server_version_string


mocked_engine = MockedEngine
