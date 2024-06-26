
import itertools

import requests

from clickhouse_sqlalchemy.types import get_http_postprocess
from ...exceptions import DatabaseException
from .exceptions import HTTPException


DEFAULT_DDL_TIMEOUT = None
DEBUG_STREAMS = False


# TODO: commonize this with the schema-parsing logic.
def _get_type(type_str):
    from .base import ClickHouseDialect_http
    dialect = ClickHouseDialect_http()
    coltype = dialect._get_column_type('_', type_str)
    return get_http_postprocess(coltype)


class RequestsTransport(object):

    def __init__(
            self,
            db_url, db_name, username, password,
            timeout=None, ch_settings=None, verify=None,
            format_info=None,
            **kwargs):

        self.db_url = db_url
        self.db_name = db_name
        self.auth = None if username is None and password is None else (username, password)
        if format_info is None:
            raise TypeError('format_info is required here')
        if (format_info.generator_from_lines is None and
                format_info.generator is None):
            raise TypeError(
                ('formats here must have `generator_from_lines` '
                 'or `generator`'))
        self.format_info = format_info

        self.timeout = None
        connect_timeout = kwargs.get('connect_timeout')
        if timeout is not None:
            if connect_timeout is not None:
                self.timeout = (float(connect_timeout), float(timeout))
            else:
                self.timeout = float(timeout)

        self.verify = verify

        self.headers = {
            key[8:]: value
            for key, value in kwargs.items()
            if key.startswith('header__')
        }

        self.statuses_to_retry = (
            kwargs.get('statuses_to_retry', '502,504')
            .split(','))

        ch_settings = dict(ch_settings or {})
        self.ch_settings = ch_settings

        ddl_timeout = kwargs.pop('ddl_timeout', DEFAULT_DDL_TIMEOUT)
        if ddl_timeout is not None:
            self.ch_settings['distributed_ddl_task_timeout'] = int(ddl_timeout)

        super(RequestsTransport, self).__init__()

    def execute_ext(
            self, query, params=None,
            allow_chunks=False, strict=False):
        """
        Query is returning rows and these rows should be parsed or
        there is nothing to return.
        """
        resp = self._send(query, params=params, stream=True)
        fmt = self.format_info

        if fmt.generator_from_lines:
            chunks = resp.iter_lines()
            parser = fmt.generator_from_lines
        else:
            chunks = resp.iter_content(fmt.chunk_size)
            parser = fmt.generator

        if DEBUG_STREAMS:
            def _dbg_chunks(chunks):
                for chunk in chunks:
                    print(' >>> output >>> {!r}'.format(chunk))
                    yield chunk

            chunks = _dbg_chunks(chunks)

        try:
            chunk = next(chunks)
        except StopIteration:
            # Empty result; e.g. a DDL request.
            if not strict:
                yield fmt.parts.META, []
                return
            raise Exception("A not well-behaving parser", fmt)

        # Put it back:
        chunks = itertools.chain([chunk], chunks)
        results = parser(chunks)

        try:
            part, meta = next(results)
        except StopIteration:
            # Warning: this is more dubious;
            # sample case: 'drop table ... on cluster ...',
            # without 'FORMAT ...',
            # with exactly one host in the cluster.
            # chunks example: [b'127.0.0.1\t9000\t0\t\t0\t0']
            if not strict:
                yield fmt.parts.META, []
                return
            raise Exception("A not well-behaving parser", fmt)
        assert part == fmt.parts.META

        # TODO: move this to a different layer.
        convs = [_get_type(field['type']) for field in meta]

        def postprocess(row):
            assert len(row) == len(convs)
            return [
                converter(value) if converter is not None else value
                for value, converter in zip(row, convs)]

        yield fmt.parts.META, meta

        for part, item in results:
            if part == fmt.parts.DATACHUNK:
                item = [postprocess(row) for row in item]
                if allow_chunks:
                    yield part, item
                else:
                    for row in item:
                        yield fmt.parts.DATA, row
            elif part in (fmt.parts.DATA, fmt.parts.TOTALS):
                item = postprocess(item)
                yield part, item
            else:
                yield part, item

    def execute(self, query, params=None, **kwargs):
        fmt = self.format_info
        results = self.execute_ext(query, params=params, **kwargs)
        try:
            part, meta = next(results)
        except StopIteration:
            raise Exception("A not well-behaving `execute_ext`")
        assert part == fmt.parts.META

        yield [field['name'] for field in meta]
        yield [field['type'] for field in meta]
        for part, item in results:
            if part == fmt.parts.DATA:
                yield item

    def raw(self, query, params=None, stream=False):
        """
        Performs raw query to database. Returns its output
        :param query: Query to execute
        :param params: Additional params should be passed during query.
        :param stream: If flag is true, Http response from ClickHouse will be
            streamed.
        :return: Query execution result
        """
        return self._send(query, params=params, stream=stream).text

    def _send(self, data, params=None, stream=False):

        if DEBUG_STREAMS:
            print(' >>> input >>> {!r}'.format(data))

        query = data
        data = data.encode('utf-8')
        params = params or {}

        params['database'] = self.db_name
        params.update(self.ch_settings)

        session = requests.Session()

        headers = {
            key: (
                # On-before-send dynamic header values for e.g. tracing / timing.
                # `None` values should be ignored by `requests`.
                value(query=query, params=params, stream=stream)
                if callable(value)
                else value)
            for key, value in self.headers.items()}
        headers["Content-Type"] = "application/octet-stream"
        # ^ requests does not set Content-Type with this set of arguments automatically, so we do it manually
        try:
            resp = session.post(
                self.db_url, auth=self.auth, params=params, data=data,
                stream=stream, timeout=self.timeout, headers=headers,
                allow_redirects=False, verify=self.verify,
            )
        except requests.exceptions.RequestException as exc:
            raise DatabaseException(exc)

        if resp.status_code != 200:
            orig = HTTPException(resp.text)
            orig.code = resp.status_code
            raise DatabaseException(orig)

        return resp
