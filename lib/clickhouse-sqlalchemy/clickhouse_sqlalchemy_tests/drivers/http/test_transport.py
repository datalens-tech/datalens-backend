from datetime import date, datetime, timezone

import pytz
import sqlalchemy as sa
from responses import mock
from mock import patch
from sqlalchemy import Column, func, create_engine

from clickhouse_sqlalchemy import types, Table, make_session
from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http
from clickhouse_sqlalchemy_tests.config import http_uri
from clickhouse_sqlalchemy_tests.session import http_tsv_session
from clickhouse_sqlalchemy_tests.testcase import HttpSessionTestCase


class TransportCase(HttpSessionTestCase):

    # Mocked data is in TSV format.
    session = http_tsv_session

    @property
    def url(self):
        return 'http://{host}:{port}'.format(host=self.host, port=self.port)

    @mock.activate
    def test_parse_func_count(self):
        mock.add(
            mock.POST, self.url, status=200,
            body='count_1\nUInt64\n42\n'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        rv = self.session.query(func.count()).select_from(table).scalar()
        self.assertEqual(rv, 42)

    @mock.activate
    def test_parse_int_types(self):
        types_ = [
            'Int8', 'UInt8', 'Int16', 'UInt16', 'Int32', 'UInt32', 'Int64',
            'UInt64'
        ]
        columns = [chr(i + ord('a')) for i in range(len(types_))]

        mock.add(
            mock.POST, self.url, status=200,
            body=(
                '\t'.join(columns) + '\n' +
                '\t'.join(types_) + '\n' +
                '\t'.join(['42'] * len(types_)) + '\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            *[Column(col, types.Int) for col in columns]
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, tuple([42] * len(columns)))

    @mock.activate
    def test_parse_float_types(self):
        types_ = ['Float32', 'Float64']
        columns = ['a', 'b']

        mock.add(
            mock.POST, self.url, status=200,
            body=(
                '\t'.join(columns) + '\n' +
                '\t'.join(types_) + '\n' +
                '\t'.join(['42'] * len(types_)) + '\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            *[Column(col, types.Float) for col in columns]
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, tuple([42.0] * len(columns)))

    # do not call that method
    @patch.object(ClickHouseDialect_http, '_get_server_version_info')
    @mock.activate
    def test_parse_date_types(self, patched_server_info):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'Date\n' +
                '2012-10-25\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.Date)
        )

        rv = self.session.query(*table.c).first()
        self.assertEqual(rv, (date(2012, 10, 25), ))

    @mock.activate
    def test_parse_datetime_type(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
            body=(
                'a\n' +
                'DateTime\n' +
                '2012-10-25 01:02:03\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.DateTime)
        )

        rv = (
            self.session.query(*table.c)
            .filter(table.c.a > datetime(2000, 12, 23, 13, 14, 15, 123))
            .first())
        self.assertEqual(rv, (datetime(2012, 10, 25, 1, 2, 3), ))

    @mock.activate
    def test_parse_datetime_utc_type(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
            body=(
                'a\n' +
                'DateTime(\'UTC\')\n' +
                '2012-10-25 01:02:03\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.DateTime)
        )

        rv = self.session.query(*table.c).first()
        expected_value = (
            datetime(2012, 10, 25, 1, 2, 3)
            .replace(tzinfo=timezone.utc))
        self.assertEqual(rv, (expected_value,))

    @mock.activate
    def test_parse_datetime_any_tz_type(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
            body=(
                'a\n' +
                'DateTime(\'MSK\')\n' +
                '2012-10-25 01:02:03\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.DateTime)
        )

        rv = self.session.query(*table.c).first()
        expected_value = pytz.timezone('Europe/Moscow').localize(
            datetime(2012, 10, 25, 1, 2, 3))
        self.assertEqual(rv, (expected_value,))

    @mock.activate
    def test_parse_nullable_datetime_any_tz_type(self):
        mock.add(
            mock.POST, 'http://localhost:8123', status=200,
            body=(
                'a\n' +
                'Nullable(DateTime(\'EST\'))\n' +
                '2012-10-25 01:02:03\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.DateTime)
        )

        rv = self.session.query(*table.c).first()
        expected_value = (
            datetime(2012, 10, 25, 1, 2, 3)
            .replace(tzinfo=pytz.timezone('EST')))
        self.assertEqual(rv, (expected_value,))

    @mock.activate
    def test_parse_nullable_type(self):
        mock.add(
            mock.POST, self.url, status=200,
            body=(
                'a\n' +
                'String\n' +
                '\\N\n' +
                '\\\\N\n' +
                '\n'
            )
        )

        table = Table(
            't1', self.metadata(),
            Column('a', types.String)
        )

        rv = self.session.query(*table.c).all()
        self.assertEqual(rv, [(None, ), ('\\N', ), ('', )])


class TransportHeadersCase(TransportCase):

    @mock.activate
    def test_dynamic_headers(self):
        header_value = 'h1'
        engine = create_engine(
            http_uri,
            connect_args={
                'server_version': '20.3.0.0',  # otherwise will break under `mock`.
                'header__X-Test-Header': lambda *_, **__: header_value,
            },
        )
        session = make_session(engine)

        mock.add(
            mock.POST, self.url, status=200,
            body='a\nUInt8\n1\n'
        )

        session.query(sa.text('select 1 as a')).scalar()
        assert mock.calls[-1].request.headers['X-Test-Header'] == header_value
