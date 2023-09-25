from datetime import datetime

from clickhouse_sqlalchemy.dt_utils import parse_datetime64

from clickhouse_sqlalchemy_tests.testcase import BaseAbstractTestCase


class Datetime64ParserTestCase(BaseAbstractTestCase):
    def test_7_digits_after_dot(self):
        self.assertEqual(
            parse_datetime64('2020-01-01 12:34:56.1234567'),
            datetime(year=2020, month=1, day=1, hour=12, minute=34, second=56, microsecond=123456)
        )

    def test_6_digits_after_dot(self):
        self.assertEqual(
            parse_datetime64('2020-01-01 12:34:56.123456'),
            datetime(year=2020, month=1, day=1, hour=12, minute=34, second=56, microsecond=123456)
        )

    def test_3_digits_after_dot(self):
        self.assertEqual(
            parse_datetime64('2020-01-01 12:34:56.123'),
            datetime(year=2020, month=1, day=1, hour=12, minute=34, second=56, microsecond=123000)
        )

    def test_no_digits_after_dot(self):
        self.assertEqual(
            parse_datetime64('2020-01-01 12:34:56'),
            datetime(year=2020, month=1, day=1, hour=12, minute=34, second=56, microsecond=0)
        )
