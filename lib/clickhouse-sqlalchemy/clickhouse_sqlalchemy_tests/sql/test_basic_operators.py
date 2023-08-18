from sqlalchemy import literal, not_

from clickhouse_sqlalchemy_tests.testcase import BaseTestCase


class NotBetweenTestCase(BaseTestCase):
    def test_notbetween(self):
        self.assertEqual(
            self.compile(not_(literal(2).between(1, 3)), literal_binds=True),
            '2 NOT BETWEEN 1 AND 3'
        )
