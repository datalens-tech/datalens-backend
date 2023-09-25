# coding: utf8
"""
...
"""

from __future__ import unicode_literals

import sqlalchemy as sa

from clickhouse_sqlalchemy_tests.testcase import BaseTestCase
from clickhouse_sqlalchemy_tests.session import http_jc_session


class HttpTotalsTestCase(BaseTestCase):

    session = http_jc_session

    def test_select_totals_simple(self):
        query = "select '' as value group by value with totals"
        res = self.session.execute(query)
        cursor = res.cursor
        results = list(res)
        self.assertEqual(results, [('',)])
        self.assertTrue(isinstance(results[0], sa.engine.row.RowProxy))
        self.assertEqual(
            cursor.ch_schema,
            [{'name': 'value', 'type': 'String'}])
        self.assertTrue(cursor.ch_stats)
        try:
            elapsed = cursor.ch_stats['statistics']['elapsed']
        except Exception:
            elapsed = None
        expected = {
            'rows': 1,
            'totals': [''],
            'statistics': {
                'elapsed': elapsed,
                'rows_read': 1,
                'bytes_read': 1}}
        self.assertEqual(cursor.ch_stats, expected)
        # Note: one row, postprocessed, but not currently wrapped in
        # `sqlalchemy.engine.result.RowProxy`»
        self.assertEqual(cursor.ch_totals, [''])
