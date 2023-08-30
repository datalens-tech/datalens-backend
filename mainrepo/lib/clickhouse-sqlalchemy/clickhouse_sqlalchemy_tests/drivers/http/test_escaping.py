from decimal import Decimal
from datetime import datetime, date

from sqlalchemy import Column, literal

from clickhouse_sqlalchemy import types, engines, Table
from clickhouse_sqlalchemy.drivers.http.escaper import Escaper
from clickhouse_sqlalchemy_tests.testcase import HttpSessionTestCase


class EscapingTestCase(HttpSessionTestCase):
    def escaped_compile(self, clause, **kwargs):
        return str(self._compile(clause, **kwargs))

    def test_select_escaping(self):
        query = self.session.query(literal('\t'))
        self.assertEqual(
            self.escaped_compile(query, literal_binds=True),
            r"SELECT '\t' AS anon_1"
        )

    def test_escaper(self):
        e = Escaper()
        self.assertEqual(e.escape([None]), '[NULL]')
        self.assertEqual(e.escape([[None]]), '[[NULL]]')
        self.assertEqual(e.escape([[123]]), '[[123]]')
        self.assertEqual(e.escape({'x': 'str'}), {'x': "'str'"})
        self.assertEqual(e.escape([Decimal('10')]), '[10.0]')
        self.assertEqual(e.escape([10.0]), '[10.0]')
        self.assertEqual(e.escape([date(2017, 1, 2)]), "['2017-01-02']")

        with self.assertRaises(Exception) as ex:
            e.escape([object()])

        self.assertIn('Unsupported object', str(ex.exception))

        with self.assertRaises(Exception) as ex:
            e.escape('str')

        self.assertIn('Unsupported param format', str(ex.exception))

    def test_escape_binary_mod(self):
        query = self.session.query(literal(1) % literal(2))
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT 1 %% 2 AS anon_1'
        )

        table = Table(
            't', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            engines.Memory()
        )

        query = self.session.query(table.c.x % table.c.x)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x %% x AS anon_1 FROM t'
        )

    def test_literal_binds(self):
        null = None
        query = self.session.query(
            literal(null).label('null'),
            literal([null]).label('nulla'),
            literal([[null]]).label('nullaa'),
            literal([(123,)]).label('intaa'),
            literal('zxc').label('str'),
            literal('фыва⋯').label('stru'),
            literal(123).label('int'),
            literal(
                340282366920938463463374607431768211456
            ).label('longint'),
            literal(date(2018, 2, 28)).label('date'),
            literal(
                datetime(2018, 2, 28, 23, 59, 59, 999999)
            ).label('datetime'),
            literal(Decimal(10.0000000010)).label('decimal'),
        )

        query_text = self.compile(query, literal_binds=True)
        expected_query_text = 'SELECT ' + ', '.join((
            'NULL AS "null"',
            '[NULL] AS nulla',
            '[[NULL]] AS nullaa',
            '[[123]] AS intaa',
            '\'zxc\' AS str',
            '\'фыва⋯\' AS stru',
            '123 AS int',
            '340282366920938463463374607431768211456 AS longint',
            'toDate(\'2018-02-28\') AS date',
            'toDateTime(\'2018-02-28 23:59:59\') AS datetime',
            '10.000000001 AS decimal'
        ))
        self.assertEqual(query_text, expected_query_text)


class InsertEscapingTestCase(HttpSessionTestCase):
    columns = [')', '(', '%', '{']  # might as well use full `string.ascii`?
    table = Table(
        'test',
        HttpSessionTestCase.metadata(),
        *(
            [Column(name, types.String, primary_key=True)
             for name in columns] +
            [engines.Memory()]))

    def test_insert_data(self):
        data = [{col: '1' for col in self.columns}]

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), data)
