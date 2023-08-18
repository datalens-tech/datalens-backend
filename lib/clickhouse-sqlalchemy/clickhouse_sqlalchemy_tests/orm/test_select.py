from sqlalchemy import (
    Column,
    exc,
    func,
    literal,
    select,
)
from sqlalchemy import text
from sqlalchemy import literal_column
from sqlalchemy import tuple_

from clickhouse_sqlalchemy import types, Table
from clickhouse_sqlalchemy.ext.clauses import Lambda, ParametrizedFunction
from clickhouse_sqlalchemy_tests.testcase import (
    BaseAbstractTestCase, HttpSessionTestCase, NativeSessionTestCase,
)


class SelectTestCase(BaseAbstractTestCase):

    def create_table(self, *columns):
        metadata = self.metadata()

        return Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True),
            *columns
        )

    def test_select(self):
        table = self.create_table()

        query = self.session.query(table.c.x)\
            .filter(table.c.x.in_([1, 2]))\
            .having(func.count() > 0)\
            .order_by(table.c.x.desc())
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x '
            'FROM t1 '
            'WHERE x IN (__[POSTCOMPILE_x_1]) '
            'HAVING count(*) > %(count_1)s '
            'ORDER BY x DESC'
        )

    def test_very_simple_select(self):
        """
        A non-CH specific select statement should work too.
        """
        query = select([literal(1).label('col1')])
        self.assertEqual(
            self.compile(query),
            'SELECT %(param_1)s AS col1'
        )

    def test_group_by_query(self):
        table = self.create_table()

        query = self.session.query(table.c.x).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 GROUP BY x'
        )

        query = self.session.query(table.c.x).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 GROUP BY x WITH TOTALS'
        )

        with self.assertRaises(exc.InvalidRequestError) as ex:
            self.session.query(table.c.x).with_totals()

        self.assertIn('with_totals', str(ex.exception))

    def test_array_join(self):
        table = self.create_table(
            Column('nested.array_column', types.Array(types.Int8)),
            Column('nested.another_array_column', types.Array(types.Int8))
        )
        first_label = table.c['nested.array_column'].label('from_array')
        second_not_label = table.c['nested.another_array_column']
        query = self.session.query(first_label, second_not_label)\
            .array_join(first_label, second_not_label)
        self.assertEqual(
            self.compile(query),
            'SELECT '
            '"nested.array_column" AS from_array, '
            '"nested.another_array_column" '
            'AS "t1_nested.another_array_column" '
            'FROM t1 '
            'ARRAY JOIN "nested.array_column" AS from_array, '
            '"nested.another_array_column"'
        )

    def test_sample(self):
        table = self.create_table()

        query = self.session.query(table.c.x).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 SAMPLE %(param_1)s GROUP BY x'
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x AS t1_x FROM t1 SAMPLE 0.1 GROUP BY x'
        )

    def test_label_read(self):
        query_text = (
            r'select '
            r'0 as "a z", '
            r'2 as "a \\z", '
            r'4 as "a \\\\z", '
            r'6 as "a \\\\\\z"')
        result = self.session.execute(query_text)

        result_keys = result.keys()
        expected = [r'a z', r'a \z', r'a \\z', r'a \\\z']
        self.assertEqual(result_keys, expected)
        result_columns = [item[0] for item in result.cursor.description]
        self.assertEqual(result_columns, expected)
        self.assertEqual(len(result.fetchall()), 1)

    def test_label_define(self):
        columns = [r'a z', r'a \z', r'a \\z', r'a \\\z', 'a \'\n"\\z']
        query = self.session.query(*[
            literal(1).label(col)
            for col in columns])
        query_text = self.compile(query, literal_binds=True)
        query_text = query_text % ()  # dbapi interpolate level
        expected_query_text = (
            r'SELECT '
            r'1 AS "a z", '
            r'1 AS "a \\z", '
            r'1 AS "a \\\\z", '
            r'1 AS "a \\\\\\z", '
            '1 AS "a \\\'\\n""\\\\z"'
        )
        self.assertEqual(query_text, expected_query_text)

        result = self.session.execute(query)

        result_keys = result.keys()
        self.assertEqual(result_keys, columns)

        result_columns = [item[0] for item in result.raw.cursor.description]
        self.assertEqual(result_columns, columns)

        self.assertEqual(len(result.fetchmany(3)), 1)

    def test_like_simple(self):
        query = self.session.query(
            literal('Hello World%').like('%World%').label('value'),
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            # Note: '%%' is for the python string interpolation
            # (dbapi layer, where parameters are applied);
            # the database should receive undoubled percentages.
            "SELECT 'Hello World%%' LIKE '%%World%%' AS value"
        )

    def test_like_extended(self):
        col = literal('Hello World%').label('value')
        query = self.session.query(col)
        query = query.filter(col.like('%World%'))
        result = self.session.execute(query)
        result = list(result)
        assert result == [('Hello World%',)]

    def test_like_full(self):
        values = [
            r'a b c % d e',
            r'a b c %% d e',
            r'a b c \% d e',
            r'a b c \\% d e',
        ]
        matches = [
            # r' c % d ' in value
            r'% c \% d %',
            # ' c \\' in value, ' d ' in value
            # (semantically significant '%')
            r'% c \\% d %',
            # r' c \% d ' in value
            r'% c \\\% d %',
            # r' b % d ' in value
            r'% b \% d %',
            # The rest should match all of the values
            # (semantically significant '%'):
            r'% c % d %', r'% c %% d %',
            r'% b % d %', r'% b %% d %',
        ]

        col = func.arrayJoin(values).label('value')
        col_ref = literal_column('value')
        query = self.session.query(
            col,
            *[
                col_ref.like(ptn).label('m{}'.format(idx))
                for idx, ptn in enumerate(matches)])

        query_text = self.compile(query, literal_binds=True)
        query_text = query_text % ()  # dbapi interpolate level
        expected_query_text = (
            r"SELECT arrayJoin(["
            r"'a b c % d e', 'a b c %% d e', "
            r"'a b c \\% d e', 'a b c \\\\% d e'"
            r"]) AS value, "
            r"value LIKE '% c \\% d %' AS m0, "
            r"value LIKE '% c \\\\% d %' AS m1, "
            r"value LIKE '% c \\\\\\% d %' AS m2, "
            r"value LIKE '% b \\% d %' AS m3, "
            r"value LIKE '% c % d %' AS m4, "
            r"value LIKE '% c %% d %' AS m5, "
            r"value LIKE '% b % d %' AS m6, "
            r"value LIKE '% b %% d %' AS m7"
        )
        self.assertEqual(query_text, expected_query_text)

        result = self.session.execute(query)

        result = list(result)
        result.sort()
        self.assertEqual(
            result,
            # value,
            # ' c % d ', 'c \\' … ' d', r'c \% d', 'b % d' (never),
            # ... (always)
            [(r'a b c % d e',
              True, False, False, False,
              True, True, True, True),
             (r'a b c %% d e',
              False, False, False, False,
              True, True, True, True),
             (r'a b c \% d e',
              False, True, True, False,
              True, True, True, True),
             (r'a b c \\% d e',
              False, True, False, False,
              True, True, True, True)])

    def test_parametrized_function(self):
        values = [1, 2, 3, 4, 5]
        table = select([func.arrayJoin(values).label('value')]).alias('sq')
        col_ref = literal_column('value')
        expr = ParametrizedFunction(
            'quantileExact',
            [literal(0.75)],
            col_ref)
        query = self.session.query(expr.label('result')).select_from(table)
        self.assertEqual(
            self.compile(query),
            'SELECT quantileExact(%(param_1)s)(value) AS result '
            'FROM (SELECT arrayJoin(%(arrayJoin_1)s) AS value) AS sq'
        )
        result = self.session.scalar(query)
        self.assertEqual(result, 4)

    def test_final(self):
        table = self.create_table()

        query = self.session.query(table.c.x).final().group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x AS t1_x FROM t1 FINAL GROUP BY x'
        )

    def test_lambda_functions(self):
        query = self.session.query(
            func.arrayFilter(
                Lambda(lambda x: x.like('%World%')),
                literal(['Hello', 'abc World'], types.Array(types.String))
            ).label('test')
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            "SELECT arrayFilter("
            # See `def test_like` about the double percentages.
            "x -> x LIKE '%%World%%', ['Hello', 'abc World']"
            ") AS test"
        )


class SelectHttpTestCase(SelectTestCase, HttpSessionTestCase):
    """ ... """


class SelectNativeTestCase(SelectTestCase, NativeSessionTestCase):
    """ ... """


class JoinTestCase(BaseAbstractTestCase):
    def create_tables(self, num):
        metadata = self.metadata()

        return [Table(
            't{}'.format(i), metadata,
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Int32, primary_key=True),
        ) for i in range(num)]

    def test_joins(self):
        t1, t2 = self.create_tables(2)

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            t1.c.x == t1.c.y,
            strictness='any')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ANY INNER JOIN t1 ON x = y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            t1.c.x == t1.c.y,
            type='inner',
            strictness='any')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ANY INNER JOIN t1 ON x = y"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(
            t2,
            tuple_(t1.c.x, t1.c.y),
            type='inner',
            strictness='all'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ALL INNER JOIN t1 USING (x, y)"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .join(t2,
                  tuple_(t1.c.x, t1.c.y),
                  type='inner',
                  strictness='all',
                  distribution='global')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL INNER JOIN t1 USING (x, y)"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(
            t2,
            tuple_(t1.c.x, t1.c.y),
            type='left outer',
            strictness='all',
            distribution='global'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL LEFT OUTER JOIN t1 USING (x, y)"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(
            t2,
            tuple_(t1.c.x, t1.c.y),
            type='LEFT OUTER',
            strictness='ALL',
            distribution='GLOBAL'
        )

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "GLOBAL ALL LEFT OUTER JOIN t1 USING (x, y)"
        )

        query = self.session.query(t1.c.x, t2.c.x) \
            .outerjoin(t2,
                       tuple_(t1.c.x, t1.c.y),
                       strictness='ALL',
                       type='FULL OUTER')

        self.assertEqual(
            self.compile(query),
            "SELECT x AS t0_x, x AS t1_x FROM t0 "
            "ALL FULL OUTER JOIN t1 USING (x, y)"
        )


class JoinHttpTestCase(JoinTestCase, HttpSessionTestCase):
    """ Join over a HTTP session """


class JoinNativeTestCase(JoinTestCase, NativeSessionTestCase):
    """ Join over a native protocol session """


class YieldTest(NativeSessionTestCase):
    def test_yield_per_and_execution_options(self):
        numbers = Table(
            'numbers', self.metadata(),
            Column('number', types.UInt64, primary_key=True),
        )

        query = self.session.query(numbers.c.number).limit(100).yield_per(15)
        query = query.execution_options(foo='bar')
        self.assertIsNotNone(query.load_options._yield_per)
        self.assertEqual(
            query._execution_options,
            {
                # 'stream_results': True,
                'foo': 'bar',
                # 'max_row_buffer': 15,
            }
        )

    def test_basic(self):
        numbers = Table(
            'numbers', self.metadata(),
            Column('number', types.UInt64, primary_key=True),
        )

        q = iter(
            self.session.query(numbers.c.number)
            .yield_per(1)
            .from_statement(text('SELECT * FROM system.numbers LIMIT 3'))
        )

        ret = []
        ret.append(next(q))
        ret.append(next(q))
        ret.append(next(q))
        try:
            next(q)
            self.assertTrue(False)
        except StopIteration:
            pass

        self.assertEqual(ret, [(0, ), (1, ), (2, )])
