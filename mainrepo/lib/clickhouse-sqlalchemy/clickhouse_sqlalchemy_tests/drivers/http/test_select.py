from sqlalchemy import Column

from clickhouse_sqlalchemy import types, Table
from clickhouse_sqlalchemy_tests.testcase import HttpSessionTestCase


class FormatSectionTestCase(HttpSessionTestCase):

    @property
    def execution_ctx_cls(self):
        return self.session.bind.dialect.execution_ctx_cls

    def _compile(self, clause, bind=None, **kwargs):
        if bind is None:
            bind = self.session.bind
        statement = super(FormatSectionTestCase, self)._compile(
            clause, bind=bind, **kwargs
        )

        context = self.execution_ctx_cls._init_compiled(
            dialect=bind.dialect, connection=bind, dbapi_connection=bind,
            execution_options={}, compiled=statement, parameters=(),
            invoked_statement=statement, extracted_parameters=(),
        )
        context.pre_exec()

        return context.statement

    def test_select_format_clause(self):
        metadata = self.metadata()

        bind = self.session.bind
        bind.cursor = lambda: None

        table = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        statement = self.compile(self.session.query(table.c.x), bind=bind)
        self.assertEqual(
            statement,
            'SELECT x AS t1_x FROM t1 FORMAT TabSeparatedWithNamesAndTypes'
        )

    def test_insert_from_select_no_format_clause(self):
        metadata = self.metadata()

        bind = self.session.bind
        bind.cursor = lambda: None

        t1 = Table(
            't1', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        t2 = Table(
            't2', metadata,
            Column('x', types.Int32, primary_key=True)
        )

        query = t2.insert() \
            .from_select(['x'], self.session.query(t1.c.x).subquery())
        statement = self.compile(query, bind=bind)
        self.assertIn(statement, [
            'INSERT INTO t2 (x) SELECT x FROM t1',
            'INSERT INTO t2 (x) SELECT x FROM (SELECT x AS x FROM t1) AS anon_1',
        ])
