from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from clickhouse_sqlalchemy.sql.ddl import DropTable
from clickhouse_sqlalchemy_tests.testcase import BaseTestCase
from clickhouse_sqlalchemy_tests.session import mocked_engine


class DDLTestCase(BaseTestCase):
    def test_create_table(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.String),
            Column('z', types.String(10)),
            # Must be quoted:
            Column('index', types.String),
            engines.Memory()
        )

        # No NOT NULL. And any PKS.
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int32, y String, z FixedString(10), "index" String) '
            'ENGINE = Memory'
        )

    def test_create_table_nested_types(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.String)),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(String)) '
            'ENGINE = Memory'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = Memory'
        )

        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Array(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Array(String))) '
            'ENGINE = Memory'
        )

    def test_create_table_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(types.String)),
            Column('z', types.Nullable(types.String(10))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(String), z Nullable(FixedString(10))) '
            'ENGINE = Memory'
        )

    def test_create_table_nested(self):
        table = Table(
            't1',
            self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('parent', types.Nested(
                Column('child1', types.Int32),
                Column('child2', types.String),
            )),
            engines.Memory()
        )
        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 ('
            'x Int32, '
            'parent Nested('
            'child1 Int32, '
            "child2 String"
            ')'
            ') ENGINE = Memory'
        )

    def test_create_table_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Array(types.Nullable(types.String))),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Array(Nullable(String))) '
            'ENGINE = Memory'
        )

    def test_create_table_nullable_nested_nullable(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True),
            Column('y', types.Nullable(
                types.Array(types.Nullable(types.String)))
            ),
            engines.Memory()
        )

        self.assertEqual(
            self.compile(CreateTable(table)),
            'CREATE TABLE t1 '
            '(x Int32, y Nullable(Array(Nullable(String)))) '
            'ENGINE = Memory'
        )

    def test_table_create_on_cluster(self):
        create_sql = (
            'CREATE TABLE t1 ON CLUSTER test_cluster '
            '(x Int32) ENGINE = Memory'
        )

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(session=engine.session),
                Column('x', types.Int32, primary_key=True),
                engines.Memory(),
                clickhouse_cluster='test_cluster'
            )

            table.create()
            self.assertEqual(engine.history, [create_sql])

        self.assertEqual(
            self.compile(CreateTable(table)),
            create_sql
        )

    def test_drop_table_clause(self):
        table = Table(
            't1', self.metadata(),
            Column('x', types.Int32, primary_key=True)
        )

        self.assertEqual(
            self.compile(DropTable(table)),
            'DROP TABLE t1'
        )
        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            'DROP TABLE IF EXISTS t1'
        )

    def test_table_drop(self):
        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(session=engine.session),
                Column('x', types.Int32, primary_key=True)
            )
            table.drop(if_exists=True)
            self.assertEqual(engine.history, ['DROP TABLE IF EXISTS t1'])

    def test_table_drop_on_cluster(self):
        drop_sql = 'DROP TABLE IF EXISTS t1 ON CLUSTER test_cluster'

        with mocked_engine() as engine:
            table = Table(
                't1', self.metadata(session=engine.session),
                Column('x', types.Int32, primary_key=True),
                clickhouse_cluster='test_cluster'
            )
            table.drop(if_exists=True)
            self.assertEqual(engine.history, [drop_sql])

        self.assertEqual(
            self.compile(DropTable(table, if_exists=True)),
            drop_sql
        )
