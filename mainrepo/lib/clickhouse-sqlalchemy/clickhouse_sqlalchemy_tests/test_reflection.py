import enum

from sqlalchemy import Column, inspect

from clickhouse_sqlalchemy import types, engines, Table
from clickhouse_sqlalchemy_tests.testcase import (
    TypesTestCase, HttpSessionTestCase, NativeSessionTestCase,
)
from clickhouse_sqlalchemy_tests.util import require_server_version


class ReflectionTestCase(TypesTestCase):
    def _type_round_trip(self, *types):
        metadata = self.metadata()

        args = (
            [Column('t%d' % i, type_) for i, type_ in enumerate(types)] +
            [engines.Memory()]
        )

        table = Table('t', metadata, *args)
        with self.create_table(table):
            return [
                c['type'] for c in
                inspect(metadata.bind).get_columns('t')
            ]

    def test_array(self):
        coltype = self._type_round_trip(types.Array(types.Int32))[0]

        self.assertIsInstance(coltype, types.Array)
        self.assertEqual(coltype.item_type, types.Int32)

    def test_array_of_array(self):
        coltype = self._type_round_trip(
            types.Array(types.Array(types.Int32))
        )[0]

        self.assertIsInstance(coltype, types.Array)
        self.assertIsInstance(coltype.item_type, types.Array)
        self.assertEqual(coltype.item_type.item_type, types.Int32)

    def test_sting_length(self):
        coltype = self._type_round_trip(types.String(10))[0]

        self.assertIsInstance(coltype, types.String)
        self.assertEqual(coltype.length, 10)

    def test_nullable(self):
        coltype = self._type_round_trip(types.Nullable(types.Int32))[0]

        # self.assertIsInstance(coltype, types.Nullable)
        # self.assertEqual(coltype.nested_type, types.Int32)
        self.assertIsInstance(coltype, types.Int32)

    @require_server_version(19, 3, 3)
    def test_low_cardinality(self):
        coltype = self._type_round_trip(types.LowCardinality(types.String))[0]

        # self.assertIsInstance(coltype, types.LowCardinality)
        # self.assertEqual(coltype.nested_type, types.String)
        self.assertIsInstance(coltype, types.String)

    def test_enum8(self):
        enum_options = {'three': 3, "quoted' ": 9, 'comma, ': 14}
        coltype = self._type_round_trip(
            types.Enum8(enum.Enum('any8_enum', enum_options))
        )[0]

        self.assertIsInstance(coltype, types.Enum8)

        self.assertEqual(sorted(coltype.enums), sorted(enum_options))
        if coltype.enum_class is not None:
            self.assertEqual(
                {o.name: o.value for o in coltype.enum_class}, enum_options,
            )

    def test_enum16(self):
        enum_options = {'first': 1024, 'second': 2048}
        coltype = self._type_round_trip(
            types.Enum16(enum.Enum('any16_enum', enum_options))
        )[0]

        self.assertIsInstance(coltype, types.Enum16)

        self.assertEqual(sorted(coltype.enums), sorted(enum_options))
        if coltype.enum_class is not None:
            self.assertEqual(
                {o.name: o.value for o in coltype.enum_class}, enum_options,
            )

    def test_decimal(self):
        coltype = self._type_round_trip(types.Decimal(38, 38))[0]

        self.assertIsInstance(coltype, types.Decimal)
        self.assertEqual(coltype.precision, 38)
        self.assertEqual(coltype.scale, 38)

    def test_datetimewithtz(self):
        coltype = self._type_round_trip(types.DateTimeWithTZ('Etc/UTC'))[0]
        self.assertIsInstance(coltype, types.DateTimeWithTZ)
        self.assertEqual(coltype.tz, 'Etc/UTC')


class ReflectionHttpTestCase(ReflectionTestCase, HttpSessionTestCase):
    """ Reflection over a HTTP session """


class ReflectionNativeTestCase(ReflectionTestCase, NativeSessionTestCase):
    """ Reflection over a native protocol session """
