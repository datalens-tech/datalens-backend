import pkgutil
from typing import Optional

import attr

from dl_core_testing.csv_table_dumper import CsvTableDumper
from dl_core_testing.database import (
    Db,
    DbConfig,
    DbTable,
)
from dl_core_testing.fixtures.primitives import FixtureTableSpec


@attr.s
class DbCsvTableDispenser:
    _tables: dict[DbConfig, dict[FixtureTableSpec, DbTable]] = attr.ib(init=False, factory=dict)
    _table_name_prefix: Optional[str] = attr.ib(default=None)
    _bulk_insert: bool = attr.ib(default=False)

    def _get_raw_csv_data(self, path: str) -> str:
        byte_data = pkgutil.get_data(__name__, path)
        assert byte_data is not None
        return byte_data.decode()

    def _make_new_csv_table(self, db: Db, spec: FixtureTableSpec) -> DbTable:
        dumper = CsvTableDumper(db=db)
        db_table = dumper.make_table_from_csv(
            raw_csv_data=self._get_raw_csv_data(spec.csv_name),
            table_schema=spec.table_schema,
            nullable=spec.nullable,
            table_name_prefix=self._table_name_prefix,
            bulk_insert=self._bulk_insert,
        )
        if db.config not in self._tables:
            self._tables[db.config] = {}
        self._tables[db.config][spec] = db_table
        return db_table

    def get_csv_table(self, db: Db, spec: FixtureTableSpec) -> DbTable:
        try:
            return self._tables[db.config][spec]
        except KeyError:
            return self._make_new_csv_table(db=db, spec=spec)
