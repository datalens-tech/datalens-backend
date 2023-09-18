import pytest
import sqlalchemy as sa

from dl_formula_testing.database import (
    Db,
    FormulaDbDispenser,
)
from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from bi_connector_yql.formula.constants import YqlDialect as D
from bi_connector_yql_tests.db.config import DB_CONFIGURATIONS


class YqlDbDispenser(FormulaDbDispenser):
    def ensure_db_is_up(self, db: Db) -> tuple[bool, str]:
        # first, check that the db is up
        status, msg = super().ensure_db_is_up(db)
        if not status:
            return status, msg

        test_table = db.table_from_columns([sa.Column(name="col1", type_=sa.Integer())])

        # secondly, try to create a test table: for some reason
        # it could be that YDB is up but you still can't do it.
        # see https://st.yandex-team.ru/KIKIMR-14589
        try:
            db.create_table(test_table)
            db.drop_table(test_table)
            return True, ""
        except Exception as exc:
            return False, str(exc)


class YQLTestBase(FormulaConnectorTestBase):
    dialect = D.YDB
    supports_arrays = False
    supports_uuid = True
    db_dispenser = YqlDbDispenser()

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CONFIGURATIONS[self.dialect]
