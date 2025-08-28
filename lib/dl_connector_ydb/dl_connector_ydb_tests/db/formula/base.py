from frozendict import frozendict
import pytest
import sqlalchemy as sa

from dl_formula_testing.database import (
    Db,
    FormulaDbDispenser,
)
from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_ydb.formula.constants import YqlDialect as D
import dl_connector_ydb_tests.db.config as test_config
from dl_connector_ydb_tests.db.config import DB_CONFIGURATIONS


class YqlDbDispenser(FormulaDbDispenser):
    def ensure_db_is_up(self, db: Db) -> tuple[bool, str]:
        # first, check that the db is up
        status, msg = super().ensure_db_is_up(db)
        if not status:
            return status, msg

        test_table = db.table_from_columns([sa.Column(name="col1", type_=sa.Integer())])

        # secondly, try to create a test table: for some reason
        # it could be that YDB is up but you still can't do it
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

    @pytest.fixture(scope="class")
    def engine_params(self) -> dict:
        return dict(
            connect_args=frozendict(
                dict(
                    host=test_config.CoreConnectionSettings.HOST,
                    port=test_config.CoreConnectionSettings.PORT,
                    protocol="grpc",
                )
            ),
            _add_declare_for_yql_stmt_vars=True,
        )
