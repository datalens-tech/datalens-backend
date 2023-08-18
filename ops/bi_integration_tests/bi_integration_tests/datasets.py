import attr

from bi_integration_tests.constants import SOURCE_TYPE_PG_TABLE_STR


@attr.s
class IntegrationTestDataset:
    source_type: str = attr.ib()
    db_name: str = attr.ib()
    table_name: str = attr.ib()


PG_SQL_FEATURES = IntegrationTestDataset(
    source_type=SOURCE_TYPE_PG_TABLE_STR,
    db_name="information_schema",
    table_name="sql_features"
)
PG_SALES = IntegrationTestDataset(
    source_type=SOURCE_TYPE_PG_TABLE_STR,
    db_name="public",
    table_name="integration_tests_sales"
)
