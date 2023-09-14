from http import HTTPStatus

import pytest
import sqlalchemy as sa

from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows
from bi_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from bi_api_lib_testing.data_api_base import DataApiTestParams
from bi_constants.enums import (
    BIType,
    WhereClauseOperation,
)
from bi_core_testing.database import (
    C,
    Db,
    DbTable,
    make_pg_table_with_enums,
    make_table,
)
from bi_core_testing.dataset import data_source_settings_from_table
from bi_sqlalchemy_postgres.base import CITEXT

from bi_connector_postgresql_tests.db.api.base import PostgreSQLDataApiTestBase


class TestPostgreSQLDataResult(PostgreSQLDataApiTestBase, DefaultConnectorDataResultTestSuite):
    do_test_arrays = True

    def test_isnull(
        self, saved_dataset: Dataset, data_api_test_params: DataApiTestParams, data_api: SyncHttpDataApiV2
    ) -> None:
        """
        Postgres has a specific implementation for the ISNULL function
        with constant literals, we basically check that it just works
        """
        ds = saved_dataset
        ds.result_schema["is_null"] = ds.field(formula="ISNULL('3')")

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title=data_api_test_params.two_dims[0]),
                ds.find_field(title="is_null"),
            ],
            filters=[
                ds.find_field(title="is_null").filter(
                    op=WhereClauseOperation.EQ,
                    values=[False],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows

    def test_enums(
        self,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        dataset_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        db_table = make_pg_table_with_enums(db)
        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(dataset_api, connection_id=saved_connection_id, dataset_params=params)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="col1"),
                ds.find_field(title="col2"),
                ds.find_field(title="col3"),
            ],
            filters=[
                ds.find_field(title="col1").filter(WhereClauseOperation.IN, values=["var1"]),
                ds.find_field(title="col2").filter(WhereClauseOperation.IN, values=["str2"]),
                ds.find_field(title="col3").filter(WhereClauseOperation.IN, values=["2"]),
            ],
        )
        assert result_resp.status_code == 200, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows == [["var1", "str2", "2"]]

    @pytest.fixture(scope="function")
    def enabled_citext_extension(self, db: Db) -> None:
        db.execute("CREATE EXTENSION IF NOT EXISTS CITEXT;")

    def test_pg_citext_column(
        self,
        dataset_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        db: Db,
        saved_connection_id: str,
        enabled_citext_extension,
    ) -> None:
        table = db.table_from_columns(
            [sa.Column(name="citext_value", type_=CITEXT)],
        )
        db.create_table(table)
        db_table = DbTable(db=db, table=table)  # type: ignore  # TODO: fix
        db_table.insert(
            [
                {"citext_value": "var1"},
                {"citext_value": "var2"},
                {"citext_value": "var3"},
            ]
        )
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id, **data_source_settings_from_table(db_table)
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = dataset_api.apply_updates(dataset=ds).dataset
        ds = dataset_api.save_dataset(ds).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="citext_value"),
            ],
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 3
        assert list(sorted(data_rows)) == [["var1"], ["var2"], ["var3"]]


class TestPostgreSQLDataGroupBy(PostgreSQLDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestPostgreSQLDataRange(PostgreSQLDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestPostgreSQLDataDistinct(PostgreSQLDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    def test_bigint_literal(
        self,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        dataset_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        columns = [
            C(name="bigint_value", user_type=BIType.integer, vg=lambda rn, **kwargs: 10000002877 + rn),
        ]
        db_table = make_table(db, columns=columns)
        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(dataset_api, connection_id=saved_connection_id, dataset_params=params)

        lower_bound = 10000002879
        distinct_resp = data_api.get_distinct(
            dataset=ds,
            field=ds.find_field(title="bigint_value"),
            filters=[
                ds.find_field(title="bigint_value").filter(
                    op=WhereClauseOperation.GTE,
                    values=[str(lower_bound)],
                ),
            ],
            fail_ok=True,
        )
        assert distinct_resp.status_code == 200, distinct_resp.json
        data_rows = get_data_rows(distinct_resp)
        assert data_rows
        assert all(int(row[0]) >= lower_bound for row in data_rows)


class TestPostgreSQLDataPreview(PostgreSQLDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
