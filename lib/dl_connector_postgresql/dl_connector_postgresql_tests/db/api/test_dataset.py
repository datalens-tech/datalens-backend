import pytest

from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_postgresql_tests.db.api.base import PostgreSQLDatasetTestBase


class TestPostgreSQLDataset(PostgreSQLDatasetTestBase, DefaultConnectorDatasetTestSuite):
    @pytest.fixture(scope="function")
    def source_listing_values(self) -> dict[str, bool | str | None]:
        return {
            "supports_source_search": True,
            "supports_source_pagination": True,
            "supports_db_name_listing": False,
            "db_name_required_for_search": False,
        }
