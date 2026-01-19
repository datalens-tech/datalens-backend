import abc

import pytest

from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_greenplum_tests.db.api.base import (
    GP6DatasetTestBase,
    GP7DatasetTestBase,
)


class GreenplumDatasetTestSuite(DefaultConnectorDatasetTestSuite, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="function")
    def source_listing_values(self) -> dict[str, bool | str | None]:
        return {
            "supports_source_search": True,
            "supports_source_pagination": True,
            "supports_db_name_listing": False,
            "db_name_required_for_search": False,
        }


class TestGP6Dataset(GP6DatasetTestBase, GreenplumDatasetTestSuite):
    pass


class TestGP7Dataset(GP7DatasetTestBase, GreenplumDatasetTestSuite):
    pass
