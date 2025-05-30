from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite

from dl_connector_trino_tests.db.api.base import TrinoDataApiTestBase


class TestTrinoBasicComplexQueries(TrinoDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    pass
