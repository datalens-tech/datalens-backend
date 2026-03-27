import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_starrocks.core.constants import (
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks.core.settings import StarRocksConnectorSettings
from dl_connector_starrocks_tests.db.api.base import (
    StarRocksDataApiTestBase,
    StarRocksDatasetTestBase,
)
from dl_connector_starrocks_tests.db.config import CoreConnectionSettings


class BaseTableTestSourceTemplate(dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_STARROCKS_TABLE
    conn_settings_cls = StarRocksConnectorSettings

    @pytest.fixture(name="datasource_parameters")
    def fixture_datasource_parameters(self) -> dict[str, str]:
        return dict(
            table_name="{{table_name}}",
            db_name=CoreConnectionSettings.CATALOG,
            schema_name=CoreConnectionSettings.DB_NAME,
        )

    @pytest.fixture(name="invalid_datasource_parameters")
    def fixture_invalid_datasource_parameters(self) -> dict[str, str]:
        return dict(
            table_name="{{invalid_parameter_name}}",
            db_name=CoreConnectionSettings.CATALOG,
            schema_name=CoreConnectionSettings.DB_NAME,
        )


class TestTableControlApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    StarRocksDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    StarRocksDatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplateConnectionDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    StarRocksDataApiTestBase,
):
    ...


class TestTableDataApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    StarRocksDataApiTestBase,
):
    ...


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_STARROCKS_SUBSELECT
    conn_settings_cls = StarRocksConnectorSettings
    table_name_pattern = f"{CoreConnectionSettings.DB_NAME}.table_.*"
    invalid_table_name = f"{CoreConnectionSettings.DB_NAME}.table_invalid"

    @pytest.fixture(name="table_name")
    def fixture_table_name(self, sample_table: DbTable) -> str:
        return f"{CoreConnectionSettings.DB_NAME}.{sample_table.name}"


class TestSubselectControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    StarRocksDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    StarRocksDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    StarRocksDatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    StarRocksDataApiTestBase,
):
    ...
