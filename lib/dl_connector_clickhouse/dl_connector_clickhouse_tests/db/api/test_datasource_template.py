import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse_tests.db.api.base import (
    ClickHouseDataApiTestBase,
    ClickHouseDatasetTestBase,
)
from dl_connector_clickhouse_tests.db.config import CoreConnectionSettings


class BaseTableTestSourceTemplate(dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_CH_TABLE
    conn_settings_cls = ClickHouseConnectorSettings

    @pytest.fixture(name="datasource_parameters")
    def fixture_datasource_parameters(self) -> dict[str, str]:
        return dict(
            table_name="{{table_name}}",
            db_name=CoreConnectionSettings.DB_NAME,
        )

    @pytest.fixture(name="invalid_datasource_parameters")
    def fixture_invalid_datasource_parameters(self) -> dict[str, str]:
        return dict(
            table_name="{{invalid_parameter_name}}",
            db_name=CoreConnectionSettings.DB_NAME,
        )


class TestTableControlApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    ClickHouseDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    ClickHouseDatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplateConnectionDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    ClickHouseDataApiTestBase,
):
    ...


class TestTableDataApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    ClickHouseDataApiTestBase,
):
    ...


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_CH_SUBSELECT
    conn_settings_cls = ClickHouseConnectorSettings
    table_name_pattern = f"{CoreConnectionSettings.DB_NAME}.table_.*"
    invalid_table_name = f"{CoreConnectionSettings.DB_NAME}.table_invalid"

    @pytest.fixture(name="table_name")
    def fixture_table_name(self, sample_table: DbTable) -> str:
        return f"{CoreConnectionSettings.DB_NAME}.{sample_table.name}"


class TestSubselectControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    ClickHouseDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    ClickHouseDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    ClickHouseDatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    ClickHouseDataApiTestBase,
):
    ...
