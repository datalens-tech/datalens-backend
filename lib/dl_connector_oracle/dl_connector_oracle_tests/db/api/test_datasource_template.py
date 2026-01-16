import dl_api_lib_testing

from dl_connector_oracle.core.constants import (
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
)
from dl_connector_oracle.core.settings import OracleConnectorSettings
from dl_connector_oracle_tests.db.api.base import (
    OracleDataApiTestBase,
    OracleDatasetTestBase,
)


class BaseTableTestSourceTemplate(dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_ORACLE_TABLE
    conn_settings_cls = OracleConnectorSettings
    table_name_pattern = "TABLE_.*"
    invalid_table_name = "TABLE_INVALID"
    failed_constraint_table_name = "FAILED_CONSTRAINT_TABLE_NAME"


class TestTableControlApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    OracleDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    OracleDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateConnectionDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    OracleDatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    OracleDataApiTestBase,
):
    ...


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_ORACLE_SUBSELECT
    conn_settings_cls = OracleConnectorSettings
    table_name_pattern = "TABLE_.*"
    invalid_table_name = "TABLE_INVALID"
    failed_constraint_table_name = "FAILED_CONSTRAINT_TABLE_NAME"


class TestSubselectControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    OracleDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    OracleDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    OracleDatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    OracleDataApiTestBase,
):
    ...
