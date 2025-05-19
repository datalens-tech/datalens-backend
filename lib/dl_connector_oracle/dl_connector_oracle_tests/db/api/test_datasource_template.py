import dl_api_lib_testing

from dl_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_SUBSELECT
from dl_connector_oracle.core.settings import OracleConnectorSettings
from dl_connector_oracle_tests.db.api.base import (
    OracleDataApiTestBase,
    OracleDatasetTestBase,
)


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_ORACLE_SUBSELECT
    conn_settings_cls = OracleConnectorSettings
    table_name_pattern = "TABLE_.*"
    invalid_table_name = "TABLE_INVALID"
    failed_constraint_table_name = "FAILED_CONSTRAINT_TABLE_NAME"


class TestControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    OracleDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    OracleDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    OracleDatasetTestBase,
):
    ...


class TestDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    OracleDataApiTestBase,
):
    ...
