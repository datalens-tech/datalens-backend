import dl_api_lib_testing

from dl_connector_greenplum.core.constants import SOURCE_TYPE_GP_SUBSELECT
from dl_connector_greenplum.core.settings import GreenplumConnectorSettings
from dl_connector_greenplum_tests.db.api.base import (
    GreenplumDataApiTestBase,
    GreenplumDatasetTestBase,
)


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_GP_SUBSELECT
    conn_settings_cls = GreenplumConnectorSettings


class TestControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    GreenplumDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    GreenplumDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    GreenplumDatasetTestBase,
):
    ...


class TestDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    GreenplumDataApiTestBase,
):
    ...
