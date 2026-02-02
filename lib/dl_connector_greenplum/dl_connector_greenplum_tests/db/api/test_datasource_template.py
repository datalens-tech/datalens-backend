import dl_api_lib_testing

from dl_connector_greenplum.core.constants import (
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum.core.settings import GreenplumConnectorSettings
from dl_connector_greenplum_tests.db.api.base import (
    GP6DataApiTestBase,
    GP6DatasetTestBase,
    GP7DataApiTestBase,
    GP7DatasetTestBase,
)


class BaseTableTestSourceTemplate(dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_GP_TABLE
    conn_settings_cls = GreenplumConnectorSettings


class TestTableControlApiSourceTemplateGP6(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    GP6DatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateGP7(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    GP7DatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabledGP6(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    GP6DatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabledGP7(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    GP7DatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateConnectionDisabledGP6(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    GP6DatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateConnectionDisabledGP7(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    GP7DatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplateGP6(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    GP6DataApiTestBase,
):
    ...


class TestTableDataApiSourceTemplateGP7(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    GP7DataApiTestBase,
):
    ...


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_GP_SUBSELECT
    conn_settings_cls = GreenplumConnectorSettings


class TestSubselectControlApiSourceTemplateGP6(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    GP6DatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateGP7(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    GP7DatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabledGP6(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    GP6DatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabledGP7(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    GP7DatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabledGP6(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    GP6DatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabledGP7(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    GP7DatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplateGP6(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    GP6DataApiTestBase,
):
    ...


class TestSubselectDataApiSourceTemplateGP7(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    GP7DataApiTestBase,
):
    ...
