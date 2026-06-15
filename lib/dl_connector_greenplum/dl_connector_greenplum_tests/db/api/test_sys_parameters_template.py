import dl_api_lib_testing

from dl_connector_greenplum.core.constants import SOURCE_TYPE_GP_SUBSELECT
from dl_connector_greenplum.core.settings import GreenplumConnectorSettings
from dl_connector_greenplum_tests.db.api.base import (
    GP6DataApiTestBase,
    GP7DataApiTestBase,
)


class BaseTestSysUserIdInSourceTemplate(dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate):
    source_type = SOURCE_TYPE_GP_SUBSELECT
    conn_settings_cls = GreenplumConnectorSettings
    # Greenplum 6 types a bare string literal as `unknown`, which DataLens cannot select; an
    # explicit TEXT cast yields a usable `text` column on both GP6 and GP7.
    subselect_who_sql = "SELECT CAST('{{_sys.user_id}}' AS TEXT) AS who"


class TestSysUserIdInSourceTemplateGP6(
    BaseTestSysUserIdInSourceTemplate,
    GP6DataApiTestBase,
): ...


class TestSysUserIdInSourceTemplateGP7(
    BaseTestSysUserIdInSourceTemplate,
    GP7DataApiTestBase,
): ...
