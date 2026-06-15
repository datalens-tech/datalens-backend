import dl_api_lib_testing

from dl_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_SUBSELECT
from dl_connector_oracle.core.settings import OracleConnectorSettings
from dl_connector_oracle_tests.db.api.base import OracleDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    OracleDataApiTestBase,
):
    source_type = SOURCE_TYPE_ORACLE_SUBSELECT
    conn_settings_cls = OracleConnectorSettings
    # Oracle has no FROM-less SELECT (needs DUAL); the alias is left unquoted so it folds to
    # upper-case WHO and the wrapping query's unquoted `t1.who` resolves — a quoted alias → ORA-00904.
    subselect_who_sql = "SELECT '{{_sys.user_id}}' AS WHO FROM DUAL"
