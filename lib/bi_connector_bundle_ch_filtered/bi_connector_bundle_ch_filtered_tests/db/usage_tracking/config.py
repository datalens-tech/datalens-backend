from bi_api_commons.base_models import RequestContextInfo
from bi_api_commons_ya_cloud.models import TenantYCFolder, IAMAuthData
from bi_testing.containers import get_test_container_hostport

from bi_connector_bundle_ch_filtered.usage_tracking.core.settings import UsageTrackingConnectionSettings

SR_CONNECTION_TABLE_NAME = 'sample'
SR_CONNECTION_SETTINGS = UsageTrackingConnectionSettings(
    DB_NAME='test_data',
    HOST=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host,
    PORT=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port,
    USERNAME='datalens',
    PASSWORD='qwerty',
    USE_MANAGED_NETWORK=False,
    ALLOWED_TABLES=[],
    REQUIRED_IAM_ROLE='datalens.instances.admin',
    SUBSELECT_TEMPLATES=(
        {
            'title': SR_CONNECTION_TABLE_NAME,
            'sql_query': "SELECT 'some_folder_id' as folder_id "
                         "WHERE folder_id = :tenant_id"
        },
    )
)

RCI = RequestContextInfo.create(
    request_id=None,
    tenant=TenantYCFolder(folder_id='folder_1'),
    user_id='datalens_id',
    user_name='datalens',
    x_dl_debug_mode=False,
    endpoint_code=None,
    x_dl_context=None,
    plain_headers={},
    secret_headers={},
    auth_data=IAMAuthData(iam_token='dummy_iam_token'),
)
RCI_WITH_WRONG_AUTH = RCI.clone(auth_data=IAMAuthData(iam_token='wrong_iam_token'))
