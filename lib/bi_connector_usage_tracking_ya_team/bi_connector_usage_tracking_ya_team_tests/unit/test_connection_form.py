from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_usage_tracking_ya_team.api.connection_form.form_config import UsageTrackingYaTeamConnectionFormFactory
from bi_connector_usage_tracking_ya_team.api.i18n.localizer import (
    CONFIGS as BI_CONNECTOR_USAGE_TRACKING_YA_TEAM_CONFIGS,
)


class TestUsageTrackingYaTeamConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = UsageTrackingYaTeamConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_USAGE_TRACKING_YA_TEAM_CONFIGS
