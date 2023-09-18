from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_chyt_internal.bi.connection_form.token_auth_form.form_config import (
    CHYTInternalTokenConnectionFormFactory,
)
from bi_connector_chyt_internal.bi.connection_form.user_auth_form.form_config import (
    CHYTInternalUserConnectionFormFactory,
)
from bi_connector_chyt_internal.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_CHYT_CONFIGS


class TestCHYTTokenAuthConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHYTInternalTokenConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_CHYT_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHYTUserAuthConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHYTInternalUserConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_CHYT_CONFIGS + BI_API_CONNECTOR_CONFIGS
