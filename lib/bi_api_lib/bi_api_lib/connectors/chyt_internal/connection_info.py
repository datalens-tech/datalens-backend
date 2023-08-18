from bi_api_connector.connection_info import ConnectionInfoProvider

from bi_api_lib.i18n.localizer import Translatable


class CHYTInternalTokenConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-ch_over_yt')


class CHYTUserAuthConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-ch_over_yt_user_auth')
