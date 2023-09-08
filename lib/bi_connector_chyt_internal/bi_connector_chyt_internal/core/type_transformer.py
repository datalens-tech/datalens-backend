from bi_connector_chyt.core.type_transformer import (
    CHYTTypeTransformer,
    make_chyt_native_to_user_map,
    make_chyt_user_to_native_map,
)

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)


class CHYTInternalTypeTransformer(CHYTTypeTransformer):
    conn_type = CONNECTION_TYPE_CH_OVER_YT

    native_to_user_map = make_chyt_native_to_user_map(CONNECTION_TYPE_CH_OVER_YT)
    user_to_native_map = make_chyt_user_to_native_map(CONNECTION_TYPE_CH_OVER_YT)


class CHYTUserAuthTypeTransformer(CHYTTypeTransformer):
    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    native_to_user_map = make_chyt_native_to_user_map(CONNECTION_TYPE_CH_OVER_YT_USER_AUTH)
    user_to_native_map = make_chyt_user_to_native_map(CONNECTION_TYPE_CH_OVER_YT_USER_AUTH)
