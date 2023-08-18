from bi_constants.enums import ConnectionType

from bi_connector_chyt.core.type_transformer import (
    CHYTTypeTransformer,
    make_chyt_native_to_user_map,
    make_chyt_user_to_native_map,
)


class CHYTInternalTypeTransformer(CHYTTypeTransformer):
    conn_type = ConnectionType.ch_over_yt

    native_to_user_map = make_chyt_native_to_user_map(ConnectionType.ch_over_yt)
    user_to_native_map = make_chyt_user_to_native_map(ConnectionType.ch_over_yt)


class CHYTUserAuthTypeTransformer(CHYTTypeTransformer):
    conn_type = ConnectionType.ch_over_yt_user_auth

    native_to_user_map = make_chyt_native_to_user_map(ConnectionType.ch_over_yt_user_auth)
    user_to_native_map = make_chyt_user_to_native_map(ConnectionType.ch_over_yt_user_auth)
