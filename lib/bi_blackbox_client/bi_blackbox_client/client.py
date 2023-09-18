import logging
from typing import Optional

import attr

from bi_blackbox_client.authenticate import (
    authenticate,
    authenticate_async,
)
from dl_app_tools.profiling_base import (
    generic_profiler,
    generic_profiler_async,
)

LOGGER = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class BlackboxSettings:
    uri: str
    client_id_name: str
    force_tvm: bool


BLACKBOX_SETTINGS_BY_NAME = {
    "Test": BlackboxSettings(
        uri="https://blackbox-test.yandex.net/blackbox",
        client_id_name="Test",
        force_tvm=False,
    ),
    "Mimino": BlackboxSettings(
        uri="https://blackbox-mimino.yandex.net/blackbox",
        client_id_name="Mimino",
        force_tvm=True,
    ),
    "Prod": BlackboxSettings(
        uri="https://blackbox.yandex.net/blackbox",
        client_id_name="Prod",
        force_tvm=True,
    ),
    "ProdYateam": BlackboxSettings(
        uri="https://blackbox.yandex-team.ru/blackbox",
        client_id_name="ProdYateam",
        force_tvm=True,
    ),
}


def _cast_passport_uid(user_id: Optional[str]) -> Optional[int]:
    if user_id is None:
        return None
    try:
        return int(user_id)
    except ValueError:
        LOGGER.warning(f"passport uid is not integer value: {user_id!r}")
        return None


@attr.s
class BlackboxClient:
    _blackbox_name: str = attr.ib()

    @generic_profiler_async("get_user_id_by_oauth_token_async")  # type: ignore  # TODO: fix
    async def get_user_id_by_oauth_token_async(self, token: str) -> Optional[int]:
        bb_settings = BLACKBOX_SETTINGS_BY_NAME[self._blackbox_name]
        auth_res = await authenticate_async(
            authorization_header="OAuth {}".format(token),
            userip="::1",
            host="localhost",
            blackbox_uri=bb_settings.uri,
            blackbox_client_id_name=bb_settings.client_id_name,
            force_tvm=bb_settings.force_tvm,
            scopes=(),
        )
        return _cast_passport_uid(auth_res.get("user_id"))

    @generic_profiler("get_user_id_by_oauth_token")
    def get_user_id_by_oauth_token_sync(self, token: str) -> Optional[int]:
        bb_settings = BLACKBOX_SETTINGS_BY_NAME[self._blackbox_name]
        auth_res = authenticate(
            authorization_header="OAuth {}".format(token),
            userip="::1",
            host="localhost",
            blackbox_uri=bb_settings.uri,
            blackbox_client_id_name=bb_settings.client_id_name,
            force_tvm=bb_settings.force_tvm,
            scopes=(),
        )
        return _cast_passport_uid(auth_res.get("user_id"))
