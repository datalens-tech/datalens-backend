import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connection_executors.async_sa_executors import _populate_keeper_from_target_dto_pool
from dl_obfuscator import SecretKeeper


def _secrepr(_value: object) -> str:
    return "***"


@attr.s(frozen=True)
class _StubTargetDTO:
    password: str = attr.ib(repr=_secrepr)
    access_token: str = attr.ib(repr=False)
    host: str = attr.ib()


class TestPopulateKeeperFromTargetDtoPool:
    def test_target_dto_secrets_added_to_keeper(self) -> None:
        rci = RequestContextInfo(secret_keeper=SecretKeeper())
        pool = [_StubTargetDTO(password="pw-XXXX", access_token="tok-XXXX", host="h")]
        _populate_keeper_from_target_dto_pool(rci, pool)
        secrets = rci.secret_keeper.secrets
        assert "pw-XXXX" in secrets
        assert "tok-XXXX" in secrets
        assert "h" not in secrets

    def test_none_rci_no_crash(self) -> None:
        pool = [_StubTargetDTO(password="x", access_token="y", host="h")]
        _populate_keeper_from_target_dto_pool(None, pool)  # must not raise
