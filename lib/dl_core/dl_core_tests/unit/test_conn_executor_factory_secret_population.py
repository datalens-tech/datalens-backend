import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.base_models import BaseAttrsDataModel
from dl_core.services_registry.conn_executor_factory import _populate_keeper_from_connection
from dl_obfuscator import SecretKeeper


@attr.s
class _StubData(BaseAttrsDataModel):
    password: str = attr.ib(default="", repr=False)
    host: str = attr.ib(default="")


@attr.s
class _StubConn:
    _data: _StubData | None = attr.ib()

    def has_data(self) -> bool:
        return self._data is not None

    @property
    def data(self) -> _StubData:
        assert self._data is not None
        return self._data


class TestPopulateKeeperFromConnection:
    def test_secret_field_added_to_keeper(self) -> None:
        rci = RequestContextInfo(secret_keeper=SecretKeeper())
        conn = _StubConn(data=_StubData(password="db-pw-XXXX", host="db.example"))
        _populate_keeper_from_connection(rci, conn)
        assert "db-pw-XXXX" in rci.secret_keeper.secrets

    def test_none_rci_no_crash(self) -> None:
        conn = _StubConn(data=_StubData(password="x", host="h"))
        _populate_keeper_from_connection(None, conn)  # must not raise

    def test_none_data_no_crash(self) -> None:
        rci = RequestContextInfo(secret_keeper=SecretKeeper())
        conn = _StubConn(data=None)
        _populate_keeper_from_connection(rci, conn)
        assert rci.secret_keeper.secrets == {}
