import attr

from bi_dls_client.dls_client import DLSClient
from bi_api_commons.base_models import RequestContextInfo


@attr.s
class DLSClientFactory:
    _host: str = attr.ib()
    _api_key: str = attr.ib()

    def get_client(self, rci: RequestContextInfo) -> DLSClient:
        return DLSClient(
            host=self._host,
            secret_api_key=self._api_key,
            rci=rci,
        )
