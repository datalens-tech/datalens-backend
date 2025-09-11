from cryptography import fernet

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.services_registry.top_level import (
    DummyServiceRegistry,
    ServicesRegistry,
)
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager import USManagerBase
import dl_retrier


class DummyUSManager(USManagerBase):
    def __init__(
        self,
        bi_context: RequestContextInfo = RequestContextInfo.create_empty(),  # noqa: B008
        services_registry: ServicesRegistry = DummyServiceRegistry(rci=RequestContextInfo.create_empty()),  # noqa: B008
    ):
        super().__init__(
            bi_context=bi_context,
            us_base_url="http://localhost:66000",
            us_api_prefix="dummy",
            crypto_keys_config=CryptoKeysConfig(  # type: ignore  # TODO: fix
                map_id_key={"dummy_usm_key": fernet.Fernet.generate_key().decode("ascii")},
                actual_key_id="dummy_usm_key",
            ),
            us_auth_context=USAuthContextMaster("FakeKey"),
            services_registry=services_registry,
            retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
        )
