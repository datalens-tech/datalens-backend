import dl_auth
import dl_core.united_storage_client as united_storage_client


class DynamicUSMasterTokenFactory:
    def __init__(
        self,
        private_key: str,
        token_lifetime_sec: int,
        min_ttl_sec: float,
    ) -> None:
        self._generator = dl_auth.DynamicMasterTokenGenerator(
            private_key=private_key,
            token_lifetime_sec=token_lifetime_sec,
            min_ttl_sec=min_ttl_sec,
        )

    def get_auth_context(
        self,
        us_master_token: str | None = None,
    ) -> united_storage_client.USAuthContextPrivateOSS:
        token = self._generator.get_token()
        return united_storage_client.USAuthContextPrivateOSS(
            us_dynamic_master_token=token,
            us_master_token=us_master_token,
        )
