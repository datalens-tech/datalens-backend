import threading
import time

import jwt

import dl_core.united_storage_client as united_storage_client


class DynamicUSMasterTokenFactory:
    def __init__(
        self,
        private_key: str,
        token_lifetime_sec: int,
        min_ttl_sec: float,
    ) -> None:
        self._private_key = private_key
        self._token_lifetime_sec = token_lifetime_sec
        self._min_ttl_sec = min_ttl_sec
        self._token: str | None = None
        self._expires_at: float = 0  # monotonic
        self._lock = threading.Lock()

    def _generate_token(self) -> str:
        now = time.time()
        payload = {
            "serviceId": "bi",
            "iat": int(now),
            "exp": int(now) + self._token_lifetime_sec,
        }
        return jwt.encode(payload, self._private_key, algorithm="RS256")

    def _get_or_refresh_token(self) -> str:
        now = time.monotonic()
        if self._token is not None and self._expires_at > now + self._min_ttl_sec:
            return self._token

        with self._lock:
            # Double-check after acquiring lock
            now = time.monotonic()
            if self._token is not None and self._expires_at > now + self._min_ttl_sec:
                return self._token

            self._token = self._generate_token()
            self._expires_at = now + self._token_lifetime_sec
            return self._token

    def get_auth_context(
        self,
        us_master_token: str | None = None,
    ) -> united_storage_client.USAuthContextPrivateOSS:
        token = self._get_or_refresh_token()
        return united_storage_client.USAuthContextPrivateOSS(
            us_dynamic_master_token=token,
            us_master_token=us_master_token,
        )
