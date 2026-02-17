import logging

import attr


LOGGER = logging.getLogger(__name__)


@attr.s
class SecretKeeper:
    """Request-scoped registry for storing secrets and query parameters"""

    _secrets: dict[str, str] = attr.ib(factory=dict, repr=False)
    _params: dict[str, str] = attr.ib(factory=dict, repr=False)
    _min_secret_length: int = attr.ib(default=3)
    _min_param_length: int = attr.ib(default=3)

    def add_secret(self, secret: str, name: str) -> None:
        if len(secret) >= self._min_secret_length:
            self._secrets[secret] = name
        else:
            LOGGER.warning("Secret %r is too short (len=%d), skipping", name, len(secret))

    def add_param(self, param: str, name: str) -> None:
        if len(param) >= self._min_param_length:
            self._params[param] = name
        else:
            LOGGER.warning("Param %r is too short (len=%d), skipping", name, len(param))

    @property
    def secrets(self) -> dict[str, str]:
        return self._secrets

    @property
    def params(self) -> dict[str, str]:
        return self._params

    def clear(self) -> None:
        self._secrets.clear()
        self._params.clear()
