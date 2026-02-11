import logging

import attr


LOGGER = logging.getLogger(__name__)

MIN_SECRET_LENGTH = 3
MIN_PARAM_LENGTH = 3


@attr.s
class SecretKeeper:
    """Request-scoped registry for storing secrets and query parameters"""

    _secrets: dict[str, str] = attr.ib(factory=dict, repr=False)
    _params: dict[str, str] = attr.ib(factory=dict, repr=False)

    def add_secret(self, secret: str, name: str) -> None:
        if len(secret) >= MIN_SECRET_LENGTH:
            self._secrets[secret] = name
        else:
            LOGGER.warning("Secret %r is too short (len=%d), skipping", name, len(secret))

    def add_param(self, param: str, name: str) -> None:
        if len(param) >= MIN_PARAM_LENGTH:
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
