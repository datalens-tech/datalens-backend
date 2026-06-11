from collections.abc import Mapping
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

    def add_secret(self, secret: str | None, name: str) -> None:
        if secret is None:
            LOGGER.info("Secret %r is None, skipping", name)
            return
        if len(secret) >= self._min_secret_length:
            self._secrets[secret] = name
        else:
            LOGGER.warning("Secret %r is too short (len=%d), skipping", name, len(secret))

    def add_param(self, param: str | None, name: str) -> None:
        if param is None:
            LOGGER.info("Param %r is None, skipping", name)
            return
        if len(param) >= self._min_param_length:
            self._params[param] = name
        else:
            LOGGER.warning("Param %r is too short (len=%d), skipping", name, len(param))

    def add_secrets(self, secrets: Mapping[str, str | None], prefix: str = "") -> None:
        """secrets: dict[name: secret_value]. If prefix is set, each name is namespaced as f"{prefix}.{name}"."""
        for name, secret in secrets.items():
            self.add_secret(secret, f"{prefix}.{name}" if prefix else name)

    def add_params(self, params: Mapping[str, str | None]) -> None:
        """params: dict[name: param_value]; None values are skipped (logged at INFO)."""
        for name, param in params.items():
            self.add_param(param, name)

    @property
    def secrets(self) -> dict[str, str]:
        return self._secrets

    @property
    def params(self) -> dict[str, str]:
        return self._params

    def clear(self) -> None:
        self._secrets.clear()
        self._params.clear()
