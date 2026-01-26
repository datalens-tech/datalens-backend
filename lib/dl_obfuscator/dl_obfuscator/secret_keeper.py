import attr


@attr.s
class SecretKeeper:
    """Request-scoped registry for storing secrets and query parameters"""

    _secrets: dict[str, str] = attr.ib(factory=dict, repr=False)
    _params: dict[str, str] = attr.ib(factory=dict, repr=False)

    def add_secret(self, key: str, value: str) -> None:
        """
        key: The name/identifier of the secret (e.g., "master_token")
        value: The actual secret value to be obfuscated

        Example:
            registry.add_secret("master_token", "abc123def456")
        """
        self._secrets[value] = key

    def add_param(self, key: str, value: str) -> None:
        self._params[value] = key

    def get_secrets(self) -> dict[str, str]:
        return self._secrets

    def get_params(self) -> dict[str, str]:
        return self._params

    def clear(self) -> None:
        self._secrets.clear()
        self._params.clear()
