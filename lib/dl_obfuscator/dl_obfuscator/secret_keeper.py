import attr


@attr.s
class SecretKeeper:
    """Request-scoped registry for storing secrets and query parameters"""

    _secrets: dict[str, str] = attr.ib(factory=dict, repr=False)
    _params: dict[str, str] = attr.ib(factory=dict, repr=False)

    def add_secret(self, secret: str, name: str | None = None) -> None:
        self._secrets[secret] = name or "hidden"

    def add_param(self, param: str, name: str | None = None) -> None:
        self._params[param] = name or "hidden"

    @property
    def secrets(self) -> dict[str, str]:
        return self._secrets

    @property
    def params(self) -> dict[str, str]:
        return self._params

    def clear(self) -> None:
        self._secrets.clear()
        self._params.clear()
