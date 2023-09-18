import attr

from bi_testing_ya.secrets import get_secret
from dl_testing.env_params.getter import EnvParamGetter


@attr.s
class YavEnvParamGetter(EnvParamGetter):
    """Gets params from Ya vault"""

    _secret: dict = attr.ib(kw_only=True, factory=dict)

    def get_str_value(self, key: str) -> str:
        return self._secret[key]

    def initialize(self, config: dict) -> None:
        yav_token = config.get("yav_token")
        use_ssh_auth = not yav_token

        if "use_ssh_auth" in config:
            use_ssh_auth = config["use_ssh_auth"] == "1"

        secret_id = config["secret_id"]
        self._secret = get_secret(
            secret_id=secret_id,
            use_ssh_auth=use_ssh_auth,
            yav_token=yav_token,
        )
