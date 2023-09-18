from typing import ClassVar

import attr

from dl_testing.env_params.getter import EnvParamGetter


@attr.s
class TvmSecretReader:
    KEY_CLIENT_SECRET: ClassVar[str] = "TVM_CLIENT_SECRET"

    _env_param_getter: EnvParamGetter = attr.ib()

    def get_client_secret(self) -> str:
        return self._env_param_getter.get_str_value(self.KEY_CLIENT_SECRET)

    def get_tvm_info(self) -> tuple[str, str, str]:
        return "int-dev", "2021163", self.get_client_secret()

    def get_tvm_info_str(self) -> str:
        return " ".join(self.get_tvm_info())
