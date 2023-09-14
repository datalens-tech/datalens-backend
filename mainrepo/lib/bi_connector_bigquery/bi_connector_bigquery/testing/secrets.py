import abc
from typing import ClassVar

import attr

from bi_testing.env_params.getter import EnvParamGetter


class BigQuerySecretReaderBase(abc.ABC):
    KEY_CONFIG: ClassVar[str] = "BIGQUERY_CONFIG"
    KEY_CREDS: ClassVar[str] = "BIGQUERY_CREDS"

    @property
    @abc.abstractmethod
    def project_config(self) -> dict:
        raise NotImplementedError

    @abc.abstractmethod
    def get_creds(self) -> str:
        raise NotImplementedError

    def get_project_id(self) -> str:
        return self.project_config["project_id"]

    def get_dataset_name(self) -> str:
        return self.project_config["dataset_name"]

    def get_table_name(self) -> str:
        return self.project_config["table_name"]


@attr.s
class BigQuerySecretReader(BigQuerySecretReaderBase):
    _env_param_getter: EnvParamGetter = attr.ib()
    _project_config: dict = attr.ib(init=False)

    @_project_config.default
    def _make_project_config(self) -> dict:
        return self._env_param_getter.get_json_value(self.KEY_CONFIG)

    @property
    def project_config(self) -> dict:
        return self._project_config

    def get_creds(self) -> str:
        return self._env_param_getter.get_str_value(self.KEY_CREDS)
