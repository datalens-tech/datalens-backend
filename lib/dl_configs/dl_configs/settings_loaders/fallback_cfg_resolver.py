from collections.abc import Mapping
import logging
from typing import (
    Any,
    ClassVar,
    Iterator,
    Optional,
)

import attr
import yaml

from dl_configs.settings_loaders.common import SDict


LOGGER = logging.getLogger(__name__)


@attr.s
class FallbackConfigResolver:
    def resolve(self, s_dict: SDict) -> Any:
        raise NotImplementedError()


@attr.s
class ObjectLikeConfig(Mapping):
    _data: dict = attr.ib()
    _path: list = attr.ib(default=[])

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def _get_key(self, key: Any) -> Any:
        if key not in self._data:
            path = ".".join(self._path + [key])
            raise AttributeError(f'There is no record in config by path: "{path}"')
        return self._data[key]

    def __getattr__(self, key: Any) -> Any:
        return self._get_key(key)

    def get(self, key: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return self._data.get(key)

    def __getitem__(self, key: Any) -> Any:
        return self._get_key(key)

    @classmethod
    def from_dict(cls, data: dict, path: Optional[list] = None) -> "ObjectLikeConfig":
        if path is None:
            path = []

        def _get_value_for_cfg(k: Any, v: Any) -> Any:
            if isinstance(v, dict):
                return cls.from_dict(v, path + [k])
            if isinstance(v, (list, tuple)):
                return [
                    cls.from_dict(item, path + [k] + [str(idx)]) if isinstance(item, dict) else item
                    for idx, item in enumerate(v)
                ]
            return v

        ret = cls(
            data={k: _get_value_for_cfg(k, v) for k, v in data.items()},
            path=path,
        )
        return ret

    def to_dict(self) -> dict[str, Any]:
        def _get_value_for_dict(v) -> Any:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            if isinstance(v, ObjectLikeConfig):
                return v.to_dict()
            if isinstance(v, (list, tuple)):
                return [item.to_dict if isinstance(item, ObjectLikeConfig) else item for item in v]
            return v

        return {k: _get_value_for_dict(v) for k, v in self._data.items()}


@attr.s
class YamlFileConfigResolver(FallbackConfigResolver):
    config_path_key: ClassVar[str] = "CONFIG_PATH"

    @classmethod
    def _get_config_path(cls, s_dict: SDict) -> Optional[str]:
        path = s_dict.get(cls.config_path_key)
        LOGGER.info('Config path is "%s"', path)
        return path

    @classmethod
    def is_config_enabled(cls, s_dict: SDict) -> bool:
        result = cls._get_config_path(s_dict) is not None
        LOGGER.info('Check path key in s_dict: "%s"', result)
        return result

    def resolve(self, s_dict: SDict) -> ObjectLikeConfig:
        LOGGER.info("Resolve by YamlFileConfigResolver")
        path = self._get_config_path(s_dict)
        with open(path) as f:  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "open" has incompatible type "str | None"; expected "int | str | bytes | PathLike[str] | PathLike[bytes]"  [arg-type]
            config = yaml.safe_load(f.read())
        return ObjectLikeConfig.from_dict(config)


@attr.s
class ConstantFallbackConfigResolver(FallbackConfigResolver):
    config: Any = attr.ib()

    def resolve(self, s_dict: SDict) -> Any:
        return self.config
