from typing import (
    ClassVar,
    Mapping,
    Optional,
    Sequence,
)

import attr

from dl_testing.env_params.getter import EnvParamGetter
from dl_utils.entrypoints import EntrypointClassManager


@attr.s
class EnvParamGetterLoader:
    _getters: dict[str, EnvParamGetter] = attr.ib(init=False, factory=dict)
    _ep_mgr: EntrypointClassManager = attr.ib(init=False)

    EP_NAME: ClassVar[str] = "env_param_getters"

    @_ep_mgr.default
    def _make_ep_mgr(self) -> EntrypointClassManager:
        return EntrypointClassManager(entrypoint_group_name=self.EP_NAME)

    def get_getter(self, name: str) -> EnvParamGetter:
        try:
            return self._getters[name]
        except KeyError:
            if name.startswith("$"):
                self._auto_add_getter(name)
                return self.get_getter(name)
            raise

    def has_getter(self, name: str) -> bool:
        return name in self._getters

    def _auto_add_getter(self, name: str) -> None:
        assert name.startswith("$")
        getter_type_name = name[1:]
        getter_cls = self._ep_mgr.get_ep_class(getter_type_name)
        getter = getter_cls()
        getter.initialize(config={})
        self._getters[name] = getter

    def _resolve_setting_item(self, setting: dict, requirement_getter: EnvParamGetter) -> Optional[str]:  # type: ignore  # 2024-01-24 # TODO: Missing return statement  [return]
        if setting["type"] == "value":
            return setting["value"]
        if setting["type"] == "param":
            key = setting["key"]
            return requirement_getter.get_str_value(key)

    def initialize(self, getter_config_list: Sequence[Mapping], requirement_getter: EnvParamGetter) -> None:
        for getter_config in getter_config_list:
            getter_name = getter_config["name"]
            getter_type_name = getter_config["type"]
            getter_cls = self._ep_mgr.get_ep_class(getter_type_name)
            getter: EnvParamGetter = getter_cls()

            raw_getter_settings = getter_config["settings"]
            resolved_getter_settings: dict[str, str] = {}
            assert isinstance(raw_getter_settings, dict)
            for setting_name, raw_setting in raw_getter_settings.items():
                assert isinstance(raw_setting, dict)
                setting_value = self._resolve_setting_item(raw_setting, requirement_getter=requirement_getter)
                resolved_getter_settings[setting_name] = setting_value  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "str | None", target has type "str")  [assignment]

            getter.initialize(config=resolved_getter_settings)
            self._getters[getter_name] = getter
