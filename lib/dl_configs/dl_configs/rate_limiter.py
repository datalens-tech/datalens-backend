from __future__ import annotations

import typing

import attr
import typing_extensions

from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_configs.utils import (
    tuple_of_models_env_converter_factory,
    tuple_of_models_json_converter_factory,
)


@attr.s(frozen=True)
class RequestEventKeyTemplate(SettingsBase):
    key: str = s_attrib("KEY")  # type: ignore
    headers: tuple[str, ...] = s_attrib("HEADERS", missing_factory=tuple)  # type: ignore

    @classmethod
    def from_json(cls, json_value: typing.Any) -> typing_extensions.Self:
        assert isinstance(json_value, typing.Mapping)
        return cls(  # type: ignore
            key=json_value["KEY"],
            headers=tuple(json_value["HEADERS"]),
        )


@attr.s(frozen=True)
class RateLimiterPattern(SettingsBase):
    url_regex: str = s_attrib("URL_REGEX")  # type: ignore
    methods: tuple[str, ...] = s_attrib("METHODS", missing_factory=tuple)  # type: ignore
    event_key_template: RequestEventKeyTemplate = s_attrib("EVENT_KEY_TEMPLATE")  # type: ignore
    limit: int = s_attrib("LIMIT")  # type: ignore
    window_ms: int = s_attrib("WINDOW_MS")  # type: ignore

    @classmethod
    def from_json(cls, json_value: typing.Any) -> typing_extensions.Self:
        assert isinstance(json_value, typing.Mapping)
        return cls(  # type: ignore
            url_regex=json_value["URL_REGEX"],
            methods=tuple(json_value["METHODS"]),
            event_key_template=RequestEventKeyTemplate.from_json(json_value["EVENT_KEY_TEMPLATE"]),
            limit=int(json_value["LIMIT"]),
            window_ms=int(json_value["WINDOW_MS"]),
        )


@attr.s(frozen=True)
class RateLimiterConfig(SettingsBase):
    PATTERNS: tuple[RateLimiterPattern, ...] = s_attrib(  # type: ignore
        "PATTERNS",
        env_var_converter=tuple_of_models_env_converter_factory(RateLimiterPattern),
        json_converter=tuple_of_models_json_converter_factory(RateLimiterPattern),
        missing_factory=tuple,
    )

    @classmethod
    def from_json(cls, json_value: typing.Any) -> typing.Optional[typing_extensions.Self]:
        if json_value is None:
            return None

        assert isinstance(json_value, typing.Mapping)
        return cls(  # type: ignore
            PATTERNS=(RateLimiterPattern.from_json(entry) for entry in json_value["PATTERNS"]),
        )
