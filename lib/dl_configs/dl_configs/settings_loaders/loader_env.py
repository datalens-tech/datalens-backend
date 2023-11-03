from __future__ import annotations

from functools import reduce
import inspect
import json
import logging
import os
from types import MappingProxyType
import typing
from typing import (
    Any,
    Callable,
    ClassVar,
    Collection,
    Optional,
    Type,
    TypeVar,
    Union,
    final,
    no_type_check,
)

import attr
import typeguard

from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
    SettingsFallbackType,
)
from dl_configs.settings_loaders.common import (
    FallbackFactory,
    SDict,
)
from dl_configs.settings_loaders.connectors_settings import generate_connectors_settings_class
from dl_configs.settings_loaders.env_remap import remap_env
from dl_configs.settings_loaders.exc import SettingsLoadingException
from dl_configs.settings_loaders.fallback_cfg_resolver import (
    FallbackConfigResolver,
    YamlFileConfigResolver,
)
from dl_configs.settings_loaders.meta_definition import (
    RequiredValue,
    ReservedKeys,
    SMeta,
    s_attrib,
)
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_constants.enums import ConnectionType
from dl_utils.utils import get_type_full_name


_SETTINGS_TV = TypeVar("_SETTINGS_TV")

SEP = "_"


LOGGER = logging.getLogger(__name__)


class InvalidConfigValueException(SettingsLoadingException):
    pass


class ConfigFieldMissing(InvalidConfigValueException):
    def __init__(self, field_set: set[str]):
        super().__init__(field_set)
        self._field_set = field_set

    @property
    def field_set(self) -> set[str]:
        return self._field_set


def get_sub_keys(prefix: str, env: SDict):
    effective_prefix = prefix if prefix.endswith(SEP) else prefix + SEP
    return {key[len(effective_prefix) :]: value for key, value in env.items() if key.startswith(effective_prefix)}


NOT_SET = object()


@attr.s
class SDictExtractor:
    expected_type: Type = attr.ib()
    key: Optional[str] = attr.ib()
    # TODO FIX: Validate default on creation time
    default: Any = attr.ib()

    def _get_scoped_s_dict(self, s_dict: SDict) -> SDict:
        base_key = self.key
        return s_dict if base_key is None else get_sub_keys(base_key.upper(), s_dict)

    def has_field(self, s_dict: SDict) -> bool:
        raise NotImplementedError()

    def _extract(self, s_dict: SDict) -> Any:
        raise NotImplementedError()

    @final
    def extract(self, s_dict: SDict) -> Any:
        ret = self._extract(s_dict)
        typeguard.check_type(self.key or "root", ret, self.expected_type)
        return ret


@attr.s
class ScalarExtractor(SDictExtractor):
    converter: Optional[Callable[[str], Any]] = attr.ib()

    def has_field(self, s_dict: SDict) -> bool:
        return self.key in s_dict

    def _extract(self, s_dict: SDict) -> Any:
        if self.key not in s_dict:
            if self.default is not NOT_SET:
                return self.default

            return None

        value = s_dict[self.key]

        if self.converter is None:
            return value

        try:
            return self.converter(value)
        # TODO FIX: take in account potential sensivity of value
        except Exception as exc:
            raise InvalidConfigValueException(f"Could not convert value {self.key}: {self.expected_type}") from exc


@attr.s
class DefaultOnlyExtractor(SDictExtractor):
    def __attrs_post_init__(self):
        assert self.default is not NOT_SET

    def has_field(self, s_dict: SDict) -> bool:
        return False

    def _extract(self, s_dict: SDict) -> Any:
        return self.default


@attr.s
class DictExtractor(SDictExtractor):
    value_converter: Optional[Callable[[str], Any]] = attr.ib()

    def has_field(self, s_dict: SDict) -> bool:
        return len(self._get_scoped_s_dict(s_dict)) > 0

    def _extract(self, s_dict: SDict) -> Any:
        value_converter = self.value_converter
        return {
            key: value_converter(value) if value_converter is not None else value
            for key, value in self._get_scoped_s_dict(s_dict).items()
        }


@attr.s
class CompositeExtractor(SDictExtractor):
    map_field_name_subextractor: dict[str, SDictExtractor] = attr.ib()
    map_field_name_field_override: dict[str, Any] = attr.ib()
    composition_factory: Callable = attr.ib()
    json_value_converter: Optional[Callable[[Any], Any]] = attr.ib(default=None)
    is_root: bool = attr.ib(default=False)
    field_enables_flag_extractor: Optional[ScalarExtractor] = attr.ib(default=None)

    def should_ignore(self, s_dict) -> bool:
        scoped_s_dict = self._get_scoped_s_dict(s_dict)
        field_enables_flag_extractor = self.field_enables_flag_extractor

        if field_enables_flag_extractor is None:
            return False

        if self.field_enables_flag_extractor.has_field(scoped_s_dict):
            should_read = self.field_enables_flag_extractor.extract(scoped_s_dict)
            return not bool(should_read)

        return False

    def has_field(self, s_dict: SDict) -> bool:
        if len(self.map_field_name_field_override):
            return True

        scoped_s_dict = self._get_scoped_s_dict(s_dict)
        if ReservedKeys.JSON_VALUE in scoped_s_dict:
            return True
        all_extractors = list(self.map_field_name_subextractor.values())
        if self.field_enables_flag_extractor is not None:
            all_extractors.append(self.field_enables_flag_extractor)

        return any(extractor.has_field(scoped_s_dict) for extractor in all_extractors)

    def run_composition_factory_with_validation(self, fields: dict[str, Any]) -> Any:
        sig = inspect.signature(self.composition_factory)
        required_params = {
            p_name for p_name, p_obj in sig.parameters.items() if p_obj.default is inspect.Parameter.empty
        }
        missing_params = required_params - set(fields.keys())
        if missing_params:
            raise ConfigFieldMissing(missing_params)

        return self.composition_factory(**fields)

    @staticmethod
    def _ensure_no_missing_fields(
        obj: Any = None, map_field_name_missing_exc: Optional[dict[str, ConfigFieldMissing]] = None
    ) -> None:
        missing_required_fields: list[attr.Attribute] = []
        missing_sub_field_code_set: set[str] = set()

        def handle_field_sub_exc(faulty_field_name: str, exc: ConfigFieldMissing):
            missing_sub_field_code_set.update(f"{faulty_field_name}.{sub_field}" for sub_field in exc.field_set)

        if obj is None:
            if map_field_name_missing_exc is not None:
                for field_name, sub_exc in map_field_name_missing_exc.items():
                    handle_field_sub_exc(field_name, sub_exc)
        else:
            for field in attr.fields(type(obj)):
                field_value = getattr(obj, field.name)
                if isinstance(field_value, RequiredValue):
                    missing_required_fields.append(field)

                if map_field_name_missing_exc is not None and field.name in map_field_name_missing_exc:
                    handle_field_sub_exc(field.name, map_field_name_missing_exc[field.name])

        if missing_required_fields or missing_sub_field_code_set:
            raise ConfigFieldMissing(
                field_set=set(field.name for field in missing_required_fields) | missing_sub_field_code_set
            )

    def _extract_from_json(self, s_dict: SDict) -> Any:
        converter = self.json_value_converter
        if converter is None:
            raise InvalidConfigValueException(f"JSON converter is not defined for field {self.key!r}")

        json_str = s_dict[ReservedKeys.JSON_VALUE]

        illegal_fields = s_dict.keys() - {ReservedKeys.JSON_VALUE}

        # It is ok if field enabler in env
        if self.field_enables_flag_extractor is not None:
            illegal_fields = illegal_fields - {self.field_enables_flag_extractor.key}

        if illegal_fields:
            raise InvalidConfigValueException(
                f"{ReservedKeys.JSON_VALUE} can not be combined with any other keys: {' '.join(sorted(illegal_fields))}"
            )

        try:
            json_value = json.loads(json_str)
        except json.JSONDecodeError:
            # TODO FIX: take in account potential sensivity of value
            raise InvalidConfigValueException(f"Malformed JSON for {self.key!r}")

        try:
            return converter(json_value)
        except Exception as exc:  # noqa
            # TODO FIX: take in account potential sensivity of value
            raise InvalidConfigValueException(f"Can not convert JSON value to target object {self.key!r}")

    def _extract(self, s_dict: SDict) -> Any:
        default_value = self.default
        scoped_s_dict = self._get_scoped_s_dict(s_dict)

        if not self.is_root:
            if self.should_ignore(s_dict):
                return None

            if ReservedKeys.JSON_VALUE in scoped_s_dict:
                return self._extract_from_json(scoped_s_dict)

        fields = {}
        map_field_name_missing_fields: dict[str, ConfigFieldMissing] = {}

        for field_name, sub_extractor in self.map_field_name_subextractor.items():
            if field_name in self.map_field_name_field_override:
                fields[field_name] = self.map_field_name_field_override[field_name]
            elif sub_extractor.has_field(scoped_s_dict) or sub_extractor.default is not NOT_SET:
                try:
                    fields[field_name] = sub_extractor.extract(scoped_s_dict)
                except ConfigFieldMissing as child_field_missing:
                    map_field_name_missing_fields[field_name] = child_field_missing

        if default_value is None and not self.has_field(s_dict):
            if map_field_name_missing_fields:
                self._ensure_no_missing_fields(map_field_name_missing_exc=map_field_name_missing_fields)
            return None

        elif default_value is NOT_SET or default_value is None:
            if map_field_name_missing_fields:
                self._ensure_no_missing_fields(map_field_name_missing_exc=map_field_name_missing_fields)
            return self.run_composition_factory_with_validation(fields)

        else:
            updated_default_value = attr.evolve(self.default, **fields)
            self._ensure_no_missing_fields(
                obj=updated_default_value, map_field_name_missing_exc=map_field_name_missing_fields
            )
            return updated_default_value


@attr.s
class EnvSettingsLoader:
    file_remap_prefix: ClassVar[str] = "BIE_FILE_MAP"

    _s_dict: Optional[SDict] = attr.ib(repr=False)

    def __attrs_post_init__(self):
        if self._s_dict is None:
            self._s_dict = MappingProxyType(dict(os.environ))
        else:
            self._s_dict = MappingProxyType(dict(self._s_dict))

    @property
    def s_dict(self) -> SDict:
        assert self._s_dict is not None
        return self._s_dict

    def load_file_mapped_keys(self, s_dict: SDict) -> SDict:
        map_key_file_path = get_sub_keys(self.file_remap_prefix, s_dict)
        loaded_keys: dict[str, str] = {}

        for key, file_path in map_key_file_path.items():
            with open(file_path, "r", encoding="ascii") as file:
                loaded_keys[key] = file.read()

        return loaded_keys

    @classmethod
    def build_default_extractor(
        cls,
        name: str,
        field_type: Type,
        meta: SMeta,
        default: Any,
    ) -> Union[ScalarExtractor, DictExtractor]:
        env_var_name = meta.name.upper()
        unwrapped_type_set = cls.unwrap_union(field_type, ignore_none=True)
        assert len(unwrapped_type_set) == 1
        effective_type = next(iter(unwrapped_type_set))

        simple_type_converters: dict[Type, Callable[[str], Any]] = {
            int: int,
            float: float,
            str: None,
            bool: lambda x: {"1": True, "0": False, "true": True, "false": False}[x.lower()],
        }
        if meta.env_var_converter is not None:
            return ScalarExtractor(
                expected_type=field_type,
                key=env_var_name,
                converter=meta.env_var_converter,
                default=default,
            )
        elif effective_type in simple_type_converters:
            return ScalarExtractor(
                expected_type=field_type,
                key=env_var_name,
                converter=simple_type_converters[effective_type],
                default=default,
            )
        elif typing.get_origin(effective_type) == dict:
            key_type, value_type = typing.get_args(effective_type)
            assert key_type == str
            value_converter: Optional[Callable[[str], Any]]

            if value_type in simple_type_converters:
                value_converter = simple_type_converters[value_type]
            else:
                raise TypeError(f"Unsupprted map value type: {field_type!r}")

            return DictExtractor(
                expected_type=field_type,
                key=env_var_name,
                value_converter=value_converter,
                default=default,
            )
        else:
            raise TypeError(f"Unsupported field type: {field_type!r} while extracting {env_var_name}")

    @staticmethod
    def unwrap_union(the_type: Type, ignore_none: bool) -> frozenset[Type]:
        if typing.get_origin(the_type) == Union:
            unwrapped_types = typing.get_args(the_type)
            if ignore_none:
                # TODO FIX: try to not use noqa: E721
                # (to consider: `not issubclass(arg_type, type(None))`
                unwrapped_types = [argtype for argtype in unwrapped_types if argtype is not type(None)]  # noqa: E721
            return frozenset(unwrapped_types)
        return frozenset([the_type])

    @classmethod
    def is_sub_settings_field(cls, attrib: attr.Attribute) -> bool:
        the_type = attrib.type
        possible_types = cls.unwrap_union(the_type, ignore_none=True)
        map_type_is_sub_setting: dict[Type, bool] = {
            t: isinstance(t, type) and issubclass(t, SettingsBase) for t in possible_types
        }
        if any(map_type_is_sub_setting.values()):
            assert all(
                map_type_is_sub_setting.values()
            ), f"Divergence in {attrib!r} type classification: {map_type_is_sub_setting!r}"
            return True
        return False

    @classmethod
    def build_composite_extractor(
        cls,
        settings_type: Type[SettingsBase],
        key_prefix: tuple[str, ...],
        default: Any = NOT_SET,
        fallback_cfg: Any = None,
        app_cfg_type: Any = None,
        is_root: bool = False,
        field_enables_flag_extractor: Optional[ScalarExtractor] = None,
        json_converter: Optional[Callable[[Any], Any]] = None,
        field_overrides: Optional[dict[str, Any]] = None,
    ) -> CompositeExtractor:
        if is_root:
            assert field_enables_flag_extractor is None
        else:
            assert app_cfg_type is None, (
                "App type passing is not allowed for non-root composite extractor for type"
                f" ({get_type_full_name(settings_type)})"
            )
            assert field_overrides is None, "Field overrides are supported only for root extractors"

        effective_fields_overrides: dict[str, Any] = {}
        if field_overrides is not None:
            effective_fields_overrides.update(**field_overrides)

        possible_types_set = cls.unwrap_union(settings_type, ignore_none=True)
        assert len(possible_types_set) == 1, "Polymorphism is not supported at this moment"

        for possible_type in possible_types_set:
            if attr.has(possible_type):
                attr.resolve_types(possible_type)

        field_actual_type = next(iter(possible_types_set))

        map_field_name_field = attr.fields_dict(field_actual_type)
        extractors_map: dict[str, SDictExtractor] = {}

        for field_name, field in map_field_name_field.items():
            field_s_meta = SMeta.from_attrib(field)
            if field_s_meta is None:
                continue

            # Adding app type to fields override if was provided in args
            # Extractor will be also created (but not used during extraction)
            if field_s_meta.is_app_cfg_type and app_cfg_type is not None:
                effective_fields_overrides[field_name] = app_cfg_type

            # Determining field default
            field_default: Any

            if field_s_meta.fallback_cfg_key is not None:
                assert field_s_meta.fallback_factory is None
                field_default = getattr(fallback_cfg, field_s_meta.fallback_cfg_key, NOT_SET)

            elif field_s_meta.fallback_factory is not None:
                assert field_s_meta.fallback_cfg_key is None
                # It's a fuse to prevent unexpected behaviour in case of
                #  fallbacks defined in both nested class and root class.
                # If become critical to have fallback factory in nested set
                #  try to rewrite this check to prevent overlapping fallback factories
                assert is_root, f"Fallback factories may be applied only on top-level configs, not on {settings_type}"

                fallback_factory: FallbackFactory

                if isinstance(field_s_meta.fallback_factory, staticmethod):
                    fallback_factory = field_s_meta.fallback_factory.__func__
                else:
                    fallback_factory = field_s_meta.fallback_factory

                assert inspect.isfunction(fallback_factory) or inspect.ismethod(fallback_factory)
                factory_signature = inspect.signature(fallback_factory)

                if len(factory_signature.parameters) == 0:
                    field_default = fallback_factory()
                elif len(factory_signature.parameters) == 1:
                    field_default = fallback_factory(fallback_cfg)
                elif len(factory_signature.parameters) == 2:
                    field_default = fallback_factory(fallback_cfg, app_cfg_type)
                else:
                    raise SettingsLoadingException(f"Unexpected signature of fallback factory for {field}")

            else:
                field_default = getattr(fallback_cfg, field_s_meta.name, NOT_SET)
                if (
                    field_default is NOT_SET
                    and cls.is_sub_settings_field(field)
                    and default is not None
                    and default is not NOT_SET
                ):
                    field_default = getattr(default, field_name)

            child_extractor: SDictExtractor

            if field_s_meta.name is None:
                child_extractor = DefaultOnlyExtractor(
                    key=None,
                    default=field_default,
                    expected_type=field.type,
                )
            elif cls.is_sub_settings_field(field):
                assert field_s_meta.fallback_cfg_key is None, "At this moment nested objects can not have defaults"
                enabled_key_name = field_s_meta.enabled_key_name

                child_extractor = cls.build_composite_extractor(
                    settings_type=field.type,
                    key_prefix=key_prefix + (field_s_meta.name,),
                    default=field_default,
                    json_converter=field_s_meta.json_converter,
                    field_enables_flag_extractor=None
                    if enabled_key_name is None
                    else cls.build_default_extractor(
                        name="enabled",
                        field_type=bool,
                        meta=SMeta(enabled_key_name),
                        default=NOT_SET,
                    ),
                )
            else:
                assert field_s_meta.enabled_key_name is None, (
                    f"enabled_key_name is supported only for nested configs:"
                    f" {get_type_full_name(settings_type)}.{field_name}"
                )
                child_extractor = cls.build_default_extractor(
                    name=field_name,
                    field_type=field.type,
                    meta=field_s_meta,
                    default=field_default,
                )

            extractors_map[field_name] = child_extractor

        return CompositeExtractor(
            key=key_prefix[-1] if key_prefix else None,
            expected_type=settings_type,
            composition_factory=field_actual_type,
            map_field_name_subextractor=extractors_map,
            default=default,
            is_root=is_root,
            json_value_converter=json_converter,
            field_enables_flag_extractor=field_enables_flag_extractor,
            map_field_name_field_override=effective_fields_overrides,
        )

    @classmethod
    def get_app_cfg_type_field(cls, settings_type: Type[SettingsBase]) -> Optional[attr.Attribute]:
        candidates = [field for field in attr.fields(settings_type) if SMeta.from_attrib(field).is_app_cfg_type]
        if len(candidates) > 1:
            raise SettingsLoadingException(
                f"Settings class {get_type_full_name(settings_type)}"
                f" has more than one app_type attributes: {candidates}"
            )
        else:
            return next(iter(candidates)) if candidates else None

    @classmethod
    def get_app_cfg_type_value_from_env(cls, settings_type: Type[SettingsBase], s_dict: SDict) -> Any:
        app_cfg_type_field = cls.get_app_cfg_type_field(settings_type)
        if app_cfg_type_field is None:
            return None

        extractor = cls.build_default_extractor(
            name=app_cfg_type_field.name,
            field_type=app_cfg_type_field.type,
            meta=SMeta.from_attrib(app_cfg_type_field),
            default=NOT_SET,
        )
        return extractor.extract(s_dict)

    @classmethod
    def build_top_level_extractor(
        cls,
        settings_type: Type[_SETTINGS_TV],
        key_prefix: Optional[str] = None,
        fallback_cfg: Any = None,
        app_cfg_type: Any = None,
        default_value: Optional[_SETTINGS_TV] = None,
        field_overrides: Optional[dict[str, Any]] = None,
    ) -> CompositeExtractor:
        return cls.build_composite_extractor(
            settings_type=settings_type,
            key_prefix=() if key_prefix is None else (key_prefix,),
            fallback_cfg=fallback_cfg,
            app_cfg_type=app_cfg_type,
            is_root=True,
            default=NOT_SET if default_value is None else default_value,
            field_overrides=field_overrides,
        )

    def load_settings(
        self,
        settings_type: Type[_SETTINGS_TV],
        key_prefix: Optional[str] = None,
        fallback_cfg_resolver: Optional[FallbackConfigResolver] = None,
        default_value: Optional[_SETTINGS_TV] = None,
    ) -> _SETTINGS_TV:
        effective_s_dict = self.s_dict
        effective_s_dict = remap_env(effective_s_dict)

        # TODO FIX: Error handling
        keys_from_files = self.load_file_mapped_keys(self.s_dict)

        keys_from_files_intersection = effective_s_dict.keys() & keys_from_files.keys()

        if keys_from_files_intersection:
            raise InvalidConfigValueException(
                f"Got intersection of explicit env vars & file mapped vars:"
                f" {' '.join(sorted(keys_from_files_intersection))}"
            )

        effective_s_dict = {**effective_s_dict, **keys_from_files}

        fallback_cfg: Any = None

        if fallback_cfg_resolver is not None:
            fallback_cfg = fallback_cfg_resolver.resolve(effective_s_dict)

        app_cfg_type = self.get_app_cfg_type_value_from_env(settings_type, effective_s_dict)

        extractor = self.build_top_level_extractor(
            settings_type=settings_type,
            key_prefix=key_prefix,
            fallback_cfg=fallback_cfg,
            app_cfg_type=app_cfg_type,
            default_value=default_value,
        )

        return extractor.extract(effective_s_dict)


def load_settings_from_env_with_fallback_legacy(
    settings_type: Type[_SETTINGS_TV],
    fallback_cfg_resolver: FallbackConfigResolver,
    env: Optional[SDict] = None,
) -> _SETTINGS_TV:
    effective_env = os.environ if env is None else env

    return EnvSettingsLoader(effective_env).load_settings(
        settings_type,
        fallback_cfg_resolver=fallback_cfg_resolver,
    )


def load_settings_from_env_with_fallback(
    settings_type: Type[_SETTINGS_TV],
    env: Optional[SDict] = None,
    default_fallback_cfg_resolver: Optional[FallbackConfigResolver] = None,
) -> _SETTINGS_TV:
    effective_env = os.environ if env is None else env

    if YamlFileConfigResolver.is_config_enabled(s_dict=effective_env):
        default_fallback_cfg_resolver = YamlFileConfigResolver()

    if default_fallback_cfg_resolver is None:
        LOGGER.warning("No fallback resolver")

    return EnvSettingsLoader(effective_env).load_settings(
        settings_type,
        fallback_cfg_resolver=default_fallback_cfg_resolver,
    )


@no_type_check  # mypy is barely working with dynamic attrs classes
def load_connectors_settings_from_env_with_fallback(
    settings_registry: dict[ConnectionType, Type[ConnectorSettingsBase]],
    fallbacks: dict[ConnectionType, SettingsFallbackType],
    whitelist: Optional[Collection[ConnectionType]] = None,
    env: Optional[SDict] = None,
    fallback_cfg_resolver: Optional[FallbackConfigResolver] = None,
) -> dict[ConnectionType, ConnectorSettingsBase]:
    settings_class = generate_connectors_settings_class(settings_registry, whitelist)

    def connectors_fallback(full_cfg: ConnectorsConfigType):
        full_settings = reduce(lambda settings, fallback: settings | fallback(full_cfg), fallbacks.values(), {})
        return settings_class(**full_settings)

    connectors = attr.make_class(
        "Connectors",
        {
            "CONNECTORS": s_attrib(
                "CONNECTORS",
                type_class=Optional[settings_class],
                fallback_factory=connectors_fallback,
            )
        },
        frozen=True,
    )

    loaded_settings = load_settings_from_env_with_fallback(connectors, env, fallback_cfg_resolver)
    connectors_settings: dict[ConnectionType, ConnectorSettingsBase] = {}
    if loaded_settings.CONNECTORS is None:
        return connectors_settings

    for name in attr.fields_dict(settings_class):
        conn_type = ConnectionType(name.lower())
        connector_settings = getattr(loaded_settings.CONNECTORS, name)
        if connector_settings is not None:
            assert isinstance(connector_settings, ConnectorSettingsBase)
            connectors_settings[conn_type] = connector_settings
    return connectors_settings
