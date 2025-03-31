from __future__ import annotations

import abc
from copy import deepcopy
import itertools
import logging
from typing import (
    Any,
    Iterator,
    Optional,
)

import attr
from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)

from dl_api_commons.base_models import TenantDef
from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_api_lib.connection_forms.registry import CONN_FORM_FACTORY_BY_TYPE
from dl_api_lib.connection_info import get_connector_info_provider
from dl_api_lib.exc import ConnectorIconNotFoundException
from dl_api_lib.i18n.localizer import Translatable
from dl_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    ConnectorBaseSettings,
    ConnectorContainerSettings,
    ConnectorIconSrc,
    ConnectorIconSrcType,
    ConnectorSettings,
    SectionSettings,
)
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_configs.utils import conn_type_set_env_var_converter
from dl_constants.enums import (
    ConnectionType,
    ConnectorAvailability,
)
from dl_i18n.localizer_base import Localizer


LOGGER = logging.getLogger(__name__)


class LocalizedSerializable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        raise NotImplementedError


def _make_translatable(text: str, domain: Optional[str]) -> Translatable:
    translatable = Translatable(text)
    if domain is not None:
        translatable.domain = domain
    return translatable


@attr.s(kw_only=True)
class Section(LocalizedSerializable):
    title_translatable: Translatable = attr.ib()
    connectors: list[ConnectorBase] = attr.ib(validator=attr.validators.min_len(1))

    @classmethod
    def from_settings(cls, settings: SectionSettings) -> Section:
        def _connector_from_settings(settings: ConnectorBaseSettings | ObjectLikeConfig) -> ConnectorBase:
            if hasattr(settings, "conn_type"):
                return Connector.from_settings(settings)  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "from_settings" of "Connector" has incompatible type "ConnectorBaseSettings | ObjectLikeConfig"; expected "ConnectorSettings"  [arg-type]
            elif hasattr(settings, "includes"):
                return ConnectorContainer.from_settings(settings)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "from_settings" of "ConnectorContainer" has incompatible type "ConnectorBaseSettings"; expected "ConnectorContainerSettings"  [arg-type]
            raise ValueError('Can\'t create a connector, neither "conn_type" nor "includes" found among settings')

        return cls(
            title_translatable=_make_translatable(settings.title_translatable.text, settings.title_translatable.domain),
            connectors=[_connector_from_settings(item) for item in settings.connectors],
        )

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        return dict(
            title=localizer.translate(self.title_translatable),
            connectors=[connector.as_dict(localizer) for connector in self.connectors],
        )


@attr.s(kw_only=True)
class ConnectorBase(LocalizedSerializable, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def hidden(self) -> bool:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def visibility_mode(self) -> ConnectorAvailability:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def alias(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def conn_type_str(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def backend_driven_form(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def get_title(self, localizer: Localizer) -> str:
        raise NotImplementedError

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        return dict(
            hidden=self.hidden,
            visibility_mode=self.visibility_mode.value,
            alias=self.alias,
            conn_type=self.conn_type_str,
            title=self.get_title(localizer),
            backend_driven_form=self.backend_driven_form,
        )


def _conn_type_converter(value: Any) -> ConnectionType:
    return ConnectionType(value) if not isinstance(value, ConnectionType) else value


def _availability_converter(value: Any) -> ConnectorAvailability:
    return ConnectorAvailability(value) if not isinstance(value, ConnectorAvailability) else value


class ConnectorIconRole(DynamicEnum):
    standard = AutoEnumValue()
    nav = AutoEnumValue()


@attr.s(kw_only=True)
class ConnectorIconSrcConfig:
    icon_type: ConnectorIconSrcType = attr.ib()

    def as_dict(self, conn: Connector) -> dict[str, Any] | None:
        return dict(
            type=self.icon_type.value,
            conn_type=conn.conn_type.value,
        )

    @classmethod
    @abc.abstractmethod
    def from_settings(cls, settings: ObjectLikeConfig | ConnectorIconSrc) -> ConnectorIconSrcConfig:
        raise NotImplementedError


@attr.s(kw_only=True)
class ConnectorIconSrcConfigData(ConnectorIconSrcConfig):
    data: Optional[str] = attr.ib(default=None)

    def as_dict(self, conn: Connector) -> dict[str, Any] | None:
        if not conn.connector_info_provider.icon_data_standard or not conn.connector_info_provider.icon_data_nav:
            return None

        data = dict(
            standard="data:image/svg+xml;base64," + conn.connector_info_provider.icon_data_standard,
            nav="data:image/svg+xml;base64," + conn.connector_info_provider.icon_data_nav,
        )
        base_dict = super().as_dict(conn=conn)
        assert base_dict
        return dict(
            **base_dict,
            data=data,
        )

    @classmethod
    def from_settings(cls, settings: ObjectLikeConfig | ConnectorIconSrc) -> ConnectorIconSrcConfigData:
        return cls(
            icon_type=ConnectorIconSrcType.data,
            data=settings.data,
        )


@attr.s(kw_only=True)
class ConnectorIconSrcConfigUrl(ConnectorIconSrcConfig):
    url_prefix: str = attr.ib()

    def as_dict(self, conn: Connector) -> dict[str, Any] | None:
        url = dict(
            standard=f"{self.url_prefix.rstrip('/')}/{ConnectorIconRole.standard.value}/{conn.conn_type.value}.svg",
            nav=f"{self.url_prefix.rstrip('/')}/{ConnectorIconRole.nav.value}/{conn.conn_type.value}.svg",
        )
        base_dict = super().as_dict(conn=conn)
        assert base_dict
        return dict(
            **base_dict,
            url=url,
        )

    @classmethod
    def from_settings(cls, settings: ObjectLikeConfig | ConnectorIconSrc) -> ConnectorIconSrcConfigUrl:
        assert isinstance(
            settings.url_prefix, str
        ), f'Expected a string value in URL config, got "{type(settings.url_prefix)}"'
        return cls(
            icon_type=ConnectorIconSrcType.url,
            url_prefix=settings.url_prefix,
        )


def connector_icon_src_config_factory(icon_data: ConnectorIconSrc | ObjectLikeConfig) -> ConnectorIconSrcConfig:
    icon_type = icon_data.icon_type
    icon_type_str = icon_type.value if isinstance(icon_type, ConnectorIconSrcType) else icon_type

    cfg_class: dict[str, type[ConnectorIconSrcConfig]] = {
        ConnectorIconSrcType.data.value: ConnectorIconSrcConfigData,
        ConnectorIconSrcType.url.value: ConnectorIconSrcConfigUrl,
    }

    return cfg_class[icon_type_str].from_settings(icon_data)


@attr.s(kw_only=True)
class Connector(ConnectorBase):
    """Represents an actual connection type"""

    _hidden: bool = attr.ib(init=False, default=False)
    conn_type: ConnectionType = attr.ib(converter=_conn_type_converter)
    availability: ConnectorAvailability = attr.ib(default=ConnectorAvailability.free, converter=_availability_converter)

    @classmethod
    def from_settings(cls, settings: ConnectorSettings) -> Connector:
        return cls(
            conn_type=ConnectionType(settings.conn_type),
            availability=settings.availability,
        )

    @property
    def hidden(self) -> bool:
        return self._hidden

    @property
    def visibility_mode(self) -> ConnectorAvailability:
        return self.availability

    @property
    def alias(self) -> str:
        return get_connector_info_provider(self.conn_type).alias or self.conn_type_str

    @property
    def connector_info_provider(self) -> ConnectionInfoProvider:
        return get_connector_info_provider(self.conn_type)

    @property
    def conn_type_str(self) -> str:
        return self.conn_type.name

    @property
    def backend_driven_form(self) -> bool:
        return self.conn_type in CONN_FORM_FACTORY_BY_TYPE

    def get_title(self, localizer: Localizer) -> str:
        return localizer.translate(get_connector_info_provider(self.conn_type).title_translatable)


@attr.s(kw_only=True)
class ConnectorContainer(ConnectorBase):
    """
    Acts like a meta-connection, the only purpose of which is to show that it combines other connections
    It is up to the UI to decide what to do with it
    """

    conn_type_str = "__meta__"
    _alias: str = attr.ib()
    includes: list[Connector] = attr.ib(validator=attr.validators.min_len(1))
    title_translatable: Translatable = attr.ib()

    @property
    def hidden(self) -> bool:
        return all(connector.hidden for connector in self.includes)

    @property
    def visibility_mode(self) -> ConnectorAvailability:
        return ConnectorAvailability.free

    @property
    def alias(self) -> str:
        return self._alias

    @property
    def backend_driven_form(self) -> bool:
        return False

    def get_title(self, localizer: Localizer) -> str:
        return localizer.translate(self.title_translatable)

    def iter_connectors(self) -> Iterator[Connector]:
        return itertools.chain(self.includes)

    @classmethod
    def from_settings(cls, settings: ConnectorContainerSettings) -> ConnectorContainer:
        return cls(
            alias=settings.alias,
            title_translatable=_make_translatable(settings.title_translatable.text, settings.title_translatable.domain),
            includes=[Connector.from_settings(item) for item in settings.includes],
        )

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        return dict(
            **super().as_dict(localizer),
            includes=[connector.as_dict(localizer) for connector in self.includes],
        )


@attr.s(kw_only=True)
class ConnectorAvailabilityConfig(SettingsBase):
    uncategorized: list[Connector] = attr.ib(factory=list)
    sections: list[Section] = attr.ib(factory=list)

    visible_connectors: set[ConnectionType] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "set[ConnectionType]")  [assignment]
        "VISIBLE",
        env_var_converter=conn_type_set_env_var_converter,
        missing_factory=set,
    )
    icon_src: ConnectorIconSrcConfig = attr.ib(default=None)

    _icon_by_conn_type: dict[str, dict[str, Any]] = attr.ib(factory=dict)

    def fill_icon_dict_by_conn_type(self) -> None:
        if self._icon_by_conn_type or self.icon_src is None:
            return
        for conn in self._iter_connectors():
            conn_type = conn.conn_type.value
            icon_src = self.icon_src.as_dict(conn=conn)
            if icon_src is not None:
                self._icon_by_conn_type[conn_type] = deepcopy(icon_src)

    def list_icons(self) -> list[dict[str, Any]]:
        self.fill_icon_dict_by_conn_type()
        return list(self._icon_by_conn_type.values())

    def get_icon(self, conn_type: str) -> Optional[dict[str, Any]]:
        self.fill_icon_dict_by_conn_type()
        if conn_type not in self._icon_by_conn_type:
            raise ConnectorIconNotFoundException()
        return self._icon_by_conn_type[conn_type]

    def __attrs_post_init__(self) -> None:
        for connector in self._iter_connectors():
            connector._hidden = connector.conn_type not in self.visible_connectors

    @classmethod
    def from_settings(
        cls, settings: ConnectorAvailabilityConfigSettings | ObjectLikeConfig
    ) -> ConnectorAvailabilityConfig:
        visible_connectors: set[ConnectionType] = {ConnectionType(item) for item in settings.visible_connectors}

        return cls(  # type: ignore  # 2024-01-24 # TODO: Unexpected keyword argument "visible_connectors" for "ConnectorAvailabilityConfig"  [call-arg]
            uncategorized=[Connector.from_settings(item) for item in settings.uncategorized],
            sections=[Section.from_settings(item) for item in settings.sections],
            visible_connectors=visible_connectors,
            icon_src=connector_icon_src_config_factory(settings.icon_src),
        )

    def _iter_connectors(self) -> Iterator[Connector]:
        return itertools.chain(
            self.uncategorized,
            itertools.chain.from_iterable(
                connector.includes if isinstance(connector, ConnectorContainer) else (connector,)  # type: ignore  # 2024-01-30 # TODO: Generator has incompatible item type "Sequence[ConnectorBase]"; expected "Iterable[Connector]"  [misc]
                for connector in itertools.chain.from_iterable(section.connectors for section in self.sections)
            ),
        )

    def get_available_connectors(self, localizer: Localizer, tenant: Optional[TenantDef]) -> dict[str, Any]:
        return dict(
            uncategorized=[connector.as_dict(localizer) for connector in self.uncategorized],
            sections=[section.as_dict(localizer) for section in self.sections],
        )

    def check_connector_is_available(self, conn_type: ConnectionType) -> bool:
        conn_options = next((conn for conn in self._iter_connectors() if conn.conn_type == conn_type), None)
        if conn_options is None:
            LOGGER.warning("Connector %s is not available in current env", conn_type.name)
            return False

        return conn_options.visibility_mode != ConnectorAvailability.uncreatable
