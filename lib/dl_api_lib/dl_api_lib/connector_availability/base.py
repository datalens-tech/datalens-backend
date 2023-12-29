from __future__ import annotations

import abc
import itertools
import logging
from typing import (
    Any,
    Iterator,
    Optional,
)

import attr

from dl_api_lib.connection_forms.registry import CONN_FORM_FACTORY_BY_TYPE
from dl_api_lib.connection_info import get_connector_info_provider_cls
from dl_api_lib.i18n.localizer import Translatable
from dl_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    ConnectorBaseSettings,
    ConnectorContainerSettings,
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
    connectors: list[ConnectorBase] = attr.ib(validator=attr.validators.min_len(1))  # type: ignore

    @classmethod
    def from_settings(cls, settings: SectionSettings) -> Section:
        def _connector_from_settings(settings: ConnectorBaseSettings | ObjectLikeConfig) -> ConnectorBase:
            if hasattr(settings, "conn_type"):
                return Connector.from_settings(settings)  # type: ignore
            elif hasattr(settings, "includes"):
                return ConnectorContainer.from_settings(settings)  # typeL ignore
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
    visibility_mode: ConnectorAvailability = None

    @property
    @abc.abstractmethod
    def hidden(self) -> bool:
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


@attr.s(kw_only=True)
class Connector(ConnectorBase):
    """Represents an actual connection type"""

    _hidden: bool = attr.ib(init=False, default=False)
    conn_type: ConnectionType = attr.ib(
        converter=lambda v: ConnectionType(v) if not isinstance(v, ConnectionType) else v
    )
    visibility_mode: ConnectorAvailability = attr.ib(
        default=ConnectorAvailability.free, converter=ConnectorAvailability
    )

    @classmethod
    def from_settings(cls, settings: ConnectorSettings) -> Connector:
        return cls(
            conn_type=ConnectionType(settings.conn_type),
            visibility_mode=settings.availability,
        )

    @property
    def hidden(self) -> bool:
        return self._hidden

    @property
    def alias(self) -> str:
        return get_connector_info_provider_cls(self.conn_type).alias or self.conn_type_str

    @property
    def conn_type_str(self) -> str:
        return self.conn_type.name

    @property
    def backend_driven_form(self) -> bool:
        return self.conn_type in CONN_FORM_FACTORY_BY_TYPE

    def get_title(self, localizer: Localizer) -> str:
        return localizer.translate(get_connector_info_provider_cls(self.conn_type).title_translatable)


@attr.s(kw_only=True)
class ConnectorContainer(ConnectorBase):
    """
    Acts like a meta-connection, the only purpose of which is to show that it combines other connections
    It is up to the UI to decide what to do with it
    """

    conn_type_str = "__meta__"
    _alias: str = attr.ib()
    includes: list[Connector] = attr.ib(validator=attr.validators.min_len(1))  # type: ignore
    title_translatable: Translatable = attr.ib()
    visibility_mode: ConnectorAvailability = attr.ib(default=ConnectorAvailability.free)

    @property
    def hidden(self) -> bool:
        return all(connector.hidden for connector in self.includes)

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

    visible_connectors: set[ConnectionType] = s_attrib(  # type: ignore
        "VISIBLE",
        env_var_converter=conn_type_set_env_var_converter,
        missing_factory=set,
    )

    def __attrs_post_init__(self) -> None:
        for connector in self._iter_connectors():
            connector._hidden = connector.conn_type not in self.visible_connectors

    @classmethod
    def from_settings(
        cls, settings: ConnectorAvailabilityConfigSettings | ObjectLikeConfig
    ) -> ConnectorAvailabilityConfig:
        visible_connectors: set[ConnectionType] = {ConnectionType(item) for item in settings.visible_connectors}

        return cls(
            uncategorized=[Connector.from_settings(item) for item in settings.uncategorized],
            sections=[Section.from_settings(item) for item in settings.sections],
            visible_connectors=visible_connectors,
        )

    def _iter_connectors(self) -> Iterator[Connector]:
        return itertools.chain(
            self.uncategorized,
            itertools.chain.from_iterable(
                connector.includes if isinstance(connector, ConnectorContainer) else (connector,)  # type: ignore
                for connector in itertools.chain.from_iterable(section.connectors for section in self.sections)
            ),
        )

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        return dict(
            uncategorized=[connector.as_dict(localizer) for connector in self.uncategorized],
            sections=[section.as_dict(localizer) for section in self.sections],
        )

    def check_connector_is_available(self, conn_type: ConnectionType) -> bool:
        conn_options = next((conn for conn in self._iter_connectors() if conn.conn_type == conn_type), None)
        if conn_options is None:
            LOGGER.warning("Connector %s is not available in current env", conn_type.name)
            return False

        return conn_options.visibility_mode == ConnectorAvailability.free
