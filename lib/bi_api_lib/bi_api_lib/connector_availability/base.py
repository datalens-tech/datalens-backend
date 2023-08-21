from __future__ import annotations

import abc
import itertools
import logging
from enum import Enum
from typing import Optional, Any, Iterator

import attr

from bi_constants.enums import ConnectionType

from bi_i18n.localizer_base import Localizer
from bi_api_lib.connection_forms.registry import CONN_FORM_FACTORY_BY_TYPE
from bi_api_lib.connection_info import get_connector_info_provider_cls
from bi_api_lib.i18n.localizer import Translatable


LOGGER = logging.getLogger(__name__)


class LocalizedSerializable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        raise NotImplementedError


class ConnectorAvailability(Enum):
    free = 'free'
    whitelist = 'whitelist'


@attr.s(kw_only=True)
class Section(LocalizedSerializable):
    title_translatable: Translatable = attr.ib()
    connectors: list[ConnectorBase] = attr.ib(validator=attr.validators.min_len(1))  # type: ignore

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        return dict(
            title=localizer.translate(self.title_translatable),
            connectors=[connector.as_dict(localizer) for connector in self.connectors]
        )


@attr.s(kw_only=True)
class ConnectorBase(LocalizedSerializable, metaclass=abc.ABCMeta):
    availability: ConnectorAvailability = attr.ib(default=ConnectorAvailability.free)
    product_id: Optional[str] = attr.ib(default=None)
    hidden: bool = attr.ib()

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
            alias=self.alias,
            conn_type=self.conn_type_str,
            title=self.get_title(localizer),
            backend_driven_form=self.backend_driven_form,
        )


@attr.s(kw_only=True)
class Connector(ConnectorBase):
    """ Represents an actual connection type """

    conn_type: ConnectionType = attr.ib()

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

    conn_type_str = '__meta__'
    _alias: str = attr.ib()
    includes: list[Connector] = attr.ib()
    title_translatable: Translatable = attr.ib()

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

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        return dict(
            **super().as_dict(localizer),
            includes=[connector.as_dict(localizer) for connector in self.includes],
        )


@attr.s(kw_only=True)
class ConnectorAvailabilityConfig:
    uncategorized: list[Connector] = attr.ib(factory=list)
    sections: list[Section] = attr.ib(factory=list)

    def _iter_connectors(self) -> Iterator[Connector]:
        return itertools.chain(
            self.uncategorized,
            itertools.chain.from_iterable(
                connector.includes if isinstance(connector, ConnectorContainer) else (connector,)  # type: ignore
                for connector in itertools.chain.from_iterable(section.connectors for section in self.sections)
            )
        )

    def as_dict(self, localizer: Localizer) -> dict[str, Any]:
        result = [  # TODO REMOVE: backward compatibility
            connector.as_dict(localizer)
            for connector in self._iter_connectors()
            if connector.availability == ConnectorAvailability.free
        ]
        for connector in result:
            ct = connector.get('conn_type')
            if ct == ConnectionType.ch_over_yt.name:
                connector['alias'] = 'chyt'
            if ct == ConnectionType.ch_over_yt_user_auth.name:
                connector['hidden'] = True
        return dict(
            uncategorized=[connector.as_dict(localizer) for connector in self.uncategorized],
            sections=[section.as_dict(localizer) for section in self.sections],
            result=result,
        )

    def check_connector_is_available(self, conn_type: ConnectionType) -> bool:
        conn_options = next((conn for conn in self._iter_connectors() if conn.conn_type == conn_type), None)
        if conn_options is None:
            LOGGER.warning('Connector %s is not available in current env', conn_type.name)
            return False

        return conn_options.availability == ConnectorAvailability.free
