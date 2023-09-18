from __future__ import annotations

import abc
import logging
from typing import (
    AbstractSet,
    ClassVar,
    Type,
)

import attr

from bi_core.connection_models import ConnDTO

LOGGER = logging.getLogger(__name__)


class ConnectionSecurityManager(metaclass=abc.ABCMeta):
    """
    This class performs security checks against connection executions.
    Assumed that instance of this class will be created on each request.
    Individual checks should be performed in context of instance fields that are representing current request context.
    """

    @abc.abstractmethod
    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        """
        Must return False if connection is potentially unsafe and should be executed in isolated environment.
        """

    # TODO FIX: determine if we need dedicated method
    @abc.abstractmethod
    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        """
        Mostly costyl for bi_core/data_processing/selectors/utils.py:get_query_type()
        Potentially it's behaviour should be same as .is_safe_connection() (TBD: check it),
         but we need exactly the same data to determine if connection is internal.
        """


class ConnectionSafetyChecker(metaclass=abc.ABCMeta):
    _DTO_TYPES: ClassVar[set[Type[ConnDTO]]]

    @classmethod
    def register_dto_types(cls, dto_classes: AbstractSet[Type[ConnDTO]]) -> None:
        cls._DTO_TYPES.update(dto_classes)

    @abc.abstractmethod
    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        raise NotImplementedError


@attr.s(kw_only=True)
class InsecureConnectionSafetyChecker(ConnectionSafetyChecker):
    """Always safe"""

    _DTO_TYPES: ClassVar[set[Type[ConnDTO]]] = set()

    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        return True


@attr.s(kw_only=True)
class NonUserInputConnectionSafetyChecker(ConnectionSafetyChecker):
    """Hosts are not entered by user"""

    _DTO_TYPES: ClassVar[set[Type[ConnDTO]]] = set()

    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        if type(conn_dto) in self._DTO_TYPES:
            LOGGER.info("%r in safe DTO types", type(conn_dto))
            return True
        return False


@attr.s
class GenericConnectionSecurityManager(ConnectionSecurityManager, metaclass=abc.ABCMeta):
    conn_sec_checkers: list[ConnectionSafetyChecker] = attr.ib()

    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        return any(conn_sec_checker.is_safe_connection(conn_dto) for conn_sec_checker in self.conn_sec_checkers)


@attr.s
class InsecureConnectionSecurityManager(GenericConnectionSecurityManager):
    conn_sec_checkers: list[ConnectionSafetyChecker] = attr.ib(default=[InsecureConnectionSafetyChecker()])

    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        return True


@attr.s(frozen=True)
class ConnSecuritySettings:
    security_checker_cls: Type[ConnectionSafetyChecker] = attr.ib()
    dtos: AbstractSet[Type[ConnDTO]] = attr.ib()
