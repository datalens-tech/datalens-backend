import abc

import attr
import dynamic_enum

import dl_constants


class AuthTarget(dynamic_enum.DynamicEnum):
    UNITED_STORAGE = dynamic_enum.AutoEnumValue()


class AuthData(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_headers(self, target: AuthTarget | None = None) -> dict[dl_constants.DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_cookies(self, target: AuthTarget | None = None) -> dict[dl_constants.DLCookies, str]:
        raise NotImplementedError()


@attr.s()
class NoAuthData(AuthData):
    def get_headers(self, target: AuthTarget | None = None) -> dict[dl_constants.DLHeaders, str]:
        return {}

    def get_cookies(self, target: AuthTarget | None = None) -> dict[dl_constants.DLCookies, str]:
        return {}
