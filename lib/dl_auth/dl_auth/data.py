import abc

import attr

import dl_constants
import dl_dynamic_enum


class AuthTarget(dl_dynamic_enum.DynamicEnum):
    UNITED_STORAGE = dl_dynamic_enum.AutoEnumValue()


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
