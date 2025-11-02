import abc
import enum

import attr

import dl_constants


class AuthTarget(str, enum.Enum):
    UNITED_STORAGE = "united_storage"


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
