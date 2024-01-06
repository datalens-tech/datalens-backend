import os
from typing import (
    Callable,
    Container,
    TypeVar,
)

from dl_constants.enums import ConnectionType


DEFAULT_ROOT_CERTIFICATES_FILENAME = "/etc/ssl/certs/ca-certificates.crt"
TEMP_ROOT_CERTIFICATES_FOLDER_PATH = "/tmp/ssl/certs/"


_T = TypeVar("_T")


def validate_one_of(valid_values: Container[_T]) -> Callable[[_T], _T]:
    def validator(value: _T) -> _T:
        if value not in valid_values:
            raise ValueError(f"Expected one of {valid_values}, but got '{value}'")
        return value

    return validator


def get_root_certificates(path: str = DEFAULT_ROOT_CERTIFICATES_FILENAME) -> bytes:
    with open(path, "rb") as fobj:
        return fobj.read()


def get_root_certificates_path() -> str:
    return DEFAULT_ROOT_CERTIFICATES_FILENAME


def get_temp_root_certificates_folder_path() -> str:
    os.makedirs(TEMP_ROOT_CERTIFICATES_FOLDER_PATH, exist_ok=True)

    return TEMP_ROOT_CERTIFICATES_FOLDER_PATH


def split_by_comma(s: str) -> tuple[str, ...]:
    return tuple(entry.strip() for entry in s.split(",") if entry)


def conn_type_set_env_var_converter(s: str) -> set[ConnectionType]:
    return set(ConnectionType(entry.strip()) for entry in s.split(",") if entry)
