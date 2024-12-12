import json
import logging
import os
import typing
from typing import (
    Callable,
    Container,
    TypeVar,
)

import typing_extensions

from dl_constants.enums import ConnectionType


DEFAULT_ROOT_CERTIFICATES_FILENAME = "/etc/ssl/certs/ca-certificates.crt"
TEMP_ROOT_CERTIFICATES_FOLDER_PATH = "/tmp/ssl/certs/"


LOGGER = logging.getLogger(__name__)


_T = TypeVar("_T")


def validate_one_of(valid_values: Container[_T]) -> Callable[[_T], _T]:
    def validator(value: _T) -> _T:
        if value not in valid_values:
            raise ValueError(f"Expected one of {valid_values}, but got '{value}'")
        return value

    return validator


def get_root_certificates(path: str = DEFAULT_ROOT_CERTIFICATES_FILENAME) -> bytes:
    """
    expects a path to a file with PEM certificates

    aiohttp-based clients expect certificates as an ascii string to create ssl.sslContext
    while grpc-clients expect them as a byte representation of an ascii string to create the special grpc ssl context
    """
    with open(path, "rb") as fobj:
        ca_data = fobj.read()
    # fail fast
    try:
        ca_data.decode("ascii")
    except UnicodeDecodeError:
        LOGGER.exception("Looks like the certificates are not in PEM format")
        raise
    return ca_data


def get_multiple_root_certificates(*paths: str) -> bytes:
    return b"\n".join([get_root_certificates(path=path) for path in paths])


def get_root_certificates_path() -> str:
    return DEFAULT_ROOT_CERTIFICATES_FILENAME


def get_temp_root_certificates_folder_path() -> str:
    os.makedirs(TEMP_ROOT_CERTIFICATES_FOLDER_PATH, exist_ok=True)

    return TEMP_ROOT_CERTIFICATES_FOLDER_PATH


def split_by_comma(s: str) -> tuple[str, ...]:
    return tuple(entry.strip() for entry in s.split(",") if entry)


def conn_type_set_env_var_converter(s: str) -> set[ConnectionType]:
    return set(ConnectionType(entry.strip()) for entry in s.split(",") if entry)


class ConstructableSettingsProtocol(typing.Protocol):
    @classmethod
    def from_json(cls, json_value: typing.Any) -> typing_extensions.Self: ...


SettingsT = typing.TypeVar("SettingsT", bound=ConstructableSettingsProtocol)


def tuple_of_models_json_converter_factory(
    model: type[SettingsT],
) -> typing.Callable[[typing.Any], tuple[SettingsT, ...]]:
    assert hasattr(model, "from_json")

    def tuple_of_models_json_converter(json_value: typing.Any) -> tuple[SettingsT, ...]:
        assert isinstance(json_value, list), f"Expected list, but got {type(json_value)}"
        return tuple(model.from_json(entry) for entry in json_value)

    return tuple_of_models_json_converter


def tuple_of_models_env_converter_factory(
    model: type[SettingsT],
) -> typing.Callable[[str], tuple[SettingsT, ...]]:
    def tuple_of_models_env_converter(env_value: str) -> tuple[SettingsT, ...]:
        json_value = json.loads(env_value)
        return tuple_of_models_json_converter_factory(model)(json_value)

    return tuple_of_models_env_converter
