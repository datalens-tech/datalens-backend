import os

from typing import Tuple, TypeVar, Container, Callable

from bi_constants.enums import ConnectionType

from bi_configs.enums import AppType, EnvType


ROOT_CERTIFICATES_FILENAME = "/etc/ssl/certs/ca-certificates.crt"
TEMP_ROOT_CERTIFICATES_FOLDER_PATH = "/tmp/ssl/certs/"


_T = TypeVar('_T')


def validate_one_of(valid_values: Container[_T]) -> Callable[[_T], _T]:
    def validator(value: _T) -> _T:
        if value not in valid_values:
            raise ValueError(f"Expected one of {valid_values}, but got '{value}'")
        return value

    return validator


def get_root_certificates() -> bytes:
    with open(get_root_certificates_path(), "rb") as fobj:
        return fobj.read()


def get_root_certificates_path() -> str:
    return ROOT_CERTIFICATES_FILENAME


def get_temp_root_certificates_folder_path() -> str:
    os.makedirs(TEMP_ROOT_CERTIFICATES_FOLDER_PATH, exist_ok=True)

    return TEMP_ROOT_CERTIFICATES_FOLDER_PATH


def split_by_comma(s: str) -> Tuple[str, ...]:
    return tuple(entry.strip() for entry in s.split(",") if entry)


def app_type_env_var_converter(env_value: str) -> AppType:
    return {
        "testing": AppType.CLOUD,
        "production": AppType.CLOUD,
        "testing-public": AppType.CLOUD_PUBLIC,
        "production-public": AppType.CLOUD_PUBLIC,
        "testing-sec-embeds": AppType.CLOUD_EMBED,
        "production-sec-embeds": AppType.CLOUD_EMBED,
        "int-testing": AppType.INTRANET,
        "int-production": AppType.INTRANET,
        "datacloud": AppType.DATA_CLOUD,
        "israel": AppType.NEBIUS,
        "nemax": AppType.NEBIUS,
        "tests": AppType.TESTS,
        "development": AppType.TESTS,
        "datacloud-sec-embeds": AppType.DATA_CLOUD_EMBED,
    }[env_value.lower()]


def env_type_env_var_converter(env_value: str) -> EnvType:
    return {
        "testing": EnvType.yc_testing,
        "production": EnvType.yc_production,
        "testing-public": EnvType.yc_testing,
        "production-public": EnvType.yc_production,
        "testing-sec-embeds": EnvType.yc_testing,
        "production-sec-embeds": EnvType.yc_production,
        "int-testing": EnvType.int_testing,
        "int-production": EnvType.int_production,
        "datacloud": EnvType.dc_any,
        "israel": EnvType.israel,
        "nemax": EnvType.nemax,
        "tests": EnvType.development,
        "development": EnvType.development,
        "datacloud-sec-embeds": EnvType.dc_any,
    }[env_value.lower()]


def jaeger_service_suffix_env_var_converter(env_value: str) -> str:
    return "-testing" if env_value.lower() in ("testing", "testing-public", "testing-sec-embeds", "int-testing") else ""


def conn_type_set_env_var_converter(s: str) -> set[ConnectionType, ...]:
    return set(ConnectionType(entry.strip()) for entry in s.split(",") if entry)
