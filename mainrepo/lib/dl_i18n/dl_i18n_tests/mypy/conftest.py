from __future__ import annotations


def mypy_check_root() -> str:
    # Because of 'TOP_LEVEL', the resource names look like 'dl_i18n/__init__.py'.
    return "dl_i18n/"


def mypy_config_resource() -> tuple[str, str]:
    return "__tests__", "datalens/backend/lib/dl_i18n/mypy.ini"
