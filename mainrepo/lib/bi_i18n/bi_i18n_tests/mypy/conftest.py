from __future__ import annotations


def mypy_check_root() -> str:
    # Because of 'TOP_LEVEL', the resource names look like 'bi_i18n/__init__.py'.
    return "bi_i18n/"


def mypy_config_resource() -> tuple[str, str]:
    return "__tests__", "datalens/backend/lib/bi_i18n/mypy.ini"
