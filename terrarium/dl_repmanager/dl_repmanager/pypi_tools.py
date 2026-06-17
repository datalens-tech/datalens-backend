"""
Simple methods to get package info from public PyPI
"""

import requests

REQUEST_TIMEOUT_SEC = 30

DEFAULT_ENDPOINT = "https://pypi.org/pypi/"


def get_package_info(package_name: str, override_endpoint: str | None = None) -> dict:
    endpoint = override_endpoint or DEFAULT_ENDPOINT
    return requests.get(url=f"{endpoint}{package_name}/json", timeout=REQUEST_TIMEOUT_SEC).json()


def get_package_info_by_version(package_name: str, version: str, override_endpoint: str | None = None) -> dict:
    endpoint = override_endpoint or DEFAULT_ENDPOINT
    return requests.get(url=f"{endpoint}{package_name}/{version}/json", timeout=REQUEST_TIMEOUT_SEC).json()
