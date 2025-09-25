import pathlib

import dl_testing


dl_testing.register_all_assert_rewrites(__name__, pathlib.Path(__file__).parent)

from .client import TestingHttpxClient


__all__ = [
    "TestingHttpxClient",
]
