import pathlib

import dl_testing


dl_testing.register_all_assert_rewrites(__name__, pathlib.Path(__file__).parent)

from .admin import AdminExtTestSuite
from .base import (
    ExtTestSuiteBase,
    ExtTestSuiteSettings,
    ReadinessSubsystemSettings,
)
from .docs import DocsExtTestSuite
from .system import SystemExtTestSuite


__all__ = [
    "AdminExtTestSuite",
    "DocsExtTestSuite",
    "ExtTestSuiteBase",
    "ExtTestSuiteSettings",
    "ReadinessSubsystemSettings",
    "SystemExtTestSuite",
]
