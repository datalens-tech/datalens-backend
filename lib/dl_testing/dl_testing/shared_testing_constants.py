from __future__ import annotations

import os

from dl_configs.utils import DEFAULT_ROOT_CERTIFICATES_FILENAME


RUN_DEVHOST_TESTS = os.environ.get("RUN_DEVHOST_TESTS")
TESTS_CACHES_DIR = os.environ.get("TESTS_CACHES_DIR")
CA_BUNDLE_FILE = (
    os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("CA_FILE_PATH") or DEFAULT_ROOT_CERTIFICATES_FILENAME
)

DL_ENV_TESTS_FILTER = os.environ.get("DL_ENV_TESTS_FILTER")
