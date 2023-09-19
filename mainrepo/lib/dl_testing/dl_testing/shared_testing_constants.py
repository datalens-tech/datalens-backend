from __future__ import annotations

import os


RUN_DEVHOST_TESTS = os.environ.get("RUN_DEVHOST_TESTS")
TESTS_CACHES_DIR = os.environ.get("TESTS_CACHES_DIR")
CA_BUNDLE_FILE = os.environ.get("REQUESTS_CA_BUNDLE") or "/etc/ssl/certs/ca-certificates.crt"

DL_ENV_TESTS_FILTER = os.environ.get("DL_ENV_TESTS_FILTER")
