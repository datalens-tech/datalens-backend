from __future__ import annotations

import os

RUN_DEVHOST_TESTS = os.environ.get('RUN_DEVHOST_TESTS')
TESTS_CACHES_DIR = os.environ.get('TESTS_CACHES_DIR')
CA_BUNDLE_FILE = os.environ.get('REQUESTS_CA_BUNDLE') or '/etc/ssl/certs/ca-certificates.crt'
RUN_TESTS_WITH_MATERIALIZATION = bool(int(os.environ.get('RUN_TESTS_WITH_MATERIALIZATION', True)))

DL_ENV_TESTS_FILTER = os.environ.get('DL_ENV_TESTS_FILTER')

# TODO FIX: Ask IAM team why there is so big lag in sync with AS
ACCESS_SERVICE_PERMISSIONS_CHECK_DELAY = 5
