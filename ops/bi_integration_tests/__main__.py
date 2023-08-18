#!/usr/bin/env python3

from __future__ import annotations

import logging
import os
import sys

import pytest

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    test_tags = os.environ.get('INTEGRATION_TESTS_TAGS')
    sys.exit(pytest.main(["-ra", f'-m {test_tags}', "bi_integration_tests_tests"]))
