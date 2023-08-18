#!/usr/bin/env python3

from __future__ import annotations

import sys

from bi_core.connection_executors.remote_query_executor.app_async import async_qe_main


if __name__ == '__main__':
    sys.exit(async_qe_main())
