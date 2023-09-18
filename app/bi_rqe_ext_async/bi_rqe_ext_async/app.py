#!/usr/bin/env python3

from __future__ import annotations

import sys

from dl_core.connection_executors.remote_query_executor.app_async import async_qe_main as app


if __name__ == '__main__':
    sys.exit(app())
