#!/usr/bin/env python3

from __future__ import annotations

from dl_core.connection_executors.remote_query_executor import app_sync as app_sync_mod


app = app_sync_mod.create_sync_app()


app_sync_mod.hook_init_logging(app)
