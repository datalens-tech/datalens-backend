#!/usr/bin/env python3

from __future__ import annotations

import sys

from bi_app_tools.entry_point import BIEntryPoint


class BIExternalAPIEntryPoint(BIEntryPoint):
    do_register_sa_dialects = False

    subcommands_global: tuple[str, ...] = BIEntryPoint.subcommands_global + (
        "bi-ext-api-grpc-proxy",
    )

    @staticmethod
    def main_bi_ext_api_grpc_proxy():
        from bi_external_api.grpc_proxy.server import main
        return main()


ENTRY_POINT = BIExternalAPIEntryPoint()


if __name__ == '__main__':
    sys.exit(ENTRY_POINT.main())
