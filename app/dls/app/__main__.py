#!/usr/bin/env python3

from __future__ import annotations

import sys

from bi_app_tools.entry_point import BIEntryPoint


class BIDLSEntryPoint(BIEntryPoint):
    """ Overrides point """

    subcommands_global = BIEntryPoint.subcommands_global + (
        'hypercorn',
        'bidls_httpjrpc_hc', 'bidls_httpjrpc', 'bidls_devhttp',
        'bidls_tasks',
    )

    @staticmethod
    def main_hypercorn():
        import hypercorn.__main__ as mod
        return mod.main()

    @staticmethod
    def main_bidls_httpjrpc_hc():
        import yadls.httpjrpc.main_hc as mod
        return mod.main()

    @staticmethod
    def main_bidls_httpjrpc():
        import yadls.httpjrpc.main as mod
        return mod.main()

    @staticmethod
    def main_bidls_devhttp():
        import yadls.httpjrpc.main as mod
        return mod.main()

    @staticmethod
    def main_bidls_tasks():
        import yadls.tasks as mod
        return mod.main()


ENTRY_POINT = BIDLSEntryPoint()


if __name__ == '__main__':
    sys.exit(ENTRY_POINT.main())
