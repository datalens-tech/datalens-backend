#!/usr/bin/env python3

from __future__ import annotations

import sys

from bi_app_tools.entry_point import BIEntryPoint


class BIAlertsEntryPoint(BIEntryPoint):
    """ Overrides point """


ENTRY_POINT = BIAlertsEntryPoint()


if __name__ == '__main__':
    sys.exit(ENTRY_POINT.main())
