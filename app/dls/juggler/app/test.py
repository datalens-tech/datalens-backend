#!/usr/bin/env python2
"""
...
"""

# from __future__ import division, absolute_import, print_function, unicode_literals

from __future__ import annotations

import os
import sys
import datetime

assert sys.argv[1] == '--service'
svc = sys.argv[2]
print("PASSIVE-CHECK:{svc};OK;{ts} pid={pid}".format(svc=svc, pid=os.getpid(), ts=datetime.datetime.now().isoformat()))
