#!./.venv/bin/python3

from __future__ import annotations

import os
import ujson as json
import requests

fln = '.dls_reqs.jsl'

reqr = requests.Session()

with open(fln) as fo:
    for idx, line in enumerate(fo):
        item = json.loads(line.strip())
        resp = reqr.request(**item)
        out = dict(idx=idx, resp=repr(resp), timing=resp.elapsed.total_seconds())
        print(json.dumps(out))
