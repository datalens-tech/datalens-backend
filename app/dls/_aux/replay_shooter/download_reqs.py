#!/usr/bin/env python2

from __future__ import annotations

import sys
import re
import ast
import ujson as json
from yt.wrapper.client import YtClient

src_path = sys.argv[1]
host = 'https://datalens-dls.private-api.ycp.cloud-preprod.yandex.net'

ytc = YtClient(proxy="hahn")
rows = ytc.read_table(src_path)
reqs = (dict(
    url='{}{}'.format(host, row['fields']['request_uri']),
    method=row['fields']['request_method'],
    data=ast.literal_eval(re.sub(r'^DLS request body: bytearray\((.*)\)$', r'\1', row['bodies.message'])) or None,
    headers=row['fields']['request_headers'],
) for row in rows)

with open('.dls_reqs.jsl', 'wb') as fo:
    for req in reqs:
        req_s = json.dumps(req)
        fo.write(req_s)
        fo.write('\n')
