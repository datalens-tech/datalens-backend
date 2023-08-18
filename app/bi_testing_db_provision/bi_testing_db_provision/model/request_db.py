from typing import Sequence

import attr

from bi_testing_db_provision.model.commons_db import DBCreds
from bi_testing_db_provision.model.request_base import ResourceRequest


@attr.s(frozen=True)
class DBRequest(ResourceRequest):
    version: str = attr.ib(default=None)
    db_names: Sequence[str] = attr.ib(default=())
    creds: Sequence[DBCreds] = attr.ib(default=())
