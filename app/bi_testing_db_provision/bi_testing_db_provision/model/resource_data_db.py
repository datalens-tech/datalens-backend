from typing import Sequence

import attr

from bi_testing_db_provision.model.commons_db import DBCreds
from bi_testing_db_provision.model.resource import ResourceExternalData


@attr.s
class ResourceExternalDataDBDefaultSQL(ResourceExternalData):
    db_host: str = attr.ib()
    db_port: int = attr.ib()
    db_names: Sequence[str] = attr.ib()
    creds: Sequence[DBCreds] = attr.ib()
