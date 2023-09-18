from __future__ import annotations

from _socket import gaierror

import psycopg2

from dl_connector_postgresql.core.postgresql_base.error_transformer import sync_pg_db_error_transformer, \
    make_async_pg_error_transformer
from dl_core.exc import SourceHostNotKnownError

NAME_OR_SERVICE_NOT_KNOWN_MSG = '''
        could not translate host name
        "c-someclusterid.ro.mdb.yandexcloud.net" to address:
        Name or service not known
        '''


def test_name_or_service_not_known_sync():
    transformer = sync_pg_db_error_transformer

    parameters = transformer.make_bi_error_parameters(
        wrapper_exc=psycopg2.OperationalError(NAME_OR_SERVICE_NOT_KNOWN_MSG)
    )

    assert parameters[0] == SourceHostNotKnownError


def test_name_or_service_not_known_async():
    transformer = make_async_pg_error_transformer()

    parameters = transformer.make_bi_error_parameters(
        wrapper_exc=gaierror(NAME_OR_SERVICE_NOT_KNOWN_MSG)
    )

    assert parameters[0] == SourceHostNotKnownError
