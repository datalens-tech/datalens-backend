"""
Python Database API Specification 2.0 interface realization for AppMetrica API
https://www.python.org/dev/peps/pep-0249/
"""

from __future__ import annotations

from sqlalchemy.types import (  # noqa; TODO: might actually be unnecessary.
    DATE,
    DATETIME,
    FLOAT,
    INTEGER,
    NULLTYPE,
    VARCHAR,
)
from sqlalchemy_metrika_api import metrika_dbapi
from sqlalchemy_metrika_api.api_client import APPMETRICA_API_HOST
from sqlalchemy_metrika_api.api_info.appmetrica import (
    AppMetricaFieldsNamespaces,
    fields_by_namespace,
)
from sqlalchemy_metrika_api.exceptions import (  # noqa
    ConnectionClosedException,
    CursorClosedException,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    MetrikaApiAccessDeniedException,
    MetrikaApiException,
    MetrikaApiObjectNotFoundException,
    MetrikaHttpApiException,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"
default_storage_plugin = ""


class Connection(metrika_dbapi.Connection):
    metrica_host = APPMETRICA_API_HOST
    metrica_fields_namespaces_enum = AppMetricaFieldsNamespaces

    @metrika_dbapi.check_connected
    def cursor(self):
        return Cursor(api_client=self._cli, connection=self)

    @metrika_dbapi.check_connected
    def get_table_names(self):
        avail_counters = self._cli.get_available_counters()
        return list(str(c_info["id"]) for c_info in avail_counters)

    def get_columns(self):
        field_props = ("name", "type", "is_dim")
        return {
            "fields": field_props,
            "data": [
                tuple(f_desc[prop] for prop in field_props) for f_desc in fields_by_namespace[self.fields_namespace]
            ],
        }


def connect(oauth_token=None, **kwargs):
    oauth_token = oauth_token or kwargs.get("password")
    fields_namespace = kwargs.get("database")
    accuracy = kwargs.get("accuracy")
    return Connection(oauth_token=oauth_token, fields_namespace=fields_namespace, accuracy=accuracy)  # , **kwargs)


class Cursor(metrika_dbapi.Cursor):
    """AppMetrica dbapi cursor"""
