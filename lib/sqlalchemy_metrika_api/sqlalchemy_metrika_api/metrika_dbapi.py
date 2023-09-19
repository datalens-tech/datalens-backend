"""
Python Database API Specification 2.0 interface realization for Metrika API
https://www.python.org/dev/peps/pep-0249/
"""

from __future__ import annotations

from datetime import date
from enum import Enum
from functools import wraps
from urllib.parse import parse_qs

import dateutil.parser
from sqlalchemy.types import (
    BOOLEAN,
    DATE,
    DATETIME,
    FLOAT,
    INTEGER,
    NULLTYPE,
    VARCHAR,
)
from sqlalchemy_metrika_api.api_client import (
    METRIKA_API_HOST,
    MetrikaApiClient,
)
from sqlalchemy_metrika_api.api_info.metrika import (
    MetrikaApiCounterSource,
    fields_by_name,
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


metrika_types_to_sqla = {
    "integer": INTEGER,
    "float": FLOAT,
    "string": VARCHAR,
    "date": DATE,
    "datetime": DATETIME,
    # "percents": FLOAT,
}


cast_processors = {
    "INTEGER": int,
    "FLOAT": float,
    "VARCHAR": str,
    "BOOLEAN": bool,
    "DATE": lambda t: dateutil.parser.parse(t).date(),
    "DATETIME": lambda t: dateutil.parser.parse(t),
}


cast_processors_for_metrika_types = {
    "date": cast_processors["DATE"],
    "datetime": cast_processors["DATETIME"],
}


cast_type_to_sqla_type = {
    "INTEGER": INTEGER,
    "FLOAT": FLOAT,
    "VARCHAR": VARCHAR,
    "BOOLEAN": BOOLEAN,
    "DATE": DATE,
    "DATETIME": DATETIME,
}


def check_connected(func):
    @wraps(func)
    def func_wrapper(self, *args, **kwargs):
        if self.is_connected is False:
            raise ConnectionClosedException("Connection object is closed")
        else:
            return func(self, *args, **kwargs)

    return func_wrapper


class Connection(object):
    metrica_host = METRIKA_API_HOST
    metrica_fields_namespaces_enum = MetrikaApiCounterSource

    fields_namespace = None
    accuracy = None

    def __init__(self, oauth_token, fields_namespace=None, accuracy=None, **client_kwargs):
        client_kwargs.setdefault("host", self.metrica_host)
        self._cli = MetrikaApiClient(oauth_token, **client_kwargs)
        if fields_namespace:
            if not hasattr(self.metrica_fields_namespaces_enum, fields_namespace):
                raise MetrikaApiException("Unknown fields namespace: %s" % fields_namespace)
            self.fields_namespace = self.metrica_fields_namespaces_enum[fields_namespace]
        self.accuracy = accuracy

        self._connected = True

    @property
    def is_connected(self):
        return self._connected

    @check_connected
    def close(self):
        del self._cli
        self._cli = None
        self._connected = False

    @check_connected
    def commit(self):
        pass

    @check_connected
    def cursor(self):
        return Cursor(api_client=self._cli, connection=self)

    @check_connected
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

    @check_connected
    def get_avail_date_min(self, counter_id):
        return self._cli.get_counter_creation_date(counter_id)


def connect(oauth_token=None, **kwargs):
    oauth_token = oauth_token or kwargs.get("password")
    fields_namespace = kwargs.get("database")
    accuracy = kwargs.get("accuracy")
    return Connection(oauth_token=oauth_token, fields_namespace=fields_namespace, accuracy=accuracy)  # , **kwargs)


def check_cursor_connected(func):
    @wraps(func)
    def func_wrapper(self, *args, **kwargs):
        if not self._connected:
            raise CursorClosedException("Cursor object is closed")
        elif not self.connection.is_connected:
            raise ConnectionClosedException("Connection object is closed")
        else:
            return func(self, *args, **kwargs)

    return func_wrapper


class InternalCommands(Enum):
    get_columns = "__GET_COLUMNS_COMMAND__"
    get_tables = "__GET_TABLES_COMMAND__"
    get_avail_date_min = "__GET_AVAILABLE_DATE_MIN__"
    get_avail_date_max = "__GET_AVAILABLE_DATE_MAX__"


class Cursor(object):
    description = None
    rowcount = -1
    arraysize = 1
    _result_data = None

    def __init__(self, api_client: MetrikaApiClient, connection: Connection):
        self._cli = api_client
        self.connection = connection
        self._connected = True

    @property
    def is_connected(self):
        return self._connected

    @check_cursor_connected
    def close(self):
        self._connected = False

    def _prepare_query_params(self, query, subst_params) -> dict:
        query_params = parse_qs(query)

        if subst_params:
            for k, v in query_params.items():
                if len(v) != 1:
                    raise ProgrammingError("Unexpected multiple parameter %s values %s" % (k, v))
                v = v[0] % subst_params
                query_params[k] = v

        return query_params

    def _exec_get_columns(self, operation, parameters):
        res = self.connection.get_columns()
        self._result_data = res["data"]
        self.rowcount = len(self._result_data)
        self.description = [(f_name, VARCHAR, None, None, None, None, None) for f_name in res["fields"]]

    def _exec_get_tables(self, operation, parameters):
        table_names = self.connection.get_table_names()
        self._result_data = [(tn,) for tn in table_names]
        self.rowcount = len(self._result_data)
        self.description = [
            ("name", VARCHAR, None, None, None, None, None),
        ]

    def _exec_get_avail_date_min(self, operation, parameters):
        counter_id = parameters.get("_COUNTER_ID_")
        date_min = self.connection.get_avail_date_min(counter_id)
        self._result_data = [(date_min.isoformat(),)]
        self.rowcount = 1
        result_columns = parameters.get("__RESULT_COLUMNS__", [])
        if result_columns:
            col_name = result_columns[0]["label"] or result_columns[0]["name"]
        else:
            col_name = "date_min"
        self.description = [
            (col_name, DATE, None, None, None, None, None),
        ]

    def _exec_get_avail_date_max(self, operation, parameters):
        # TODO: use counter timezone
        today = date.today()
        self._result_data = [(today.isoformat(),)]
        self.rowcount = 1
        result_columns = parameters.pop("__RESULT_COLUMNS__", [])
        if result_columns:
            col_name = result_columns[0]["label"] or result_columns[0]["name"]
        else:
            col_name = "date_max"
        self.description = [
            (col_name, DATE, None, None, None, None, None),
        ]

    def _meth_by_operation(self, operation):
        meth = {
            InternalCommands.get_columns.value: self._exec_get_columns,
            InternalCommands.get_tables.value: self._exec_get_tables,
            InternalCommands.get_avail_date_min.value: self._exec_get_avail_date_min,
            InternalCommands.get_avail_date_max.value: self._exec_get_avail_date_max,
        }.get(operation)
        return meth

    @check_cursor_connected
    def execute(self, operation, parameters=None):
        self.rowcount = None
        self._result_data = None
        self.description = None

        meth = self._meth_by_operation(operation) or self._execute_select_data
        return meth(operation, parameters)

    def _execute_select_data(self, operation, parameters):
        casts = parameters.pop("__CASTS__", None)
        result_columns = parameters.pop("__RESULT_COLUMNS__", None)
        for col in result_columns:
            col_name = col["name"]
            if casts and col_name in casts:
                col["cast_processor"] = cast_processors[casts[col_name]]
            elif col_name in fields_by_name and fields_by_name[col_name]["type"] in cast_processors_for_metrika_types:
                col["cast_processor"] = cast_processors_for_metrika_types[fields_by_name[col_name]["type"]]

        query_params = self._prepare_query_params(operation, parameters)

        if self.connection.accuracy is not None:
            query_params.update(accuracy=self.connection.accuracy)

        result = self._cli.get_table_data(query_params, result_columns=result_columns)

        self._result_data = result["data"]
        self.rowcount = len(self._result_data)

        self.description = []
        for col in result["fields"]:
            if casts and col["name"] in casts:
                col_type = cast_type_to_sqla_type[casts[col["name"]]]
            elif col["name"] not in fields_by_name:
                col_type = VARCHAR
            else:
                col_type = metrika_types_to_sqla.get(fields_by_name[col["name"]]["type"], NULLTYPE)
            self.description.append((col["label"] or col["name"], col_type, None, None, None, None, None))

    # def executemany(self, sql: str, seq_of_parameters: Iterable[Iterable]): ...

    @check_cursor_connected
    def fetchone(self):
        try:
            if self._result_data:
                return self._result_data.pop(0)
            else:
                return None
        except StopIteration:
            return None

    @check_cursor_connected
    def fetchmany(self, size=None):
        if size is None or size > len(self._result_data):
            size = len(self._result_data)
        rows = self._result_data[:size]
        self._result_data = self._result_data[size:]
        return rows

    @check_cursor_connected
    def fetchall(self):
        rows = self._result_data
        self._result_data = []
        return rows

    def __iter__(self):
        raise NotImplementedError()

    def setinputsizes(self, *args, **kwargs):
        pass

    def setoutputsize(self, *args, **kwargs):
        pass
