from __future__ import annotations

DASHSQL_EXAMPLE_PARAMS = {
    "some_string": {"type_name": "string", "value": "some\\:string\nwith\\stuff"},
    "some_integer": {"type_name": "integer", "value": "562949953421312"},
    "some_float": {"type_name": "float", "value": "73786976294838206464.5"},
    "some_boolean": {"type_name": "boolean", "value": "true"},
    "some_other_boolean": {"type_name": "boolean", "value": "false"},
    "some_date": {"type_name": "date", "value": "2021-07-19"},
    "some_datetime": {"type_name": "datetime", "value": "2021-07-19T19:35:43"},
    "3xtr4 ше1гd param": {"type_name": "string", "value": "11"},
    "3xtr4 же1гd param": {"type_name": "string", "value": "22"},
    "intvalues": {"type_name": "integer", "value": ["1", "2", "3"]},
    "strvalues": {"type_name": "string", "value": ["a", "b", "c"]},
}
