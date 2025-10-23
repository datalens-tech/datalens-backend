import orjson

import dl_json.types as json_types


dumps_bytes = orjson.dumps
loads_bytes = orjson.loads


def dumps_str(obj: json_types.JsonSerializable) -> str:
    return dumps_bytes(obj).decode("utf-8")


def loads_str(s: str) -> json_types.JsonSerializable:
    return loads_bytes(s.encode("utf-8"))
