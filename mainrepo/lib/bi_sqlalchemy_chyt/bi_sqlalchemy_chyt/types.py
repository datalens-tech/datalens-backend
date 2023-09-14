from clickhouse_sqlalchemy.types import Int


class YtBoolean(Int):
    __visit_name__ = "ytboolean"
