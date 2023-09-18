from __future__ import annotations

import sqlalchemy as sa
from ydb.sqlalchemy import YqlDialect as UPSTREAM


class BIYQDialect(UPSTREAM):
    name = "bi_yq"
    driver = "bi_yq"

    @staticmethod
    def dbapi():
        from bi_sqlalchemy_yq import dbapi

        return dbapi

    def _check_unicode_returns(self, *args, **kwargs):
        return "conditional"

    def get_columns(self, connection, table_name, schema=None, **kw):
        raise Exception("Not Implemented")

    def has_table(self, connection, table_name, schema=None):
        raise Exception("Not Implemented")


def register_dialect():
    sa.dialects.registry.register("bi_yq", "bi_sqlalchemy_yq.base", "BIYQDialect")
