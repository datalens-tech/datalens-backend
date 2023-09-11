from __future__ import annotations

import abc


class ConnectorsDataBase(metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def connector_name(cls) -> str:
        """
        TODO: BI-4359 just remove it after migration to new scheme
        """
        raise NotImplementedError


def normalize_sql_query(sql_query: str) -> str:
    # Only normalize newlines for now:
    sql_query = '\n{}\n'.format(sql_query.strip('\n'))
    return sql_query
