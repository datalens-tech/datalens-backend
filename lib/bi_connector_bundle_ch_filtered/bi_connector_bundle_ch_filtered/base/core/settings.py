import attr

import json

from bi_constants.enums import RawSQLLevel

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.settings_loaders.meta_definition import s_attrib


def normalize_sql_query(sql_query: str) -> str:
    # Only normalize newlines for now:
    sql_query = '\n{}\n'.format(sql_query.strip('\n'))
    return sql_query


def _subselect_templates_env_var_converter(v: str) -> tuple[dict[str, str], ...]:
    templates: tuple[dict[str, str], ...] = tuple(json.loads(v))
    for tpl in templates:
        tpl_keys = set(tpl.keys())
        assert tpl_keys == {'sql_query', 'title'}, f'Unexpected keys for a subselect template: {tpl_keys}'
        tpl['sql_query'] = normalize_sql_query(tpl['sql_query'])

    return templates


@attr.s(frozen=True)
class FrozenConnectorSettingsBase(ConnectorSettingsBase):
    ALLOW_PUBLIC_USAGE: bool = True

    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore
    USERNAME: str = s_attrib("USERNAME")  # type: ignore
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore
    USE_MANAGED_NETWORK: bool = s_attrib("USE_MANAGED_NETWORK")  # type: ignore
    ALLOWED_TABLES: list[str] = s_attrib(  # type: ignore
        "ALLOWED_TABLES",
        env_var_converter=json.loads,
    )
    SUBSELECT_TEMPLATES: tuple[dict[str, str], ...] = s_attrib(  # type: ignore
        "SUBSELECT_TEMPLATES",
        env_var_converter=_subselect_templates_env_var_converter,
    )


@attr.s(frozen=True)
class CHFrozenConnectorSettings(FrozenConnectorSettingsBase):
    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore
    DB_NAME: str = s_attrib("DB_NAME")  # type: ignore
    RAW_SQL_LEVEL: RawSQLLevel = s_attrib("RAW_SQL_LEVEL", missing=RawSQLLevel.off, env_var_converter=RawSQLLevel)  # type: ignore
    PASS_DB_QUERY_TO_USER: bool = s_attrib("PASS_DB_QUERY_TO_USER", missing=False)  # type: ignore


@attr.s(frozen=True)
class ServiceConnectorSettingsBase(CHFrozenConnectorSettings):
    """"""
