from __future__ import annotations

from typing import ClassVar, Optional, Type

import os

from bi_configs import environments


ENV_NAME = os.environ.get("DLS_ENV") or os.environ.get("YENV_TYPE") or "development"


class CommonSettings:
    ENV_NAME: ClassVar[str] = ENV_NAME
    ENV_CONFIG: ClassVar[Optional[environments.IAMAwareInstallation]] = None
    HOST: ClassVar[Optional[str]] = None

    # secrets
    API_KEY: ClassVar[Optional[str]] = None
    DB_PASSWORD: ClassVar[Optional[str]] = None
    DB_SLAVE_PASSWORD: ClassVar[Optional[str]] = None
    SENTRY_DSN: ClassVar[Optional[str]] = None
    STAFF_TOKEN: ClassVar[Optional[str]] = None
    YC_KEY_DATA_B: ClassVar[Optional[str]] = None

    # database config
    DB_HOST: ClassVar[Optional[str]] = None
    DB_PORT: ClassVar[int] = 5432
    DB_USER: ClassVar[str] = "yadls"
    DB_NAME: ClassVar[Optional[str]] = None
    DB_SLAVE_HOST: ClassVar[Optional[str]] = None
    DB_SLAVE_PORT: ClassVar[Optional[int]] = None
    DB_SLAVE_USER: ClassVar[Optional[str]] = None
    DB_SLAVE_NAME: ClassVar[Optional[str]] = None
    # management-convenience values
    _DB_CID: ClassVar[Optional[str]] = None
    _DB_FACE: ClassVar[Optional[str]] = None

    # ...
    YC_IAM_LEGACY_HOST: ClassVar[Optional[str]] = None

    _SOLOMON_FACE: ClassVar[Optional[str]] = None
    SOLOMON_HOST: ClassVar[Optional[str]] = None
    SOLOMON_PROJECT: ClassVar[Optional[str]] = None
    SOLOMON_CLUSTER: ClassVar[Optional[str]] = None
    SOLOMON_SERVICE: ClassVar[Optional[str]] = None
    SOLOMON_OAUTH: ClassVar[Optional[str]] = None

    # WARNING: duplicated in some entry points
    HTTP_PORT: ClassVar[int] = 80
    API_PREFIX: ClassVar[str] = "/_dls"

    JSON_LOG: ClassVar[bool] = True
    ERRFILE_LOG: ClassVar[bool] = True

    DBG_DBSHELL: ClassVar[bool] = False

    REQUIRE_TENANT_FILTER: ClassVar[bool] = False
    USE_CLOUD_SUGGEST: ClassVar[bool] = False

    GRANT_REQUEST_TTL_DAYS: ClassVar[Optional[int]] = 360


class DevelopmentSettings(CommonSettings):
    API_KEY: ClassVar[str] = ""  # NOTE: means "not required"
    DB_PASSWORD: ClassVar[str] = "yadls"
    DB_SLAVE_PASSWORD: ClassVar[str] = "yadls"

    DB_HOST: ClassVar[str] = "localhost"
    DB_PORT: ClassVar[int] = 5432
    DB_USER: ClassVar[str] = "yadls"
    DB_NAME: ClassVar[str] = "yadls"

    HTTP_PORT: ClassVar[int] = 38080
    GRANT_REQUEST_TTL_DAYS: ClassVar[Optional[int]] = 3


class CommonYateamSettings(CommonSettings):
    SOLOMON_HOST: ClassVar[Optional[str]] = "https://solomon.yandex.net"
    SOLOMON_PROJECT: ClassVar[Optional[str]] = "datalens"


class IntBetaSettings(CommonYateamSettings):
    ENV_CONFIG: ClassVar[Optional[environments.IAMAwareInstallation]] = environments.InternalTestingInstallation  # type: ignore  # TODO: fix
    _DB_FACE: ClassVar[Optional[str]] = "https://yc.yandex-team.ru/folders/foo867ak5sdnsd3f9a56/managed-postgresql/cluster/6db4cc46-3f10-4532-b097-21bc56777867"
    _DB_CID: ClassVar[Optional[str]] = "6db4cc46-3f10-4532-b097-21bc56777867"
    # DB_HOST = "myt-69ril7s9l841dgtt.db.yandex.net"
    # DB_HOST = "myt-69ril7s9l841dgtt.db.yandex.net,sas-l38uznqhz83dkrkx.db.yandex.net,iva-2x0kz6va6tcgwccm.db.yandex.net"
    # # backend containers: myt, sas
    DB_HOST: ClassVar[str] = "myt-69ril7s9l841dgtt.db.yandex.net,sas-l38uznqhz83dkrkx.db.yandex.net,vla-gt1tumoaoqy3bee1.db.yandex.net"
    DB_PORT: ClassVar[int] = 6432
    DB_USER: ClassVar[str] = "yadls"
    DB_NAME: ClassVar[str] = "dls_int_beta_db"
    # HOST="https://dls-beta.…/_dls/",

    _SOLOMON_FACE: ClassVar[Optional[str]] = "https://solomon.yandex-team.ru/admin/projects/datalens/shards/datalens_preprod_back_preprod_back_dls"
    SOLOMON_CLUSTER: ClassVar[Optional[str]] = "preprod_back"
    SOLOMON_SERVICE: ClassVar[Optional[str]] = "preprod_back_dls"
    GRANT_REQUEST_TTL_DAYS: ClassVar[Optional[int]] = 7


class IntProdSettings(CommonYateamSettings):
    ENV_CONFIG: ClassVar[Optional[environments.IAMAwareInstallation]] = environments.InternalProductionInstallation  # type: ignore  # TODO: fix
    _DB_FACE: ClassVar[Optional[str]] = "https://yc.yandex-team.ru/folders/foo867ak5sdnsd3f9a56/managed-postgresql/cluster/4fbff674-1ac6-4645-8baf-e352679f430c"
    _DB_CID: ClassVar[Optional[str]] = "4fbff674-1ac6-4645-8baf-e352679f430c"
    # DB_HOST = "myt-x1lulo3qiyc816rx.db.yandex.net,sas-rz5x7qy34c1wf0kd.db.yandex.net,iva-seugymk8d3fqfv67.db.yandex.net"
    DB_HOST: ClassVar[str] = "myt-x1lulo3qiyc816rx.db.yandex.net,sas-rz5x7qy34c1wf0kd.db.yandex.net,vla-3rzoqv43b0n17kwe.db.yandex.net"
    DB_PORT: ClassVar[int] = 6432
    DB_USER: ClassVar[str] = "yadls"
    DB_NAME: ClassVar[str] = "dls_int_prod_db"
    # HOST="https://dls-prod.…/_dls/",

    _SOLOMON_FACE: ClassVar[Optional[str]] = "https://solomon.yandex-team.ru/admin/projects/datalens/shards/datalens_prod_back_prod_back_dls"
    SOLOMON_CLUSTER: ClassVar[Optional[str]] = "prod_back"
    SOLOMON_SERVICE: ClassVar[Optional[str]] = "prod_back_dls"
    GRANT_REQUEST_TTL_DAYS: ClassVar[Optional[int]] = 30


class CommonYacloudSettings(CommonSettings):
    ENV_CONFIG: ClassVar[environments.IAMAwareInstallation]
    REQUIRE_TENANT_FILTER: ClassVar[bool] = True
    YC_IAM_LEGACY_HOST: ClassVar[str]


class ExtTestingSettings(CommonYacloudSettings):
    USE_CLOUD_SUGGEST: ClassVar[bool] = True

    ENV_CONFIG: ClassVar[environments.IAMAwareInstallation] = environments.ExternalTestingInstallation  # type: ignore  # TODO: fix
    _DB_FACE: ClassVar[Optional[str]] = "https://console-preprod.cloud.yandex.ru/folders/aoevv1b69su5144mlro3/managed-postgresql/cluster/e4umie58fv4181a38gmi"
    _DB_CID: ClassVar[Optional[str]] = "e4umie58fv4181a38gmi"
    # # backend containers: myt, sas
    DB_HOST: ClassVar[str] = "rc1a-uiimx2yh6k3hlwpn.mdb.cloud-preprod.yandex.net,rc1b-kyze9j7xpmdwr5lq.mdb.cloud-preprod.yandex.net"
    DB_PORT: ClassVar[int] = 6432
    DB_USER: ClassVar[str] = "yadls"
    DB_NAME: ClassVar[str] = "dl_ext_dls_test_db"
    HOST: ClassVar[Optional[str]] = "https://datalens-dls.private-api.ycp.cloud-preprod.yandex.net/_dls/"
    YC_IAM_LEGACY_HOST: ClassVar[str] = "https://identity.private-api.cloud-preprod.yandex.net:14336"  # see https://st.yandex-team.ru/BI-2100

    _SOLOMON_FACE: ClassVar[Optional[str]] = "https://solomon.cloud-preprod.yandex-team.ru/admin/projects/aoee4gvsepbo0ah4i2j6/shards/aoee4gvsepbo0ah4i2j6_preprod_back_preprod_back_dls"
    SOLOMON_HOST: ClassVar[Optional[str]] = "https://solomon.cloud-preprod.yandex-team.ru"
    SOLOMON_PROJECT: ClassVar[Optional[str]] = "aoee4gvsepbo0ah4i2j6"
    SOLOMON_CLUSTER: ClassVar[Optional[str]] = "preprod_back"
    SOLOMON_SERVICE: ClassVar[Optional[str]] = "preprod_back_dls"
    GRANT_REQUEST_TTL_DAYS: ClassVar[Optional[int]] = 30


class ExtProductionSettings(CommonYacloudSettings):
    USE_CLOUD_SUGGEST: ClassVar[bool] = True

    ENV_CONFIG: ClassVar[environments.IAMAwareInstallation] = environments.ExternalProductionInstallation  # type: ignore  # TODO: fix
    _DB_FACE: ClassVar[Optional[str]] = "https://console.cloud.yandex.ru/folders/b1g77mbejmj4m6flq848/managed-postgresql/cluster/c9qbb32okfd1i9hfv9n6"
    _DB_CID: ClassVar[Optional[str]] = "dc4dba83-7de8-41d4-849e-a09aaf08a829"
    # DB_HOST = "iva-p3royikmbvncjq79.db.yandex.net,vla-v0dar09hkppros7x.db.yandex.net,myt-iejusqlko2dculfv.db.yandex.net"
    DB_HOST: ClassVar[str] = "rc1a-cu6f6trpqt56ev6f.mdb.yandexcloud.net,rc1b-mvu9yqhe0b3jfcc0.mdb.yandexcloud.net,rc1c-x9fomhvozcnrug7f.mdb.yandexcloud.net"
    DB_PORT: ClassVar[int] = 6432
    DB_USER: ClassVar[str] = "yadls"
    DB_NAME: ClassVar[str] = "dl_ext_dls_prod_db"
    HOST: ClassVar[Optional[str]] = "https://datalens-dls.private-api.ycp.cloud.yandex.net/_dls/"
    YC_IAM_LEGACY_HOST: ClassVar[str] = "https://identity.private-api.cloud.yandex.net:14336"  # see https://st.yandex-team.ru/BI-2100

    _SOLOMON_FACE: ClassVar[Optional[str]] = "https://solomon.cloud-prod.yandex-team.ru/admin/projects/b1g08s4su5tgce7cpeo5/shards/b1g08s4su5tgce7cpeo5_prod_back_prod_back_dls"
    SOLOMON_HOST: ClassVar[Optional[str]] = "https://solomon.cloud-prod.yandex-team.ru"
    SOLOMON_PROJECT: ClassVar[Optional[str]] = "b1g08s4su5tgce7cpeo5"
    SOLOMON_CLUSTER: ClassVar[Optional[str]] = "prod_back"
    SOLOMON_SERVICE: ClassVar[Optional[str]] = "prod_back_dls"
    GRANT_REQUEST_TTL_DAYS: ClassVar[Optional[int]] = 30


SETTINGS_CLS_BY_ENV_NAME = {
    "development": DevelopmentSettings,
    "int_beta": IntBetaSettings,
    "int_prod": IntProdSettings,
    "ext_testing": ExtTestingSettings,
    "ext_production": ExtProductionSettings,
}
SETTINGS_CLS_BASE: Type[CommonSettings] = SETTINGS_CLS_BY_ENV_NAME.get(ENV_NAME) or CommonSettings


def updated_from_env(settings_cls: Type[CommonSettings], prefix: Optional[str] = None) -> Type[CommonSettings]:
    if prefix is None:
        prefix = os.environ.get("DLS_ENV_PREFIX", "DLS_")
    _proc_functions = {
        int: lambda val: int(val) if val is not None else None,
        bool: lambda val: bool(val) if val is not None else None,
        str: lambda val: str(val) if val is not None else None,
        type(None): lambda val: str(val) if val is not None else None,
        type: lambda val: None,  # cannot override e.g. ENV_CONFIG
    }
    keys = [
        (key, _proc_functions[type(getattr(settings_cls, key))])
        for key in dir(settings_cls)
        if not key.startswith("__")
    ]
    # TODO: raise on unknown env keys too.
    new_data = {key: func(os.environ.get("{}{}".format(prefix, key))) for key, func in keys}
    new_data = {key: val for key, val in new_data.items() if val is not None}
    if not new_data:
        return settings_cls
    return type("EnvironmentSettings", (settings_cls,), new_data)


settings = updated_from_env(SETTINGS_CLS_BASE)


if settings.DB_HOST and "," in settings.DB_HOST:
    from statcommons.hosts import HostManager
    hostmgr = HostManager([
        (hostname, hostname.split("-", 1)[0])
        for hostname in settings.DB_HOST.split(",")])
    settings.DB_HOST = hostmgr.host_string


if os.environ.get("DLS_DB_HAX"):
    settings.DB_HOST = "c-{}.rw.db.yandex.net".format(settings._DB_CID)
