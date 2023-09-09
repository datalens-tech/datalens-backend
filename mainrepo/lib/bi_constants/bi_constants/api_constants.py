from __future__ import annotations

import enum


class DLHeaders(str, enum.Enum):
    """
    Base marker class for all headers enumeration.
    Should be empty because enums with members can not be extended by other members.
    """


class DLCookies(enum.Enum):
    """"""


class DLContextKey(str, enum.Enum):
    DASH_ID = "dashId"
    CHART_ID = "chartId"
    CHART_KIND = "chartKind"
    DASH_TAB_ID = "dashTabId"


class DLHeadersCommon(DLHeaders):
    IAM_TOKEN = "X-YaCloud-SubjectToken"
    AUTHORIZATION_TOKEN = "Authorization"
    FOLDER_ID = "X-YaCloud-FolderId"
    REQUEST_ID = "X-Request-ID"
    PUBLIC_API_KEY = "X-DL-Back-Public-API-Token"
    FILE_UPLOADER_MASTER_TOKEN = "X-DL-File-Uploader-Master-Token"
    TENANT_ID = "X-DL-TenantId"  # Tenant in the UnitedStorage format.
    ORG_ID = "X-YaCloud-OrgId"
    PROJECT_ID = "X-DC-ProjectId"
    FORWARDED_FOR = "X-Forwarded-For"
    CSRF_TOKEN = "X-CSRF-Token"
    CONTENT_TYPE = "Content-Type"
    ACCEPT_LANGUAGE = "Accept-Language"
    ORIGIN = "Origin"

    ACCESS_CTRL_REQ_HEADERS = "Access-Control-Request-Headers"
    ACCESS_CTRL_REQ_METH = "Access-Control-Request-Method"

    # US access-control
    SUDO = "X-DL-Sudo"
    ALLOW_SUPERUSER = "X-DL-Allow-Superuser"

    API_KEY = "X-DL-API-Key"

    DL_COMPONENT = "X-DL-Component"
    DL_CONTEXT = "X-DL-Context"
    US_MASTER_TOKEN = "X-US-Master-Token"
    US_PUBLIC_TOKEN = "X-US-Public-API-Token"

    EMBED_TOKEN = "X-DL-Embed-Token"


class DLCookiesCommon(DLCookies):
    YA_TEAM_SESSION_ID = "Session_id"
    YA_TEAM_SESSION_ID_2 = "sessionid2"
    YC_SESSION = "yc_session"


class YcTokenHeaderMode(enum.Enum):
    INTERNAL = enum.auto()
    EXTERNAL = enum.auto()
    UNIVERSAL = enum.auto()  # TODO: delete me some day
