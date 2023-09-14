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
    AUTHORIZATION_TOKEN = "Authorization"
    REQUEST_ID = "X-Request-ID"
    PUBLIC_API_KEY = "X-DL-Back-Public-API-Token"
    FILE_UPLOADER_MASTER_TOKEN = "X-DL-File-Uploader-Master-Token"
    TENANT_ID = "X-DL-TenantId"  # Tenant in the UnitedStorage format.
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

    # TODO: BI-4918 drop after all usages moved to bi_api_commons_ya_cloud.constants.DLHeadersYC
    IAM_TOKEN = "X-YaCloud-SubjectToken"
    FOLDER_ID = "X-YaCloud-FolderId"
    ORG_ID = "X-YaCloud-OrgId"


class DLCookiesCommon(DLCookies):
    ...
