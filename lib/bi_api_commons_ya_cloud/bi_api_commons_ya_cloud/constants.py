import enum

from bi_constants.api_constants import DLHeaders


class DLHeadersYC(DLHeaders):
    IAM_TOKEN = "X-YaCloud-SubjectToken"
    FOLDER_ID = "X-YaCloud-FolderId"
    ORG_ID = "X-YaCloud-OrgId"


class YcTokenHeaderMode(enum.Enum):
    INTERNAL = enum.auto()
    EXTERNAL = enum.auto()
    UNIVERSAL = enum.auto()  # TODO: delete me some day
