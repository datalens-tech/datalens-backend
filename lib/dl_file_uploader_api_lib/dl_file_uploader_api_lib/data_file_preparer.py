import logging
from typing import Optional
import urllib.parse

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.exc import InvalidLink
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import DataFile
from dl_file_uploader_lib.redis_model.models.models import (
    GSheetsUserSourceProperties,
    YaDocsUserSourceProperties,
)


LOGGER = logging.getLogger(__name__)


async def gsheets_data_file_preparer(
    url: str,
    redis_model_manager: RedisModelManager,
    token: Optional[str],
) -> DataFile:
    default_host: str = "docs.google.com"
    allowed_hosts: frozenset[str] = frozenset({default_host})
    example_url: str = (
        "https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit#gid=1140182768"
    )
    example_url_message: str = f"example: {example_url!r}"

    parts = urllib.parse.urlparse(url)
    if parts.scheme not in ("http", "https"):
        raise InvalidLink(f"Invalid url scheme: {parts.scheme!r}, must be 'http' or 'https'; {example_url_message}")
    netloc_pieces = parts.netloc.split(":", 1)
    host = netloc_pieces[0]
    if len(netloc_pieces) == 2:
        port = netloc_pieces[1]
        if port not in ("80", "443"):
            raise InvalidLink(f"Invalid port: {port!r}. Should be omitted or default; {example_url_message}")
    if host not in allowed_hosts:
        raise InvalidLink(f"Invalid host: {host!r}; should be {default_host!r}; {example_url_message}")

    path = parts.path
    prefix = "/spreadsheets/d/"
    if not path.startswith(prefix):
        raise InvalidLink(f"Invalid URL path prefix: {path!r}; should be {prefix!r}; {example_url_message}")
    path = path[len(prefix) :]

    suffix = "/edit"
    spreadsheet_id = path[: -len(suffix)] if path.endswith(suffix) else path

    df = DataFile(
        manager=redis_model_manager,
        filename="Spreadsheet",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(
            spreadsheet_id=spreadsheet_id,
            refresh_token=token,
        ),
    )

    return df


async def yadocs_data_file_preparer(
    oauth_token: Optional[str],
    private_path: Optional[str],
    public_link: Optional[str],
    redis_model_manager: RedisModelManager,
) -> DataFile:
    default_host: str = "disk.yandex.ru"
    allowed_hosts: frozenset[str] = frozenset({default_host})
    example_url: str = "https://disk.yandex.ru/i/OyzdmFI0MUEEgA"
    example_url_message: str = f"example: {example_url!r}"
    if public_link is not None:
        parts = urllib.parse.urlparse(public_link)
        if parts.scheme not in ("http", "https"):
            raise InvalidLink(f"Invalid url scheme: {parts.scheme!r}, must be 'http' or 'https'; {example_url_message}")
        host = parts.hostname
        if host not in allowed_hosts:
            raise InvalidLink(f"Invalid host: {host!r}; should be {default_host!r}; {example_url_message}")

        path = parts.path
        prefix = "/i/"
        if not path.startswith(prefix):
            raise InvalidLink(f"Invalid URL path prefix: {path!r}; should be {prefix!r}; {example_url_message}")

    df = DataFile(
        manager=redis_model_manager,
        filename="YaDocument",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.yadocs,
        user_source_properties=YaDocsUserSourceProperties(
            private_path=private_path, public_link=public_link, oauth_token=oauth_token
        ),
    )

    return df
