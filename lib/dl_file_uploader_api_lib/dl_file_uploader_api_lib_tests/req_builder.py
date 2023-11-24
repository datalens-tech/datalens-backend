from typing import (
    Any,
    ClassVar,
    Optional,
)

import aiohttp

from dl_api_commons.client.common import Req
from dl_constants.api_constants import (
    DLHeaders,
    DLHeadersCommon,
)

from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


class ReqBuilder:
    ORIGIN: ClassVar[str] = "https://foo.bar"

    @classmethod
    def upload_csv(cls, csv_data: str) -> Req:
        upload_req_data = (
            f"--111111111111\n"
            f'Content-Disposition: form-data; name="file"; filename="sample.csv"\n'
            f"Content-Type: application/octet-stream\n"
            f"\n"
            f"{csv_data}"
        )

        return Req(
            method="post",
            url="/api/v2/files",
            extra_headers={
                DLHeadersCommon.CONTENT_TYPE: "multipart/form-data; boundary=111111111111",
                DLHeadersCommon.ORIGIN: cls.ORIGIN,
            },
            data=upload_req_data,
        )

    @classmethod
    def upload_xlsx(cls, xlsx_data: str) -> Req:
        with aiohttp.MultipartWriter() as mpwriter:
            part = mpwriter.append(xlsx_data)
            part.set_content_disposition("form-data", name="file", filename="data.xlsx")
            return Req(
                method="post",
                url="/api/v2/files",
                extra_headers={DLHeadersCommon.ORIGIN: cls.ORIGIN},
                data=mpwriter,
            )

    @classmethod
    def upload_gsheet(
        cls,
        spreadsheet_id: Optional[str] = None,
        url: Optional[str] = None,
        authorized: bool = False,
        *,
        require_ok: bool = True,
    ) -> Req:
        if (spreadsheet_id is None) ^ (url is not None):
            raise ValueError("Expected exactly one of [`spreadsheet_id`, `url`] to be specified")
        if url is None:
            url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

        return Req(
            method="post",
            url="/api/v2/links",
            data_json={
                "type": "gsheets",
                "url": url,
                "authorized": authorized,
            },
            require_ok=require_ok,
        )

    @classmethod
    def upload_documents(
        cls,
        public_link: Optional[str] = None,
        private_path: Optional[str] = None,
        oauth_token: Optional[str] = None,
        authorized: bool = False,
        *,
        require_ok: bool = True,
    ) -> Req:
        return Req(
            method="post",
            url="/api/v2/documents",
            data_json={
                "public_link": public_link,
                "private_path": private_path,
                "oauth_token": oauth_token,
                "authorized": authorized,
            },
            require_ok=require_ok,
        )

    @classmethod
    def file_status(cls, file_id: str) -> Req:
        return Req(
            method="get",
            url=f"/api/v2/files/{file_id}/status",
        )

    @classmethod
    def file_sources(cls, file_id, *, require_ok: bool = True) -> Req:
        return Req(method="get", url=f"/api/v2/files/{file_id}/sources", require_ok=require_ok)

    @classmethod
    def source_status(cls, file_id: str, source_id: str) -> Req:
        return Req(
            method="get",
            url=f"/api/v2/files/{file_id}/sources/{source_id}/status",
        )

    @classmethod
    def source_info(cls, file_id: str, source_id: str, data_json: Optional[dict[str, Any]] = None) -> Req:
        return Req(
            method="post",
            url=f"/api/v2/files/{file_id}/sources/{source_id}",
            data_json=data_json,
        )

    @classmethod
    def apply_source_settings(cls, file_id: str, source_id: str, data_json: Optional[dict[str, Any]] = None) -> Req:
        return Req(
            method="post",
            url=f"/api/v2/files/{file_id}/sources/{source_id}/apply_settings",
            data_json=data_json,
        )

    @classmethod
    def preview(
        cls,
        file_id: str,
        source_id: str,
        master_token_header: Optional[dict[DLHeaders, str]],
        data_json: Optional[dict[str, Any]] = None,
    ) -> Req:
        return Req(
            method="post",
            url=f"/api/v2/files/{file_id}/sources/{source_id}/preview",
            extra_headers=master_token_header,
            data_json=data_json,
        )

    @classmethod
    def internal_params(
        cls,
        file_id: str,
        source_id: str,
        master_token_header: Optional[dict[DLHeaders, str]],
        data_json: Optional[dict[str, Any]] = None,
        *,
        require_ok: bool = True,
    ) -> Req:
        return Req(
            method="post",
            url=f"/api/v2/files/{file_id}/sources/{source_id}/internal_params",
            extra_headers=master_token_header,
            data_json=data_json,
            require_ok=require_ok,
        )

    @classmethod
    def update_conn_data(
        cls,
        connection: GSheetsFileS3Connection | YaDocsFileS3Connection,
        save: bool,
        file_type: Optional[str] = "gsheets",
        *,
        require_ok: bool = True,
    ) -> Req:
        sources_desc = [src.get_desc() for src in connection.data.sources]
        sources = list()
        if file_type == "gsheets":
            sources = [
                dict(
                    id=src_desc.source_id,
                    title=src_desc.title,
                    spreadsheet_id=src_desc.spreadsheet_id,
                    sheet_id=src_desc.sheet_id,
                    first_line_is_header=src_desc.first_line_is_header,
                )
                for src_desc in sources_desc
            ]
        elif file_type == "yadocs":
            sources = [
                dict(
                    id=src_desc.source_id,
                    title=src_desc.title,
                    public_link=src_desc.public_link,
                    sheet_id=src_desc.sheet_id,
                    first_line_is_header=src_desc.first_line_is_header,
                )
                for src_desc in sources_desc
            ]
        return Req(
            method="post",
            url="/api/v2/update_connection_data",
            data_json={
                "connection_id": connection.uuid,
                "authorized": False,
                "save": save,
                "type": file_type,
                "sources": sources,
            },
            require_ok=require_ok,
        )

    @classmethod
    def cleanup(
        cls,
        tenant_id: str,
        master_token_header: Optional[dict[DLHeaders, str]] = None,
        *,
        require_ok: bool = True,
    ) -> Req:
        return Req(
            method="post",
            url="/api/v2/cleanup",
            extra_headers=master_token_header,
            data_json={
                "tenant_id": tenant_id,
            },
            require_ok=require_ok,
        )

    @classmethod
    def rename_tenant_files(cls, master_token_header: Optional[dict[DLHeaders, str]], tenant_id: str) -> Req:
        return Req(
            method="post",
            url="/api/v2/rename_tenant_files",
            extra_headers=master_token_header,
            data_json={
                "tenant_id": tenant_id,
            },
        )
