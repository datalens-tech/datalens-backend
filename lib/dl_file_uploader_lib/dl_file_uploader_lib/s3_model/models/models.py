from __future__ import annotations

from typing import Optional

import attr

from dl_file_uploader_lib.s3_model.base import S3Model


@attr.s(init=True, kw_only=True)
class S3DataSourcePreview(S3Model):
    ID_PREFIX = "df_preview"

    preview_data: list[list[Optional[str]]] = attr.ib()
