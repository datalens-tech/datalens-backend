from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_file_uploader_lib.redis_model.base import RedisModel


@attr.s(init=True, kw_only=True)
class DataSourcePreview(RedisModel):
    preview_data: list[list[Optional[str]]] = attr.ib()

    KEY_PREFIX: ClassVar[str] = "df_preview"
