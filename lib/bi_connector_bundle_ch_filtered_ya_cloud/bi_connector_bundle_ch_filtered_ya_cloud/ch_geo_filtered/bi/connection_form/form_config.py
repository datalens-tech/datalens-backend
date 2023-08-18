from __future__ import annotations

import abc
from typing import Optional

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.bi.connection_info import (
    CHGeoFilteredConnectionInfoProvider,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import Translatable


class CHGeoFilteredFormFactory(ConnectionFormFactory, metaclass=abc.ABCMeta):
    def get_form_config(
            self,
            connectors_settings: Optional[ConnectorsSettingsByType],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        return ConnectionForm(
            title=CHGeoFilteredConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CustomizableRow(items=[
                    C.DescriptionRowItem(
                        text=self._localizer.translate(Translatable('label_ch-geo-filtered-description'))
                    ),
                ]),
            ],
        )
