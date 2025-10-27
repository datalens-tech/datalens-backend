from __future__ import annotations

from aiohttp import web

from dl_api_lib.api_common.data_serialization import get_fields_data_serializable
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView


# TODO FIX: Generalize with sync version
class DatasetFieldsView(DatasetDataBaseView):
    endpoint_code = "DatasetFieldsGet"

    @DatasetDataBaseView.with_resolved_entities
    async def get(self) -> web.Response:
        # Pass dataset_id to US from URL
        self.dl_request.us_manager.set_dataset_context(self.dataset_id)

        fields = get_fields_data_serializable(self.dataset)
        return web.json_response(dict(fields=fields, revision_id=self.dataset.revision_id))
