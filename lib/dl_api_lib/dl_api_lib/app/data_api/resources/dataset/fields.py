from aiohttp import web

from dl_api_lib.api_common.data_serialization import get_fields_data_serializable
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from dl_api_lib.schemas.data import DatasetFieldsQuerySchema


# TODO FIX: Generalize with sync version
class DatasetFieldsView(DatasetDataBaseView):
    endpoint_code = "DatasetFieldsGet"

    @DatasetDataBaseView.with_dataset_us_context
    @DatasetDataBaseView.with_resolved_entities
    async def get(self) -> web.Response:
        query_params = DatasetFieldsQuerySchema().load(dict(self.request.query))
        fields = get_fields_data_serializable(
            self.dataset,
            for_result=False,
            include_details=query_params["include_details"],
        )
        return web.json_response({"fields": fields, "revision_id": self.dataset.revision_id})
