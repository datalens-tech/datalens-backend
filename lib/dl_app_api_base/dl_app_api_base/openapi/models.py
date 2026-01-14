import collections
from typing import Sequence

import attrs

from dl_app_api_base.openapi.protocols import OpenApiRouteProtocol
import dl_pydantic


@attrs.define(frozen=True, kw_only=True)
class OpenApiSpec:
    @attrs.define(frozen=True, kw_only=True)
    class Info:
        title: str
        description: str

    @attrs.define(frozen=True, kw_only=True)
    class Tag:
        name: str
        description: str

    openapi: str = "3.0.1"
    info: Info | None = None
    tags: list[Tag] = attrs.field(factory=list)
    routes: Sequence[OpenApiRouteProtocol] = attrs.field(factory=list)

    @property
    def raw(self) -> dict:
        result: dict = {}

        result["openapi"] = self.openapi
        result["tags"] = [attrs.asdict(tag) for tag in self.tags]
        if self.info:
            result["info"] = attrs.asdict(self.info)

        defs: dict = {}

        paths: dict = collections.defaultdict(lambda: collections.defaultdict(dict))
        for route in self.routes:
            if not route.handler.OPENAPI_INCLUDE:
                continue

            request_schema = route.handler._request_schema
            required_fields = {"path", "query", "body"}
            actual_fields = set(request_schema.model_fields.keys())
            if not required_fields <= actual_fields:
                raise TypeError(
                    f"RequestSchema must contain path, query, body fields. "
                    f"Missing: {required_fields - actual_fields}, got: {actual_fields}"
                )

            responses: dict = {}

            parameters: list[dict] = []

            path_type = request_schema.model_fields["path"].annotation
            assert path_type is not None and issubclass(path_type, dl_pydantic.BaseModel)
            path_schema = path_type.model_json_schema()
            for property_name, property_schema in path_schema.get("properties", {}).items():
                parameters.append(
                    {
                        "name": property_name,
                        "in": "path",
                        "required": "default" not in property_schema,
                        "schema": property_schema,
                    }
                )

            query_type = request_schema.model_fields["query"].annotation
            assert query_type is not None and issubclass(query_type, dl_pydantic.BaseModel)
            query_schema = query_type.model_json_schema()
            for property_name, property_schema in query_schema.get("properties", {}).items():
                parameters.append(
                    {
                        "name": property_name,
                        "in": "query",
                        "required": "default" not in property_schema,
                        "schema": property_schema,
                    }
                )

            body_type = request_schema.model_fields["body"].annotation
            assert body_type is not None and issubclass(body_type, dl_pydantic.BaseModel)
            body_schema = body_type.model_json_schema()
            if len(body_schema.get("properties", {})) > 0:
                parameters.append(
                    {
                        "name": "body",
                        "in": "body",
                        "required": True,
                        "schema": body_schema,
                    }
                )

            for status, response_schema in route.handler._response_schemas.items():
                response_schema_dict = response_schema.model_json_schema()

                for key, value in response_schema_dict.get("$defs", {}).items():
                    defs[key] = value

                responses[str(status.value)] = {
                    "content": {
                        "application/json": {"schema": response_schema.model_json_schema()},
                    },
                }
            paths[route.path][route.method.lower()] = {
                "tags": route.handler.OPENAPI_TAGS,
                "summary": route.handler.OPENAPI_DESCRIPTION,
                "parameters": parameters,
                "responses": responses,
            }

        result["paths"] = paths
        result["$defs"] = defs

        return result
