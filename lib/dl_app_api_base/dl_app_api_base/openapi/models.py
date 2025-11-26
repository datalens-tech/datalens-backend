import collections

import attrs

import dl_app_api_base.handlers.base as handlers_base


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
    routes: list[handlers_base.Route] = attrs.field(factory=list)

    @property
    def raw(self) -> dict:
        result: dict = {}

        result["openapi"] = self.openapi
        result["tags"] = [attrs.asdict(tag) for tag in self.tags]
        if self.info:
            result["info"] = attrs.asdict(self.info)

        paths: dict = collections.defaultdict(lambda: collections.defaultdict(dict))
        for route in self.routes:
            responses: dict = {}
            for status, response_schema in route.handler._response_schemas.items():
                responses[str(status.value)] = {
                    "content": {
                        "application/json": {"schema": response_schema.model_json_schema()},
                    },
                }
            paths[route.path][route.method.lower()] = {
                "tags": route.handler.TAGS,
                "summary": route.handler.DESCRIPTION,
                "responses": responses,
            }

        result["paths"] = paths

        return result
