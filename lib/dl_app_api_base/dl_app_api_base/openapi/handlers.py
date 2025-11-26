import copy
import os
from typing import Any

import aiohttp.web
import attrs
import jinja2
from typing_extensions import Self

import dl_app_api_base.handlers.base as base
import dl_app_api_base.handlers.responses as responses


SWAGGER_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "swagger/templates/")
SWAGGER_STATIC_DIR = os.path.join(os.path.dirname(__file__), "swagger/static/")

SWAGGER_UI_BUNDLE_PARAMETERS = {
    "dom_id": '"#swagger-ui"',
    "deepLinking": "true",
    "displayRequestDuration": "true",
    "layout": '"StandaloneLayout"',
    "plugins": "[SwaggerUIBundle.plugins.DownloadUrl]",
    "presets": "[SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset]",
}


@attrs.define(frozen=True, kw_only=True)
class OpenApiHandler(base.BaseHandler):
    raw_spec: dict[str, Any]

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        return responses.Response.with_data(self.raw_spec)


@attrs.define(frozen=True, kw_only=True)
class SwaggerHandlerDependencies:
    url_prefix: str
    config_url: str


@attrs.define(frozen=True, kw_only=True)
class SwaggerHandler(base.BaseHandler):
    doc_html: str

    @classmethod
    def from_dependencies(cls, dependencies: SwaggerHandlerDependencies) -> Self:
        environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(SWAGGER_TEMPLATE_DIR),
            autoescape=jinja2.select_autoescape(["html"]),
        )
        template = environment.get_template("doc.html")

        url_prefix = dependencies.url_prefix.rstrip("/")

        parameters = copy.deepcopy(SWAGGER_UI_BUNDLE_PARAMETERS)
        parameters["url"] = f'"{url_prefix}{dependencies.config_url}"'

        doc_html = template.render(
            url_prefix=url_prefix,
            parameters=parameters,
        )

        return cls(doc_html=doc_html)

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        return aiohttp.web.Response(text=self.doc_html, content_type="text/html")

    @property
    def static_dir(self) -> str:
        return SWAGGER_STATIC_DIR
