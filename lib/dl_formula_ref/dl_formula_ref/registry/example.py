from __future__ import annotations

import attr

from dl_formula_ref.examples.config import (
    ExampleConfig,
    PredefinedExampleConfig,
)
from dl_formula_ref.examples.renderer import ExampleRenderer
from dl_formula_ref.registry.example_base import (
    DataExampleRendererConfig,
    ExampleBase,
)


@attr.s(frozen=True)
class SimpleExample(ExampleBase):
    _text: str = attr.ib()

    def render(self, func_name: str, locale: str, config: DataExampleRendererConfig, under_cut: bool) -> str:
        return "```\n" f"{self._text.strip()}\n" "```"


@attr.s(frozen=True)
class DataExampleBase(ExampleBase):
    _example_config: ExampleConfig = attr.ib(kw_only=True)

    @property
    def example_config(self) -> ExampleConfig:
        return self._example_config

    def _get_renderer(self, locale: str, config: DataExampleRendererConfig) -> ExampleRenderer:
        return ExampleRenderer(
            jinja_env=config.jinja_env,
            example_template_filename=config.template_filename,
            locale=locale,
            storage_filename=config.example_data_filename,
        )


@attr.s(frozen=True)
class DataExample(DataExampleBase):
    @property
    def example_config(self) -> ExampleConfig:
        return self._example_config

    def render(self, func_name: str, locale: str, config: DataExampleRendererConfig, under_cut: bool) -> str:
        example_renderer = self._get_renderer(locale=locale, config=config)
        text = example_renderer.render_example_from_storage(
            example=self._example_config,
            func_name=func_name,
            under_cut=under_cut,
        )
        return text


@attr.s(frozen=True)
class PredefinedDataExample(DataExampleBase):
    _example_config: PredefinedExampleConfig = attr.ib(kw_only=True)

    def render(self, func_name: str, locale: str, config: DataExampleRendererConfig, under_cut: bool) -> str:
        example_renderer = self._get_renderer(locale=locale, config=config)
        text = example_renderer.render_example(
            example=self._example_config,
            result_table=self._example_config.result_table,
            under_cut=under_cut,
        )
        return text
