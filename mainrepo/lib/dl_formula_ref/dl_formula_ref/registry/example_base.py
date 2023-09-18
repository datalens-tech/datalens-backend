from __future__ import annotations

import abc

import attr
import jinja2

from dl_formula_ref.registry.scopes import SCOPES_DEFAULT


@attr.s
class DataExampleRendererConfig:
    jinja_env: jinja2.Environment = attr.ib(kw_only=True)
    template_filename: str = attr.ib(kw_only=True)
    example_data_filename: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ExampleBase(abc.ABC):
    scopes: int = attr.ib(kw_only=True, default=SCOPES_DEFAULT)

    @abc.abstractmethod
    def render(self, func_name: str, locale: str, config: DataExampleRendererConfig, under_cut: bool) -> str:
        raise NotImplementedError
