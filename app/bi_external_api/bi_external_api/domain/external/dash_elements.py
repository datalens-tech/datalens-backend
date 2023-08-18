import enum
from typing import Sequence, Optional, ClassVar, Any

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor, MapperBaseModel
from bi_external_api.domain.utils import ensure_tuple


class DashElementKind(enum.Enum):
    title = enum.auto()
    text = enum.auto()

    # widget_container = enum.auto()  # legacy
    charts_container = enum.auto()

    control_select = enum.auto()
    control_multiselect = enum.auto()
    control_date = enum.auto()
    control_date_range = enum.auto()
    control_external = enum.auto()
    control_text_input = enum.auto()


@ModelDescriptor(
    is_abstract=True,
    children_type_discriminator_attr_name="kind",
    children_type_discriminator_aliases_attr_name="kind_aliases",
)
@attr.s(frozen=True, kw_only=True)
class DashElement:
    """
    Abstract dash element
    """
    kind: ClassVar[DashElementKind]
    kind_aliases: ClassVar[tuple[str, ...]] = tuple()


class DashTitleTextSize(enum.Enum):
    xs = "xs"
    s = "s"
    m = "m"
    l = "l"


@ModelDescriptor()
@attr.s(frozen=True)
class DashTitle(DashElement):
    kind = DashElementKind.title

    text: str = attr.ib()
    size: DashTitleTextSize = attr.ib()
    show_in_toc: bool = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DashText(DashElement):
    kind = DashElementKind.text

    text: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class WidgetTab:
    id: str = attr.ib()
    chart_name: str = attr.ib()
    title: Optional[str] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True)
class DashChartsContainer(
    DashElement,
    MapperBaseModel,
):
    kind = DashElementKind.charts_container
    kind_aliases = ("widget_container",)

    hide_title: bool = attr.ib()
    tabs: Sequence[WidgetTab] = attr.ib(converter=ensure_tuple)

    default_active_chart_tab_id: Optional[str] = attr.ib(default=None)

    @classmethod
    def pre_load(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        if "default_widget_id" in data:
            # backward compat
            value = data.pop("default_widget_id", None)
            if not data.get("default_active_chart_tab_id") and value:
                data["default_active_chart_tab_id"] = value
                return data

        return None
