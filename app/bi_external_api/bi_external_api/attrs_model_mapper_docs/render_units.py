import abc
import os.path
from typing import (
    Any,
    ClassVar,
    Optional,
    Sequence,
    Type,
    Union,
)
from urllib.parse import urlparse

import attr
import yaml

from bi_external_api.attrs_model_mapper.utils import (
    Locale,
    MText,
)


@attr.s()
class RenderContext:
    headers_level: int = attr.ib()
    current_file: str = attr.ib()
    locale: Locale = attr.ib(kw_only=True)

    def with_current_file(self, current_file: str) -> "RenderContext":
        return attr.evolve(self, current_file=current_file)

    def resolve_rel_ref(self, ref: str) -> str:
        ref_url = urlparse(ref)
        if ref_url.scheme == "":
            assert ref_url.netloc == "" and ref_url.params == "" and ref_url.query == "", f"Unsupported ref: {ref_url}"

            rel_to_root_current_dir = os.path.dirname(self.current_file)
            rel_to_root_target_file_path = ref_url.path
            normalized_path = os.path.relpath(rel_to_root_target_file_path, rel_to_root_current_dir)

            return ref_url._replace(path=normalized_path).geturl()

        # Not a local relative url
        return ref

    def localized_m_text(self, m_text: Optional[MText]) -> Optional[str]:
        if m_text is None:
            return None
        return m_text.at_locale(self.locale)

    def localized_m_text_strict(self, m_text: Optional[MText]) -> str:
        if m_text is None:
            raise ValueError("No m_text provided for strict localized text getter")
        ret = m_text.at_locale(self.locale)
        if ret is None:
            raise ValueError(f"m_text has no locale {self.locale!r}: {m_text}")
        return ret

    def localize(self, src: Union[str, MText, Sequence[Union[str, MText]]]) -> str:
        if isinstance(src, str):
            return src
        elif isinstance(src, MText):
            return self.localized_m_text_strict(src)
        else:
            return "".join([self.localize(sub_src) for sub_src in src])


class DocUnit(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def render_md(self, context: RenderContext) -> Sequence[str]:
        raise NotImplementedError()


@attr.s()
class EmptyLine(DocUnit):
    def render_md(self, context: RenderContext) -> Sequence[str]:
        return [""]


@attr.s()
class DocHeader(DocUnit):
    value: Union[str, MText, Sequence[Union[str, MText]]] = attr.ib()

    def render_md(self, render_ctx: RenderContext) -> Sequence[str]:
        ret = []
        if render_ctx.headers_level > 1:
            ret.append("")
        ret.append(f"{'#' * render_ctx.headers_level} {render_ctx.localize(self.value)}\n")

        return ret


@attr.s()
class DocLink(DocUnit):
    text: Union[str, MText, Sequence[Union[str, MText]]] = attr.ib()
    href: str = attr.ib()

    def render_as_single_str(self, render_ctx: RenderContext) -> str:
        rendered_link_text = render_ctx.localize(self.text)
        assert "\n" not in rendered_link_text

        return f"[{rendered_link_text}]({render_ctx.resolve_rel_ref(self.href)})"

    def render_md(self, render_ctx: RenderContext) -> Sequence[str]:
        return [self.render_as_single_str(render_ctx)]


@attr.s()
class DocText(DocUnit):
    text: Union[str, MText, DocLink, Sequence[Union[str, DocLink, MText]]] = attr.ib()

    def normalize_text_to_sequence(self) -> Sequence[Union[str, DocLink, MText]]:
        txt = self.text
        if isinstance(
            txt,
            (
                str,
                MText,
                DocLink,
            ),
        ):
            return [txt]
        return txt

    def render_md(self, render_ctx: RenderContext) -> Sequence[str]:
        text_part_list = self.normalize_text_to_sequence()

        # Adopting links to plain text
        plain_text_part_list: list[Union[str, MText]] = [
            text_part.render_as_single_str(render_ctx) if isinstance(text_part, DocLink) else text_part
            for text_part in text_part_list
        ]
        return [render_ctx.localize(plain_text_part_list)]


@attr.s()
class DocTableHeader(DocUnit):
    title_list: Sequence[MText] = attr.ib()

    def render_md(self, context: RenderContext) -> Sequence[str]:
        localized_title_list = [context.localized_m_text_strict(m_text) for m_text in self.title_list]

        return [
            "|".join([f" {title} " for title in localized_title_list]).rstrip(" "),
            "|".join(["-" * (len(title) + 2) for title in localized_title_list]),
        ]


class CompositeDocUnit(DocUnit, metaclass=abc.ABCMeta):
    add_headers_level: ClassVar[bool] = True

    @abc.abstractmethod
    def get_children(self) -> Sequence[Optional[DocUnit]]:
        raise NotImplementedError()

    def render_md(self, context: RenderContext) -> Sequence[str]:
        child_context = (
            attr.evolve(context, headers_level=context.headers_level + 1) if self.add_headers_level else context
        )

        ret: list[str] = []
        for child in self.get_children():
            if child is not None:
                ret.extend(child.render_md(child_context))

        return ret


@attr.s()
class DocUnitGroup(CompositeDocUnit):
    add_headers_level = False

    content: Sequence[Optional[DocUnit]] = attr.ib()

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        return self.content


@attr.s()
class DocSection(CompositeDocUnit):
    header: DocHeader = attr.ib()
    content: Sequence[Optional[DocUnit]] = attr.ib()

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        return [
            self.header,
            *self.content,
        ]


@attr.s()
class MultiLineTableRow(DocUnit):
    items: Sequence[Union[str, DocUnit]] = attr.ib()

    @staticmethod
    def convert_to_normalized_single_line_cells(cell_lines_list: Sequence[Sequence[str]]) -> Optional[Sequence[str]]:
        ret: list[str] = []
        for cell_lines in cell_lines_list:
            if len(cell_lines) > 1:
                return None
            elif len(cell_lines) == 1:
                cell_single_line = cell_lines[0]
                if "\n" in cell_single_line:
                    return None
                ret.append(cell_single_line)
            else:
                ret.append("")

        return ret

    def render_md(self, context: RenderContext) -> Sequence[str]:
        cells_line_list: list[Sequence[str]] = []

        for item in self.items:
            cell_content: Sequence[str]

            if isinstance(item, DocUnit):
                cell_content = item.render_md(context)
            else:
                cell_content = [item]
            cells_line_list.append(cell_content)

        may_be_single_line_cell_list = self.convert_to_normalized_single_line_cells(cells_line_list)

        # If all cells are single line - add
        if may_be_single_line_cell_list is not None:
            return ["|| " + " | ".join(may_be_single_line_cell_list) + " ||"]

        ret: list[str] = ["||"]

        for idx, cell_lines in enumerate(cells_line_list):
            if idx != 0:
                ret.append("|")
            ret.extend(cell_lines)

        ret.append("||")

        return ret


@attr.s()
class MultiLineTable(CompositeDocUnit):
    add_headers_level = False

    rows: Sequence[MultiLineTableRow] = attr.ib()

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        return [
            EmptyLine(),
            DocText("#|"),
            *self.rows,
            DocText("|#"),
        ]


@attr.s(auto_attribs=True)
class FieldLine(DocUnit):
    path: Sequence[str]

    type_text: str

    nullable: bool
    required: bool

    description: Optional[MText] = None
    type_ref: Optional[str] = None

    def _get_type_md(self, render_ctx: RenderContext) -> str:
        type_text: str
        if self.type_ref is not None:
            type_text = f"[{self.type_text}]({render_ctx.resolve_rel_ref(self.type_ref)})"
        else:
            type_text = f"**{self.type_text}**"

        return f"{type_text} {'*' if self.required else ''}"

    @classmethod
    def get_table_header(self) -> DocTableHeader:
        return DocTableHeader(
            [
                MText(ru="Поле", en="Field"),
                MText(ru="Тип", en="Type"),
                MText(ru="Описание", en="Description"),
            ]
        )

    def render_md(self, render_ctx: RenderContext) -> Sequence[str]:
        path = ".".join(f"`{part}`" for part in self.path)

        return [
            f"{path}"
            f" | {self._get_type_md(render_ctx)}"
            f" | {render_ctx.localized_m_text(self.description) or ''}".rstrip(" ").replace("\n", "<br>")
        ]


@attr.s()
class OperationExampleDoc(CompositeDocUnit):
    title: Union[MText, str] = attr.ib()
    description: Optional[MText] = attr.ib()
    rq: dict[str, Any] = attr.ib()
    rs: Optional[dict[str, Any]] = attr.ib()

    def _dump_dict(self, d: dict[str, Any]) -> str:
        yaml_text = yaml.safe_dump(d, default_flow_style=False, sort_keys=False)
        return f"```yaml\n{yaml_text}```"

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        rs_dict = self.rs

        return [
            DocHeader(["Example: ", self.title]),
            DocText(self.description) if self.description else None,
            EmptyLine(),
            DocText("**Request**"),
            EmptyLine(),
            DocText(self._dump_dict(self.rq)),
            EmptyLine(),
            DocText("**Response**"),
            EmptyLine(),
            DocText(self._dump_dict(rs_dict)) if rs_dict is not None else None,
        ]


@attr.s()
class ClassDoc(CompositeDocUnit, metaclass=abc.ABCMeta):
    type: Type = attr.ib()
    header: Optional[DocHeader] = attr.ib()
    description: Optional[MText] = attr.ib()


@attr.s()
class RegularClassDoc(ClassDoc):
    fields: Sequence[FieldLine] = attr.ib()
    discriminator_field_name: Optional[str] = attr.ib(default=None)
    discriminator_field_value: Optional[str] = attr.ib(default=None)

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        discriminator_text: Optional[DocText] = None
        if self.discriminator_field_value is not None:
            discriminator_text = DocText(f"`{self.discriminator_field_name}`:`{self.discriminator_field_value}`")

        description = self.description

        return [
            self.header,
            *([EmptyLine(), discriminator_text, EmptyLine()] if discriminator_text else []),
            *([EmptyLine(), DocText(description), EmptyLine()] if description else []),
            *([FieldLine.get_table_header(), *self.fields] if self.fields else [DocText("**No parameters**")]),
        ]


@attr.s()
class GenericClassDoc(ClassDoc):
    discriminator_field_name: str = attr.ib()
    map_discriminator_object_doc: dict[str, RegularClassDoc] = attr.ib()

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        return [
            self.header,
            DocText([MText("Поле-дискриминатор: ", en="Discriminator field: "), f"`{self.discriminator_field_name}`"]),
            DocText(self.description) if self.description else None,
            *[obj_doc for discriminator, obj_doc in self.map_discriminator_object_doc.items()],
        ]


@attr.s()
class OperationDoc(CompositeDocUnit):
    header: DocHeader = attr.ib()

    description: MText = attr.ib()

    request: RegularClassDoc = attr.ib()
    response: RegularClassDoc = attr.ib()
    examples: list[OperationExampleDoc] = attr.ib()

    def get_children(self) -> Sequence[Optional[DocUnit]]:
        return [
            self.header,
            DocText(self.description),
            DocSection(DocHeader("Request"), [self.request]),
            DocSection(
                DocHeader("Response"),
                [
                    self.response,
                ],
            ),
            DocSection(DocHeader("Examples"), self.examples) if self.examples else None,
        ]
