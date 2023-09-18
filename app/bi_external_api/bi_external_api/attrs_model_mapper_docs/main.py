import os
from typing import (
    Optional,
    Sequence,
    Type,
)

import attr
import yaml

from bi_external_api.attrs_model_mapper.domain import (
    AmmEnumField,
    AmmField,
    AmmGenericSchema,
    AmmListField,
    AmmNestedField,
    AmmRegularSchema,
    AmmScalarField,
)
from bi_external_api.attrs_model_mapper.utils import (
    Locale,
    MText,
)
from bi_external_api.attrs_model_mapper_docs.domain import (
    AmmOperation,
    AmmOperationExample,
)
from bi_external_api.attrs_model_mapper_docs.render_units import (
    ClassDoc,
    DocHeader,
    DocSection,
    DocText,
    DocUnit,
    FieldLine,
    GenericClassDoc,
    OperationDoc,
    OperationExampleDoc,
    RegularClassDoc,
    RenderContext,
)
from bi_external_api.attrs_model_mapper_docs.writer_utils import DocWriter


@attr.s()
class Docs:
    _operation_per_file: bool = attr.ib()
    _elements_dir_name: Optional[str] = attr.ib()
    _generate_toc: bool = attr.ib(default=False)

    _map_type_file_name: dict[Type, str] = attr.ib(factory=dict)
    _dedicated_class_docs: list[ClassDoc] = attr.ib(factory=list)
    _operations: list[tuple[AmmOperation, OperationDoc]] = attr.ib(factory=list)

    def field_to_doc_lines(self, field: AmmField, path: Sequence[str]) -> Sequence[FieldLine]:
        cp = field.common_props

        if isinstance(field, AmmScalarField):
            scalar_type = field.scalar_type
            type_text: str

            if isinstance(field, AmmEnumField):
                options = " / ".join([f"`{val}`" for val in field.values])
                type_text = f"enum/{scalar_type.__name__}[{options}]"
            else:
                type_text = scalar_type.__name__

            return [
                (
                    FieldLine(
                        path,
                        type_text=type_text,
                        nullable=cp.allow_none,
                        required=cp.required,
                        description=field.common_props.description,
                    )
                )
            ]

        elif isinstance(field, AmmNestedField):
            nested_schema = field.item
            nested_schema_doc_file_path = self.get_file_path_for_type(nested_schema.clz)

            main_line = FieldLine(
                path,
                type_text=nested_schema.clz.__name__,
                type_ref=nested_schema_doc_file_path,
                nullable=cp.allow_none,
                required=cp.required,
                description=field.common_props.description,
            )
            if nested_schema_doc_file_path is not None:
                return [main_line]

            assert isinstance(
                nested_schema, AmmRegularSchema
            ), f"Attempt to generate inline field docs for non-regular schema: {nested_schema}"
            return [
                main_line,
                *self.field_dict_to_doc_lines(nested_schema.fields, path),
            ]

        elif isinstance(field, AmmListField):
            next_path = [*path[:-1], path[-1] + "[]"]
            return [
                FieldLine(
                    path,
                    type_text="list",
                    nullable=cp.allow_none,
                    required=cp.required,
                    description=field.common_props.description,
                ),
                *self.field_to_doc_lines(field.item, next_path),
            ]

        raise AssertionError(f"Unexpected type of field: {type(field)}")

    def field_dict_to_doc_lines(
        self,
        field_dict: dict[str, AmmField],
        path: Sequence[str],
    ) -> Sequence[FieldLine]:
        ret: list[FieldLine] = []

        for field_name, field in field_dict.items():
            ret.extend(self.field_to_doc_lines(field, [*path, field_name]))

        return ret

    def regular_schema_to_object_doc(
        self,
        schema: AmmRegularSchema,
        generate_header: bool = True,
        discriminator_f_name: Optional[str] = None,
        discriminator_f_val: Optional[str] = None,
    ) -> RegularClassDoc:
        return RegularClassDoc(
            header=DocHeader(schema.clz.__name__) if generate_header else None,
            type=schema.clz,
            fields=self.field_dict_to_doc_lines(schema.fields, path=[]),
            description=schema.description,
            discriminator_field_name=discriminator_f_name,
            discriminator_field_value=discriminator_f_val,
        )

    def generic_schema_to_object_doc(
        self,
        schema: AmmGenericSchema,
        # TODO FIX: Make optional when description will be ready in AmmSchema
        description_override: Optional[MText],
    ) -> GenericClassDoc:
        return GenericClassDoc(
            header=DocHeader(schema.clz.__name__),
            description=description_override,
            discriminator_field_name=schema.discriminator_property_name,
            map_discriminator_object_doc={
                d: self.regular_schema_to_object_doc(
                    reg_schema,
                    discriminator_f_name=schema.discriminator_property_name,
                    discriminator_f_val=d,
                )
                for d, reg_schema in schema.mapping.items()
            },
            type=schema.clz,
        )

    def example_to_doc(self, example: AmmOperationExample, idx: int) -> OperationExampleDoc:
        return OperationExampleDoc(
            title=example.title or str(idx),
            description=example.description,
            rq=example.rq,
            rs=example.rs,
        )

    def operation_to_doc(self, op: AmmOperation) -> OperationDoc:
        return OperationDoc(
            header=DocHeader(op.code),
            description=op.description,
            request=self.regular_schema_to_object_doc(op.amm_schema_rq, generate_header=False),
            response=self.regular_schema_to_object_doc(op.amm_schema_rs, generate_header=False),
            examples=[self.example_to_doc(s, idx + 1) for idx, s in enumerate(op.examples)],
        )

    def get_file_path_for_type(self, clz: Type) -> Optional[str]:
        return self._map_type_file_name.get(clz)

    def get_file_path_for_type_strict(self, clz: Type) -> str:
        may_be_path = self.get_file_path_for_type(clz)
        assert may_be_path is not None, f"File path fot type {clz} is not registered."
        return may_be_path

    def register_file_path_for_type(self, clz: Type, file_path: str) -> None:
        self._map_type_file_name[clz] = file_path

    def default_file_name_for_type(self, clz: Type) -> str:
        file_name = f"{clz.__name__}.md"
        if self._elements_dir_name:
            return os.path.join(self._elements_dir_name, file_name)
        return file_name

    def register_generic_schema(
        self,
        generic_schema: AmmGenericSchema,
        description_override: Optional[MText],
        file_path: Optional[str] = None,
    ) -> None:
        self.register_file_path_for_type(
            generic_schema.clz,
            file_path or self.default_file_name_for_type(generic_schema.clz),
        )
        self._dedicated_class_docs.append(
            self.generic_schema_to_object_doc(generic_schema, description_override),
        )

    def register_regular_schema_as_ref(
        self,
        regular_schema: AmmRegularSchema,
        file_path: Optional[str] = None,
    ) -> None:
        self.register_file_path_for_type(
            regular_schema.clz,
            file_path or self.default_file_name_for_type(regular_schema.clz),
        )
        doc = self.regular_schema_to_object_doc(regular_schema)
        self._dedicated_class_docs.append(doc)

    def register_operations(self, operations: Sequence[AmmOperation]) -> None:
        self._operations.extend([(op, self.operation_to_doc(op)) for op in operations])

    def render(self, dir_path: str, locale: Locale) -> None:
        initial_ctx = RenderContext(
            headers_level=0,
            current_file="",
            locale=locale,
        )
        doc_writer = DocWriter(initial_ctx, base_dir=dir_path)

        for class_doc in self._dedicated_class_docs:
            doc_writer.write(
                class_doc,
                self.get_file_path_for_type_strict(class_doc.type),
                append_nl=True,
            )

        if self._operations:
            if self._operation_per_file:
                root_doc_content: list[DocUnit] = []

                for operation, op_doc in self._operations:
                    rel_file_path = os.path.join("operations", f"{operation.code}.md")

                    root_doc_content.append(
                        DocSection(
                            header=DocHeader(f"[{operation.code}]({rel_file_path})"),
                            content=[DocText(operation.description), DocText([])],
                        )
                    )
                    doc_writer.write(op_doc, rel_file_path)

                doc_writer.write(
                    DocSection(
                        header=DocHeader(MText(ru="Список операций", en="Operations list")),
                        content=root_doc_content,
                    ),
                    "all.md",
                )

            else:
                doc_writer.write(
                    DocSection(
                        header=DocHeader(MText(ru="Описание операций", en="Operations description")),
                        content=[op[1] for op in self._operations],
                    ),
                    "README.md",
                )
        else:
            if self._generate_toc:
                doc_writer.write_text(
                    yaml.safe_dump(
                        dict(
                            title="Configs",
                            items=[
                                dict(
                                    # TODO FIX: Extract TOC title somehow more accurate
                                    name=initial_ctx.localize(class_doc.header.value),  # type: ignore
                                    href=self.get_file_path_for_type_strict(class_doc.type),
                                )
                                for class_doc in self._dedicated_class_docs
                            ],
                        ),
                        default_flow_style=False,
                        sort_keys=False,
                    ),
                    "toc-i.yaml",
                )
