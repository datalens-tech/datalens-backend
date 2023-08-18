import json
import os
import urllib.parse
from typing import Sequence, Optional, Any

import attr
import yaml

from bi_external_api.attrs_model_mapper.domain import (
    AmmField,
    AmmScalarField,
    AmmNestedField,
    AmmRegularSchema,
    AmmListField,
    AmmOneOfDescriptorField,
    AmmEnumDescriptor,
)
from bi_external_api.attrs_model_mapper.utils import Locale, MText
from bi_external_api.attrs_model_mapper_docs.domain import AmmOperation
from bi_external_api.attrs_model_mapper_docs.md_link_extractor import process_links
from bi_external_api.attrs_model_mapper_docs.render_units import (
    DocHeader,
    RenderContext,
    DocSection,
    DocUnit,
    MultiLineTable,
    MultiLineTableRow,
    DocText,
    DocLink,
    DocUnitGroup,
)
from bi_external_api.attrs_model_mapper_docs.writer_utils import DocWriter
from bi_external_api.docs.dc_grpc_amm_schema import ProtoGenDocAccessor, ExternalClassDef


@attr.s(kw_only=True, frozen=True, auto_attribs=True)
class TOCInclude:
    title: str
    rel_path: str


@attr.s(kw_only=True, frozen=True)
class ServiceGroup:
    directory: str = attr.ib()
    grpc_package_prefix: str = attr.ib()
    models_header: str = attr.ib()
    title: str = attr.ib()
    # For service-related content managed manually or by another generator
    custom_includes: tuple[TOCInclude, ...] = attr.ib(default=())
    map_svc_fqdn_description_fallback: dict[str, str] = attr.ib(factory=dict)  # type: ignore

    def get_service_toc_name(self, grpc_service_id: str) -> str:
        return grpc_service_id.split(".")[-1]

    def get_service_operations_file_path(self, grpc_service_id: str) -> str:
        return os.path.join(self.directory, grpc_service_id.split(".")[-1], "all_operations.md")

    def get_all_service_ids(self, proto_gen_doc: ProtoGenDocAccessor) -> list[str]:
        return proto_gen_doc.get_services_by_prefix(self.grpc_package_prefix)


@attr.s(kw_only=True)
class TypeInfo:
    type_id: str = attr.ib()
    is_enum: bool = attr.ib()
    file_path: str = attr.ib()
    header_text: str = attr.ib()
    anchor: Optional[str] = attr.ib()


@attr.s()
class GRPCDocs:
    _proto_gen_doc: ProtoGenDocAccessor = attr.ib()
    _service_groups: list[ServiceGroup] = attr.ib()
    _map_type_id_ref: dict[str, str] = attr.ib(factory=dict)
    _map_dcdoc_url_md_url: dict[str, str] = attr.ib(factory=dict)

    def adopt_dc_doc_link(self, doc_link: DocLink) -> Optional[DocLink]:
        parsed_href = urllib.parse.urlparse(doc_link.href)
        if parsed_href.scheme != "dcdoc":
            return None

        if doc_link.href not in self._map_dcdoc_url_md_url:
            raise ValueError(f"Mapping for DC docs URL is not registered: {doc_link.href}")
        return DocLink(text=doc_link.text, href=self._map_dcdoc_url_md_url[doc_link.href])

    def handle_text_from_spec(self, m_txt: Optional[MText]) -> Optional[DocText]:
        if m_txt is None:
            return None
        # TODO FIX: Figure out what to do with multiple locales
        en_txt: Optional[str] = m_txt.en
        if en_txt is None:
            return None

        return DocText(process_links(en_txt, self.adopt_dc_doc_link))

    def create_field_row(
            self,
            path: Sequence[str],
            type_text: str,
            type_ref: Optional[str],
            description: Optional[DocText],
    ) -> MultiLineTableRow:
        type_id_md = (
            DocLink(text=type_text, href=type_ref)
            if type_ref is not None
            else DocText(["**", type_text, "**"])
        )
        return MultiLineTableRow([
            DocText(".".join(f"`{part}`" for part in path)),
            DocUnitGroup([
                type_id_md,
                description,
            ]),
        ])

    def field_to_doc_lines(self, field: AmmField, path: Sequence[str]) -> Sequence[MultiLineTableRow]:
        cp = field.common_props
        processed_description = self.handle_text_from_spec(cp.description)

        if isinstance(field, AmmScalarField):
            type_identifier = field.scalar_type_identifier
            assert type_identifier is not None, f"Type identifier is not defined for AmmField: {field!r}"

            return [
                self.create_field_row(
                    path,
                    type_text=type_identifier,
                    type_ref=self._map_type_id_ref.get(type_identifier),
                    description=processed_description,
                )
            ]

        elif isinstance(field, AmmOneOfDescriptorField):
            return [
                self.create_field_row(
                    path,
                    type_text="one of: " + " / ".join(f"`{fn}`" for fn in field.field_names),
                    type_ref=None,
                    description=processed_description,
                )
            ]

        elif isinstance(field, AmmNestedField):
            nested_schema = field.item
            type_identifier = nested_schema.identifier
            assert type_identifier is not None, f"Type identifier is not defined for AmmSchema: {field.item!r}"
            as_ref = type_identifier in self._map_type_id_ref

            main_line = self.create_field_row(
                path,
                type_text=type_identifier,
                type_ref=self._map_type_id_ref[type_identifier],
                description=processed_description,
            )
            if as_ref:
                return [main_line]

            assert isinstance(nested_schema, AmmRegularSchema), \
                f"Attempt to generate inline field docs for non-regular schema: {nested_schema}"
            return [
                main_line,
                *self.field_dict_to_doc_lines(nested_schema.fields, path),
            ]

        elif isinstance(field, AmmListField):
            next_path = [*path[:-1], path[-1] + "[]"]
            return [
                self.create_field_row(
                    path,
                    type_text="list",
                    type_ref=None,
                    description=processed_description,
                ),
                *self.field_to_doc_lines(field.item, next_path),
            ]

        raise AssertionError(f"Unexpected type of field: {type(field)}")

    def field_dict_to_doc_lines(
            self,
            field_dict: dict[str, AmmField],
            path: Sequence[str],
    ) -> Sequence[MultiLineTableRow]:
        ret: list[MultiLineTableRow] = []

        for field_name, field in field_dict.items():
            ret.extend(self.field_to_doc_lines(field, [*path, field_name]))

        return ret

    def regular_schema_to_object_doc(
            self,
            schema: AmmRegularSchema,
    ) -> DocUnitGroup:
        return DocUnitGroup([
            self.handle_text_from_spec(schema.description),
            MultiLineTable([
                MultiLineTableRow(["**Field**", "**Description**"]),
                *self.field_dict_to_doc_lines(schema.fields, path=[])
            ]),
        ])

    def enum_descriptor_to_doc_units(self, enum_descriptor: AmmEnumDescriptor) -> Sequence[Optional[DocUnit]]:
        return [
            DocText(enum_descriptor.description) if enum_descriptor.description else None,
            MultiLineTable([
                MultiLineTableRow(["**Option**", "**Description**"]),
                *[
                    MultiLineTableRow([
                        DocText(["**", option.key, "**"]),
                        DocText(option.description) if option.description else ""]
                    )
                    for option in enum_descriptor.members
                ],
            ])
        ]

    def operation_to_doc(self, op: AmmOperation) -> DocSection:
        return DocSection(
            header=DocHeader(op.code),
            content=[
                self.handle_text_from_spec(op.description),
                DocSection(
                    header=DocHeader("Request"),
                    content=[self.regular_schema_to_object_doc(op.amm_schema_rq)],
                ),
                DocSection(
                    header=DocHeader("Response"),
                    content=[self.regular_schema_to_object_doc(op.amm_schema_rs)],
                )
            ],
        )

    def render(self, dir_path: str, locale: Locale) -> None:
        initial_ctx = RenderContext(
            headers_level=0,
            current_file="",
            locale=locale,
        )
        doc_writer = DocWriter(
            initial_ctx,
            base_dir=dir_path
        )

        # Resolving references for messages
        type_info_to_extract_list: list[TypeInfo] = []
        map_file_path_title: dict[str, str] = {}

        for service_group in self._service_groups:
            sg_msg_id_list = self._proto_gen_doc.get_messages_by_prefix(service_group.grpc_package_prefix)
            sg_enum_id_list = self._proto_gen_doc.get_enums_by_prefix(service_group.grpc_package_prefix)

            file_path = os.path.join(service_group.directory, "models.md")
            file_header = service_group.models_header

            if file_path in map_file_path_title:
                raise ValueError(f"File path {file_path!r} duplicated.")

            map_file_path_title[file_path] = file_header

            for is_enum, id_list in [(True, sg_enum_id_list), (False, sg_msg_id_list)]:
                for type_id in id_list:
                    anchor = type_id.replace(".", "-").lower()

                    type_info_to_extract_list.append(
                        TypeInfo(
                            file_path=file_path,
                            anchor=anchor,
                            is_enum=is_enum,
                            type_id=type_id,
                            header_text=type_id.removeprefix(service_group.grpc_package_prefix)
                        )
                    )

        self._map_type_id_ref.update({
            type_info.type_id: f"{type_info.file_path}#{type_info.anchor}"
            for type_info in type_info_to_extract_list
        })

        # Rendering ref map
        path_map: dict[str, list[TypeInfo]] = {}
        for type_info in type_info_to_extract_list:
            path_map.setdefault(type_info.file_path, []).append(type_info)

        for file_path, type_info_list in path_map.items():
            children: list[DocUnit] = []

            for type_info in type_info_list:
                section_content: list[Optional[DocUnit]] = []
                if type_info.is_enum:
                    enum_descriptor = self._proto_gen_doc.get_enum(type_info.type_id)
                    section_content.extend(self.enum_descriptor_to_doc_units(enum_descriptor))
                else:
                    amm_schema = self._proto_gen_doc.get_schema_for_message(type_info.type_id)
                    assert isinstance(amm_schema, AmmRegularSchema)

                    section_content.append(
                        self.regular_schema_to_object_doc(
                            amm_schema,
                        ),
                    )
                header_value = type_info.header_text
                if type_info.anchor is not None:
                    header_value += f" {{#{type_info.anchor}}}"

                children.append(DocSection(
                    DocHeader(header_value),
                    content=section_content,
                ))

            doc_writer.write(DocSection(
                header=DocHeader(map_file_path_title[file_path]),
                content=children,
            ), file_path)

        for service_group in self._service_groups:
            group_service_list = service_group.get_all_service_ids(self._proto_gen_doc)

            for service_name in group_service_list:
                all_operations = self._proto_gen_doc.get_operations(service_name)

                doc_writer.write(
                    DocSection(
                        header=DocHeader(f"Service {service_name}"),
                        content=[self.operation_to_doc(op) for op in all_operations]
                    ),
                    service_group.get_service_operations_file_path(service_name),
                )

        # Preparing TOC & indexes
        root_toc_includes: list[dict[str, Any]] = []

        for service_group in self._service_groups:
            all_service_ids = service_group.get_all_service_ids(self._proto_gen_doc)
            service_group_toc_path = os.path.join(service_group.directory, "toc-i.yaml")
            root_toc_includes.append(dict(
                name=service_group.title,
                items=None,
                include=dict(
                    mode="link",
                    path=service_group_toc_path,
                ),
            ))
            # index.yaml for service group
            index_title = f"{service_group.title} API reference"

            doc_writer.write_text(
                yaml.safe_dump(dict(
                    title=index_title,
                    # TODO FIX: Externalize
                    description=(
                        f"This section contains API reference for DoubleCloud {service_group.title} service."
                    ),
                    meta=dict(
                        title=index_title,
                        noIndex=False,
                    ),
                    links=[
                        dict(
                            title=service_group.get_service_toc_name(svc_id),
                            description=(
                                self._proto_gen_doc.get_service_description(svc_id)
                                or service_group.map_svc_fqdn_description_fallback.get(svc_id)
                                or ""
                            ),
                            href=os.path.relpath(
                                service_group.get_service_operations_file_path(svc_id),
                                service_group.directory,
                            ),
                        )
                        for svc_id in all_service_ids
                    ],
                ), default_flow_style=False, sort_keys=False),
                os.path.join(service_group.directory, "index.yaml"),
            )
            # TOC for service group
            doc_writer.write_text(
                yaml.safe_dump(dict(
                    title=service_group.title,
                    items=[
                        dict(name="Overview", href="index.yaml"),
                        *[
                            dict(
                                name=service_group.get_service_toc_name(svc_id),
                                href=os.path.relpath(
                                    service_group.get_service_operations_file_path(svc_id),
                                    service_group.directory,
                                )
                            )
                            for svc_id in all_service_ids
                        ],
                        *[
                            dict(name="Models", href="models.md")
                        ],
                        *[
                            dict(
                                name=incl.title,
                                items=None,
                                include=dict(
                                    mode="link",
                                    path=incl.rel_path,
                                ),
                            ) for incl in service_group.custom_includes
                        ],
                    ],
                ), default_flow_style=False, sort_keys=False),
                service_group_toc_path,
            )

        doc_writer.write_text(
            yaml.safe_dump(dict(
                title="API reference",
                items=root_toc_includes,
            ), default_flow_style=False, sort_keys=False),
            "toc-i.yaml",
        )


def main(*, proto_doc_json: dict[str, Any], render_target: str) -> None:
    acc = ProtoGenDocAccessor(
        proto_doc_json,
        {
            ExternalClassDef(full_name="bool"),
            ExternalClassDef(full_name="double"),
            ExternalClassDef(full_name="google.protobuf.BoolValue"),
            ExternalClassDef(full_name="google.protobuf.DoubleValue"),
            ExternalClassDef(full_name="google.protobuf.Duration"),
            ExternalClassDef(full_name="google.protobuf.Empty"),
            ExternalClassDef(full_name="google.protobuf.Int64Value"),
            ExternalClassDef(full_name="google.protobuf.StringValue"),
            ExternalClassDef(full_name="google.protobuf.Timestamp"),
            ExternalClassDef(full_name="google.protobuf.Value"),
            ExternalClassDef(full_name="google.rpc.Status"),
            ExternalClassDef(full_name="google.type.DayOfWeek"),
            ExternalClassDef(full_name="int64"),
            ExternalClassDef(full_name="string")
        }
    )
    docs = GRPCDocs(acc, [
        ServiceGroup(
            title="Common",
            directory="common",
            grpc_package_prefix="doublecloud.v1.",
            models_header="Common models",
        ),
        ServiceGroup(
            title="ClickHouse",
            directory="clickhouse",
            grpc_package_prefix="doublecloud.clickhouse.v1.",
            models_header="ClickHouse API models",
            map_svc_fqdn_description_fallback={
                "doublecloud.clickhouse.v1.BackupService": (
                    "A set of methods for managing ClickHouse cluster backups."
                ),
                "doublecloud.clickhouse.v1.VersionService": (
                    "A set of methods for managing ClickHouse software versions."
                ),
            },
        ),
        ServiceGroup(
            title="Kafka",
            directory="kafka",
            grpc_package_prefix="doublecloud.kafka.v1.",
            models_header="Kafka API models",
            map_svc_fqdn_description_fallback={
                "doublecloud.kafka.v1.VersionService": "A set of methods for managing Kafka software versions.",
            },
        ),
        ServiceGroup(
            title="Network",
            directory="network",
            grpc_package_prefix="doublecloud.network.v1.",
            models_header="Network API models",
        ),
        ServiceGroup(
            title="Transfer",
            directory="transfer",
            grpc_package_prefix="doublecloud.transfer.v1.",
            models_header="Transfer API models",
            map_svc_fqdn_description_fallback={
                "doublecloud.transfer.v1.EndpointService": "A set of methods for managing endpoints.",
                "doublecloud.transfer.v1.OperationService": "A set of methods for managing operations.",
                "doublecloud.transfer.v1.TransferService": "A set of methods for managing transfers.",
            },
        ),
        ServiceGroup(
            title="Visualization",
            directory="visualization",
            grpc_package_prefix="doublecloud.visualization.v1.",
            models_header="Visualization API models",
            custom_includes=(
                # Include of JSON schemas
                TOCInclude(
                    title="Configs",
                    rel_path="configs/toc-i.yaml",
                ),
            ),
            map_svc_fqdn_description_fallback={
                "doublecloud.visualization.v1.WorkbookService": (
                    "A set of methods for managing Visualization assets."
                ),
            }
        ),
    ], map_dcdoc_url_md_url={
        "dcdoc://visualization-configs/Connection": "visualization/configs/Connection.md",
        "dcdoc://visualization-configs/Dataset": "visualization/configs/Dataset.md",
        "dcdoc://visualization-configs/Workbook": "visualization/configs/WorkBook.md",
    })
    docs.render(render_target, locale="en")


if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), "../../temp/dc_api_docs.json")) as f:
        pd_json = json.load(f)

    docs_dir = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../../all_dc_docs"
    ))
    os.makedirs(docs_dir, exist_ok=True)

    main(
        proto_doc_json=pd_json,
        render_target=docs_dir,
    )
