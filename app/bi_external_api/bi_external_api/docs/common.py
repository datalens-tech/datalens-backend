import enum
from typing import (
    ClassVar,
    Optional,
    Type,
)

import attr

from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.attrs_model_mapper_docs.main import Docs
from bi_external_api.attrs_model_mapper_docs.operations_builder import (
    AmmOperationsBuilder,
    UserOperationInfo,
)
from bi_external_api.domain import external as ext


@attr.s
class DocsBuilder:
    operation_kind_enum: ClassVar[Type[enum.Enum]]
    rq_base_type: ClassVar[Type]
    rs_base_type: ClassVar[Type]
    elements_dir_name: ClassVar[Optional[str]] = "elements"
    generate_toc: ClassVar[bool] = False

    operation_per_file: bool = attr.ib(default=False)

    def get_model_mapper(self) -> ModelMapperMarshmallow:
        raise NotImplementedError()

    def get_user_operation_info_list(self) -> list[UserOperationInfo]:
        raise NotImplementedError()

    def get_generics_description(self) -> list[tuple[Type, MText]]:
        return [
            (
                ext.MeasureColoringSpec,
                MText(
                    ru="Спецификация определения цвета для показателя",
                    en="Measures coloring specification",
                ),
            ),
            (
                ext.DataSourceSpec,
                MText(
                    ru="Спецификация источника данных: таблица, подзапрос и т. д.",
                    en="Data source specification: table, subquery, etc.",
                ),
            ),
            (
                ext.CalcSpec,
                MText(
                    ru="Спецификация поля данных: поле в таблице, формула и т. д.",
                    en="Data field specification: table cell, formula etc.",
                ),
            ),
            (
                ext.ChartFieldSource,
                MText(
                    ru="Ссылка на поле данных или специальный тип поля.",
                    en="Link to a data field or a special field type.",
                ),
            ),
            (
                ext.FieldColoring,
                MText(
                    ru="Конфигурация цветового изменения чарта.",
                    en="Chart color changes configuration.",
                ),
            ),
            (
                ext.Visualization,
                MText(
                    ru="Визуализация данных.",
                    en="Data visualization",
                ),
            ),
            (
                ext.ControlValueSource,
                MText(
                    ru="Источник опций для селектора.",
                    en="Options source for a selector.",
                ),
            ),
            (
                ext.Value,
                MText(
                    ru="Значение опции для селектора.",
                    en="Option values for a selector.",
                ),
            ),
            (
                ext.DashElement,
                MText(
                    ru="Элемент дашборда: виджет, селектор и т. д.",
                    en="Dashboard element: widget, selector etc.",
                ),
            ),
            (
                ext.Secret,
                MText(
                    ru="Секрет (пароль или токен) для подключения. На данный момент поддерживается только plain-text.",
                    en="Secret for connection (password or token). Currently, only the plain text is supported.",
                ),
            ),
            (
                ext.EntryRef,
                MText(
                    ru="Ссылка на компонент в DataLens.",
                    en="Link to a component in Visualization.",
                ),
            ),
            (ext.Connection, MText(ru="Подключение к базе данных.", en="Connection to database")),
        ]

    def get_types_to_register_as_dedicated_docs(self) -> list[Type]:
        return [ext.Dataset, ext.Chart, ext.Dashboard]

    def build(self) -> Docs:
        reg = self.get_model_mapper().get_amm_schema_registry()
        docs = Docs(
            operation_per_file=self.operation_per_file,
            elements_dir_name=self.elements_dir_name,
            generate_toc=self.generate_toc,
        )

        for gen, desc in self.get_generics_description():
            docs.register_generic_schema(reg.get_generic_type_schema(gen), desc)

        for clz in self.get_types_to_register_as_dedicated_docs():
            docs.register_regular_schema_as_ref(
                reg.get_regular_type_schema(clz),
            )

        docs.register_operations(
            AmmOperationsBuilder(
                operation_kind_enum=self.operation_kind_enum,
                rq_base_type=self.rq_base_type,
                rs_base_type=self.rs_base_type,
                model_mapper=self.get_model_mapper(),
                user_op_info_list=self.get_user_operation_info_list(),
            ).build()
        )

        return docs
