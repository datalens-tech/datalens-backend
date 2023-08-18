from typing import Type

import attr

from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.attrs_model_mapper_docs.operations_builder import (
    UserOperationInfo,
    OperationExample,
)
from bi_external_api.docs.common import DocsBuilder
from bi_external_api.domain import external as ext
from bi_external_api.domain.external import WorkbookIndexItem
from bi_external_api.enums import ExtAPIType
from bi_external_api.ext_examples import (
    CHConnectionBuilder,
    ChartBuilderSingleDataset,
    SuperStoreLightDSBuilder, DashBuilderSingleTab,
)


@attr.s()
class DoubleCloudDocsBuilder(DocsBuilder):
    operation_kind_enum = ext.WorkbookOpKind
    rq_base_type = ext.DCOpRequest
    rs_base_type = ext.DCOpResponse
    elements_dir_name = None  # Suggested to generate only models docs in root
    generate_toc = True

    generate_operations: bool = attr.ib(kw_only=True, default=False)

    def get_model_mapper(self) -> ModelMapperMarshmallow:
        return ext.get_external_model_mapper(ExtAPIType.DC)

    def get_sample_dataset(
            self, *,
            name: str = None,
            add_fields: bool = False,
            conn_name: str,
            fill_defaults: bool = False,
    ) -> ext.DatasetInstance:
        builder = SuperStoreLightDSBuilder(conn_name=conn_name)

        if add_fields:
            builder = builder.do_add_default_fields().add_field(
                ext.DatasetField(
                    id="sum_sales",
                    title="Sales sum",
                    cast=ext.FieldType.float,
                    aggregation=ext.Aggregation.sum,
                    calc_spec=ext.DirectCS("sales", avatar_id=None),
                    description="Sum of sales",
                )
            )
        if fill_defaults:
            builder = builder.with_fill_defaults()

        return builder.build_instance(name=name)

    def get_sample_workbook(self, conn_name: str) -> ext.WorkBook:
        # Dataset
        ds_name = "sales"
        dataset_inst = self.get_sample_dataset(name=ds_name, add_fields=True, conn_name=conn_name)

        # Charts
        base_chart_builder = ChartBuilderSingleDataset(ds_name=ds_name)

        chart_ind_total_sales = base_chart_builder.with_visualization(
            ext.Indicator(
                ext.ChartField(ext.ChartFieldRef("sum_sales")),
            )
        ).build_instance(name="chart_ind_total_sales")

        chart_cols_sales_per_region = base_chart_builder.with_visualization(
            ext.ColumnChart(
                x=[ext.ChartField.create_as_ref("region")],
                y=[ext.ChartField.create_as_ref("sum_sales")],
                sort=[ext.ChartSort(source=ext.ChartFieldRef(id="region"), direction=ext.SortDirection.ASC)]
            )
        ).build_instance(name="chart_cols_sales_per_region")

        all_charts: list[ext.ChartInstance] = [chart_ind_total_sales, chart_cols_sales_per_region]

        # Dashboards
        main_dash = DashBuilderSingleTab(chart_names=[inst.name for inst in all_charts]).build_instance("main_dash")

        return ext.WorkBook(
            datasets=[dataset_inst],
            charts=all_charts,
            dashboards=[main_dash],
        )

    def get_types_to_register_as_dedicated_docs(self) -> list[Type]:
        return [*super().get_types_to_register_as_dedicated_docs(), ext.WorkBook]

    def get_user_operation_info_list(self) -> list[UserOperationInfo]:
        if self.generate_operations:
            return self.get_user_operation_info_list_real()
        return []

    def get_user_operation_info_list_real(self) -> list[UserOperationInfo]:
        WRITE_SAFE_WB_TITLE = "The workbook"
        WRITE_SAFE_WB_ID = "eequotiu7f"
        DEFAULT_CONN_NAME = "conn_ch_0"
        DEFAULT_PROJECT_ID = "aey8weeboh"

        return [
            UserOperationInfo(
                ext.WorkbookOpKind.wb_read,
                MText(
                    ru="Получить конфигурацию воркбука.",
                    en="Get workbook configuration."
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="Пустой воркбук",
                            en="Empty workbook"
                        ),
                        description=MText(
                            ru="TBD",
                            en="Description: TBD",
                        ),
                        rq=ext.DCOpWorkbookGetRequest(workbook_id=WRITE_SAFE_WB_ID),
                        rs=ext.DCOpWorkbookGetResponse(workbook=ext.WorkBook.create_empty()),
                    ),
                ]
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.wb_create,
                MText(
                    ru="Создать воркбук.",
                    en="Create a workbook.",
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="Пустой воркбук",
                            en="Empty workbook",
                        ),
                        description=MText(
                            ru="TBD",
                            en="Description: TBD",
                        ),
                        rq=ext.DCOpWorkbookCreateRequest(
                            project_id=DEFAULT_PROJECT_ID,
                            workbook_title=WRITE_SAFE_WB_TITLE,
                        ),
                        rs=ext.DCOpWorkbookCreateResponse(workbook_id=WRITE_SAFE_WB_ID)
                    ),
                ]
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.wb_modify,
                MText(
                    ru="Применить конфигурацию воркбука."
                       " Выполняется сравнение текущего состояния каждого объекта в воркбуке и конфига из запроса."
                       " Объекты воркбука, отсутствующие в запросе, будут удалены."
                       "\n\n"
                       "Свойства объекта, измененные через UI и не поддержанные в API, не будут затронуты."
                       " Это поведение можно изменить установив флаг `force_rewrite`."
                       " Тогда все объекты будут переписаны, вне зависимости от наличия изменений в UI.",
                    en="Apply workbook configuration."
                       " This compares the current status of each object in the workbook with the configuration from the query."
                       " The object in the workbook not listed in the query will be deleted."
                       "\n\n"
                       "The properties of the object edited in the UI and not supported by the API won't be affected."
                       " This behavior can be changed by using the `force_rewrite` flag."
                       " This will rewrite all the objects, regardless of changes in the UI.",
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="Наполнение пустого воркбука.",
                            en="Empty workbook",
                        ),
                        description=MText(
                            ru="TBD",
                            en="Description: TBD",
                        ),
                        rq=ext.DCOpWorkbookModifyRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            workbook=self.get_sample_workbook(DEFAULT_CONN_NAME),
                        ),
                        rs=ext.DCOpWorkbookModifyResponse(
                            # TODO FIX: Fill defaults
                            workbook=self.get_sample_workbook(DEFAULT_CONN_NAME),
                            # TODO FIX: Fill modification plan
                            executed_plan=ext.ModificationPlan(operations=[]),
                        )
                    ),
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.wb_delete,
                MText(
                    ru="Удалить воркбук.",
                    en="Delete a workbook.",
                ),
                example_list=[
                    OperationExample(
                        title=None,
                        description=MText(ru="TBD", en="Delete workbook"),
                        rq=ext.DCOpWorkbookDeleteRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                        ),
                        rs=ext.DCOpWorkbookDeleteResponse(),
                    ),
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.connection_get,
                MText(
                    ru="Получить конфигурацию подключения.",
                    en="Get connection configuration.",
                ),
                example_list=[
                    OperationExample(
                        title=None,
                        description=MText(
                            ru="TBD",
                            en="Sample for CH conn",
                        ),
                        rq=ext.DCOpConnectionGetRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            name=DEFAULT_CONN_NAME,
                        ),
                        rs=ext.DCOpConnectionGetResponse(
                            connection=CHConnectionBuilder(
                                raw_sql_level=ext.RawSQLLevel.subselect
                            ).build_instance(DEFAULT_CONN_NAME),
                        )
                    )
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.connection_create,
                MText(
                    ru="Создать подключение в существующем воркбуке.",
                    en="Create a connection in existing workbook.",
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="ClickHouse",
                            en="ClickHouse",
                        ),
                        description=MText(
                            ru="TBD",
                            en="Description: TBD",
                        ),
                        rq=ext.DCOpConnectionCreateRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            connection=CHConnectionBuilder(
                                raw_sql_level=ext.RawSQLLevel.subselect
                            ).build_instance(DEFAULT_CONN_NAME),
                            secret=ext.PlainSecret("My-Str0Ng-pass"),
                        ),
                        rs=ext.DCOpConnectionCreateResponse(
                            ext.EntryInfo(kind=ext.EntryKind.connection, id="66tsr6rmna", name=DEFAULT_CONN_NAME)
                        )
                    ),
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.connection_modify,
                MText(
                    ru="Отредактировать подключение.",
                    en="Modify a connection.",
                ),
                example_list=[
                    OperationExample(
                        title=None,
                        description=MText(
                            ru="TBD",
                            en="Description: TBD",
                        ),
                        rq=ext.DCOpConnectionModifyRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            connection=CHConnectionBuilder(
                                raw_sql_level=ext.RawSQLLevel.subselect
                            ).build_instance(DEFAULT_CONN_NAME),
                            secret=ext.PlainSecret("My-Str0Ng-pass"),
                        ),
                        rs=ext.DCOpConnectionModifyResponse(
                        )
                    ),
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.connection_delete,
                MText(
                    ru="Удалить подключение.",
                    en="Delete a connection.",
                ),
                example_list=[
                    OperationExample(
                        title=None,
                        description=MText(
                            ru="TBD",
                            en=f"Delete connection"
                               f" with name `{DEFAULT_CONN_NAME}` in workbook with ID `{WRITE_SAFE_WB_ID}`",
                        ),
                        rq=ext.DCOpConnectionDeleteRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            name=DEFAULT_CONN_NAME,
                        ),
                        rs=ext.DCOpConnectionDeleteResponse(
                        )
                    )
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.advise_dataset_fields,
                MText(
                    ru="Составить конфигурацию датасета исходя из схемы источника данных."
                       " В запросе передается датасет с пустым списком полей."
                       "\n\n"
                       "В ответе будет датасет со всеми полями источника данных."
                       " Для получения схемы используется подключение из поля `connection_ref`."
                       " ID полей будут сгенерированы из названия колонок, предполагается, что пользователь их перепишет.",
                    en="Put together a dataset configuration based on the data source schema."
                       " The query transfers a dataset with a blank fields list."
                       "\n\n"
                       "The response will contain a dataset with all the fields at the source."
                       " To get a schema, use the connection from the `connection_ref` field."
                       " Field IDs will be generated from column names. We suppose the user will rewrite them.",
                ),
                example_list=[
                    OperationExample(
                        description=MText(
                            ru="TBD",
                            en="Description: TBD",
                        ),
                        rq=ext.DCOpAdviseDatasetFieldsRequest(
                            partial_dataset=self.get_sample_dataset(
                                name="whatever",
                                conn_name="whatever",
                                add_fields=False,
                                fill_defaults=False,
                            ).dataset,
                            workbook_id=WRITE_SAFE_WB_ID,
                            connection_name="%my_connection_name%",
                        ),
                        rs=ext.DCOpAdviseDatasetFieldsResponse(
                            dataset=self.get_sample_dataset(
                                name="whatever",
                                conn_name="whatever",
                                add_fields=True,
                                fill_defaults=True,
                            ).dataset
                        ),
                    )
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.wb_list,
                MText(
                    ru="Получить список всех воркбуков в проекте.",
                    en="Get all workbook list for a project.",
                ),
                example_list=[
                    OperationExample(
                        title=None,
                        description=MText(ru="Получить список воркбуков.", en="Get workbook list."),
                        rq=ext.DCOpWorkbookListRequest(
                            project_id=DEFAULT_PROJECT_ID,
                        ),
                        rs=ext.DCOpWorkbookListResponse(
                            workbooks=[
                                WorkbookIndexItem(
                                    id=WRITE_SAFE_WB_ID,
                                    title=WRITE_SAFE_WB_TITLE
                                )
                            ]
                        ),
                    ),
                ],
            ),
        ]
