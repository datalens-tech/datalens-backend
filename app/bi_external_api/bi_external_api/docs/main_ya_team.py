from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.attrs_model_mapper_docs.operations_builder import (
    UserOperationInfo,
    OperationExample,
)
from bi_external_api.docs.common import DocsBuilder
from bi_external_api.domain import external as ext
from bi_external_api.enums import ExtAPIType
from bi_external_api.ext_examples import (
    CHYTConnectionBuilder,
    CHConnectionBuilder,
    SuperStoreExtDSBuilder,
    WorkbookBuilderSingleDatasetSingleDash, ModificationPlanBuilderWorkbookBased, ChartBuilderSingleDataset,
)


class YaTeamDocsBuilder(DocsBuilder):
    operation_kind_enum = ext.WorkbookOpKind
    rq_base_type = ext.WorkbookOpRequest
    rs_base_type = ext.WorkbookOpResponse

    def get_model_mapper(self) -> ModelMapperMarshmallow:
        return ext.get_external_model_mapper(ExtAPIType.YA_TEAM)

    def get_user_operation_info_list(self) -> list[UserOperationInfo]:
        READ_WB_ID = "examples/API/example_1"
        WRITE_SAFE_WB_ID = "the_bottomless_abyss/temporary_wb_1"
        DEFAULT_CONN_NAME = "main_connection"
        DEFAULT_DATASET_NAME = "superstore_sales"

        DEFAULT_DATASET_BUILDER = SuperStoreExtDSBuilder(
            entry_name=DEFAULT_DATASET_NAME,
            conn_name=DEFAULT_CONN_NAME,
        )
        DEFAULT_WORKBOOK_BUILDER = WorkbookBuilderSingleDatasetSingleDash(
            dataset_builder=DEFAULT_DATASET_BUILDER.do_add_default_fields(),
            chart_builders=[
                (
                    ChartBuilderSingleDataset(ext.Indicator(
                        ext.ChartField(ext.ChartFieldRef("sum_sales")),
                    ))
                        .add_formula_field("sum_sales", ext.FieldType.float, "SUM([Sales])")
                        .with_entry_name("total_sales_indicator")
                ),
                (
                    ChartBuilderSingleDataset(ext.ColumnChart(
                        x=[ext.ChartField.create_as_ref("region")],
                        y=[ext.ChartField.create_as_ref("sales_sum")],
                        sort=[ext.ChartSort(source=ext.ChartFieldRef(id="region"), direction=ext.SortDirection.ASC)]
                    ))
                        .add_aggregated_field("sales", ext.Aggregation.sum)
                        .with_entry_name("sales_per_region")
                ),
            ],
        ).with_add_charts_to_dash()

        CLUSTERIZE_WB_BUILDER = WorkbookBuilderSingleDatasetSingleDash(
            dataset_builder=SuperStoreExtDSBuilder(entry_name="t32mwwpvun20k", conn_name="4ztka7stosdow"),
            chart_builders=[
                ChartBuilderSingleDataset(ext.Indicator(
                    ext.ChartField(ext.ChartFieldRef("sum_sales")),
                ))
                    .add_formula_field("sum_sales", ext.FieldType.float, "SUM([Sales])")
                    .with_entry_name("blk5lfc1b0302")
            ],
            dash_name="1bavcje1f478s"
        )
        CLUSTERIZE_NAME_MAP = [
            ext.NameMapEntry(
                entry_kind=ext.EntryKind.dataset, local_name="t32mwwpvun20k", unique_entry_id="t32mwwpvun20k",
                legacy_location=["External API samples", "Clusterize samples", "ds_sales_shared"]
            ),
            ext.NameMapEntry(
                entry_kind=ext.EntryKind.chart, local_name="blk5lfc1b0302", unique_entry_id="blk5lfc1b0302",
                legacy_location=["External API samples", "Clusterize samples", "wb_1", "chart_a_1"]
            ),
            ext.NameMapEntry(
                entry_kind=ext.EntryKind.dashboard, local_name="1bavcje1f478s", unique_entry_id="1bavcje1f478s",
                legacy_location=["External API samples", "Clusterize samples", "wb_1", "dash_a_1"]
            ),
            ext.NameMapEntry(
                entry_kind=ext.EntryKind.connection, local_name="4ztka7stosdow", unique_entry_id="4ztka7stosdow",
                legacy_location=["Yandex DataLens Demo", "DataLens Demo Dashboard", "Sample ClickHouse_44H"]
            ),
        ]

        return [
            UserOperationInfo(
                ext.WorkbookOpKind.wb_read,
                MText(
                    ru="Получить конфигурацию воркбука.",
                    en="Get workbook config."
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="Пустой воркбук",
                            en="Empty workbook"
                        ),
                        rq=ext.WorkbookReadRequest(workbook_id=READ_WB_ID),
                        rs=ext.WorkbookReadResponse(workbook=ext.WorkBook.create_empty())
                    ),
                ]
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.wb_create,
                MText(
                    ru="Создать воркбук. Будут созданы папка навигации и подключения, перечисленные в запросе."
                       " Сейчас можно создать только подключения.",
                    en="Create workbook. This creates navigation and connection folders listed in the query."
                       " Currently, you can create connections only.",
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="Пустой воркбук",
                            en="Empty workbook",
                        ),
                        rq=ext.FakeWorkbookCreateRequest(workbook_id=WRITE_SAFE_WB_ID, workbook=None),
                        rs=ext.FakeWorkbookCreateResponse(workbook_id=WRITE_SAFE_WB_ID, created_entries_info=[])
                    ),
                    OperationExample(
                        title=MText(
                            ru="Воркбук с CHYT-подключением",
                            en="Workbook with CHYT connection",
                        ),
                        rq=ext.FakeWorkbookCreateRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            workbook=ext.WorkbookConnectionsOnly(
                                connections=[CHYTConnectionBuilder().build_instance(DEFAULT_CONN_NAME)]
                            ),
                            # TODO FIX: secret is not serialized due to load-only
                            connection_secrets=[ext.ConnectionSecret(
                                conn_name=DEFAULT_CONN_NAME,
                                secret=ext.PlainSecret("My-Lovely-YT-token")
                            )],
                        ),
                        rs=ext.FakeWorkbookCreateResponse(
                            workbook_id=WRITE_SAFE_WB_ID,
                            created_entries_info=[
                                ext.EntryInfo(kind=ext.EntryKind.connection, id="as123m34", name=DEFAULT_CONN_NAME)
                            ],
                        ),
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
                       " This compares the current status of each object in the qorkbook with the configuration from the query."
                       " The object in the workbook not listed in the query will be deleted."
                       "\n\n"
                       "The proprties of the object edited in the UI and not bupported by the API will not be affected."
                       " This behavior can be changed by using the `force_rewrite` flag."
                       " This will rewrite all the objects, regardless of changes in the UI.",
                ),
                example_list=[
                    OperationExample(
                        rq=ext.WorkbookWriteRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            workbook=DEFAULT_WORKBOOK_BUILDER.build(),
                        ),
                        rs=ext.WorkbookWriteResponse(
                            executed_plan=ModificationPlanBuilderWorkbookBased(
                                DEFAULT_WORKBOOK_BUILDER.with_fill_defaults(),
                                existing_entry_names=[DEFAULT_DATASET_NAME, "some_entry_that_not_presented_in_rq"],
                                modified_entry_names=[DEFAULT_DATASET_NAME],
                            ).build(),
                            workbook=DEFAULT_WORKBOOK_BUILDER.with_fill_defaults().build()
                        ),
                    ),
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.connection_create,
                MText(
                    ru="Создать подключение в существующем воркбуке.",
                    en="Create connection in existing workbook.",
                ),
                example_list=[
                    OperationExample(
                        title=MText(
                            ru="CHYT",
                            en="CHYT",
                        ),
                        rq=ext.ConnectionCreateRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            connection=CHYTConnectionBuilder().build_instance(DEFAULT_CONN_NAME),
                            secret=ext.PlainSecret("My-Lovely-YT-token"),
                        ),
                        rs=ext.ConnectionCreateResponse(
                            ext.EntryInfo(kind=ext.EntryKind.connection, id="as123m34", name=DEFAULT_CONN_NAME)
                        )
                    ),
                    OperationExample(
                        title=MText(
                            ru="CHYT User Auth",
                            en="CHYT User Auth",
                        ),
                        rq=ext.ConnectionCreateRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            connection=CHYTConnectionBuilder(is_user_auth=True).build_instance(DEFAULT_CONN_NAME),
                            secret=None,
                        ),
                        rs=ext.ConnectionCreateResponse(
                            ext.EntryInfo(kind=ext.EntryKind.connection, id="as123m34", name=DEFAULT_CONN_NAME)
                        )
                    ),
                    OperationExample(
                        title=MText(
                            ru="ClickHouse",
                            en="ClickHouse",
                        ),
                        rq=ext.ConnectionCreateRequest(
                            workbook_id=WRITE_SAFE_WB_ID,
                            connection=CHConnectionBuilder(
                                raw_sql_level=ext.RawSQLLevel.subselect
                            ).build_instance(DEFAULT_CONN_NAME),
                            secret=ext.PlainSecret("My-Str0Ng-pass"),
                        ),
                        rs=ext.ConnectionCreateResponse(
                            ext.EntryInfo(kind=ext.EntryKind.connection, id="as123m34", name=DEFAULT_CONN_NAME)
                        )
                    ),
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
                        rq=ext.AdviseDatasetFieldsRequest(
                            partial_dataset=SuperStoreExtDSBuilder(
                                "%this_name_will_not_be_taken_in_account%"
                            ).set_source_as_sub_select().build(),
                            connection_ref=ext.EntryIDRef(id="%my_connection_id%"),
                        ),
                        rs=ext.AdviseDatasetFieldsResponse(
                            dataset=SuperStoreExtDSBuilder(
                                DEFAULT_CONN_NAME
                            ).do_add_default_fields().set_source_as_sub_select().with_fill_defaults().build()
                        ),
                    )
                ],
            ),
            UserOperationInfo(
                ext.WorkbookOpKind.wb_clusterize,
                MText(
                    ru="Кластеризовать воркбук.",
                    en="Clusterize workbook",
                ),
                example_list=[
                    OperationExample(
                        rq=ext.WorkbookClusterizeRequest(
                            dash_id_list=["1bavcje1f478s"],
                        ),
                        rs=ext.WorkbookClusterizeResponse(
                            workbook=CLUSTERIZE_WB_BUILDER.build(),
                            name_map=CLUSTERIZE_NAME_MAP,
                        ),
                    ),
                    OperationExample(
                        rq=ext.WorkbookClusterizeRequest(
                            navigation_folder_path="External API samples/Clusterize samples/wb_1",
                        ),
                        rs=ext.WorkbookClusterizeResponse(
                            workbook=CLUSTERIZE_WB_BUILDER.build(),
                            name_map=CLUSTERIZE_NAME_MAP,
                        ),
                    ),
                ],
            ),
        ]
