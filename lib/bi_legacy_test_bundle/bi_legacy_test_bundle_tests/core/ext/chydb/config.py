from bi_constants.enums import CreateDSFrom


CHYDB_TEST_DATASET_PARAMS_BASE = dict(
    created_from=CreateDSFrom.CHYDB_TABLE,
    dsrc_params=dict(
        ydb_cluster='ru',
        ydb_database='/ru/home/hhell/mydb',
    ),
    # https://ydb.yandex-team.ru/db/ydb-ru/home/hhell/mydb/browser/some_dir/test_table_e
    # made using
    # https://yql.yandex-team.ru/Operations/Xsv2XFPzVMVQm_4SJ-MB7k8wY2bOqiNOmElm8kuUkTE=
    table_name='some_dir/test_table_e',
)


CHYDB_TEST_TABLE_SQL = "ydbTable('{}', '{}', '{}')".format(
    CHYDB_TEST_DATASET_PARAMS_BASE['dsrc_params']['ydb_cluster'],
    CHYDB_TEST_DATASET_PARAMS_BASE['dsrc_params']['ydb_database'],
    CHYDB_TEST_DATASET_PARAMS_BASE['table_name'])
