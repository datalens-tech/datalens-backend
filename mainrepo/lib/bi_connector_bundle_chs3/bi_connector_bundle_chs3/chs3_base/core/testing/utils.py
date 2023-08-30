from bi_configs.settings_submodels import S3Settings

from bi_testing.s3_utils import s3_tbl_func_maker

from bi_core_testing.database import DbTable

from bi_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO


def create_s3_native_from_ch_table(
        filename: str,
        s3_bucket: str,
        s3_settings: S3Settings,
        clickhouse_table: DbTable,
        tbl_schema: str,
        double_data: bool = False,
) -> None:
    # FIXME: Move to chs3 connectors
    tbl = clickhouse_table
    db = clickhouse_table.db
    s3_tbl_func = s3_tbl_func_maker(s3_settings)
    s3_tbl_func_for_db = s3_tbl_func(
        for_='db',
        conn_dto=BaseFileS3ConnDTO(
            conn_id=None,
            protocol='http',
            host=db.url.host,
            port=db.url.port,
            username=db.url.username,
            password=db.url.password,
            multihosts=db.get_conn_hosts(),  # type: ignore

            s3_endpoint='http://s3-storage:8000',
            access_key_id=s3_settings.ACCESS_KEY_ID,
            secret_access_key=s3_settings.SECRET_ACCESS_KEY,
            bucket=s3_bucket,
            replace_secret='WE_ONLY_NEED_DB_MODE_HERE_SO_NO_SECRET',
        ),
        filename=filename,
        file_fmt='Native',
        schema_line=tbl_schema,  # TODO: update DbTable to serve some sort of schema
    )
    insert_stmt = f'INSERT INTO FUNCTION {s3_tbl_func_for_db} SELECT * FROM {tbl.db.quote(tbl.name)}'
    if double_data:
        insert_stmt += f' UNION ALL SELECT * FROM {tbl.db.quote(tbl.name)}'
    db.execute(insert_stmt)
