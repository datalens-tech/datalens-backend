from dl_configs.settings_submodels import S3Settings
from dl_core_testing.database import DbTable
from dl_testing.s3_utils import s3_tbl_func_maker

from dl_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO


def create_s3_native_from_ch_table(
    filename: str,
    s3_bucket: str,
    s3_settings: S3Settings,
    clickhouse_table: DbTable,
    tbl_schema: str,
) -> None:
    tbl = clickhouse_table
    db = clickhouse_table.db
    s3_tbl_func = s3_tbl_func_maker(s3_settings)
    s3_tbl_func_for_db = s3_tbl_func(
        for_="db",
        conn_dto=BaseFileS3ConnDTO(
            conn_id=None,
            protocol="http",
            host=db.url.host,  # type: ignore  # 2024-01-24 # TODO: Argument "host" to "BaseFileS3ConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
            port=db.url.port,  # type: ignore  # 2024-01-24 # TODO: Argument "port" to "BaseFileS3ConnDTO" has incompatible type "int | None"; expected "int"  [arg-type]
            username=db.url.username,  # type: ignore  # 2024-01-24 # TODO: Argument "username" to "BaseFileS3ConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
            password=db.url.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "BaseFileS3ConnDTO" has incompatible type "object"; expected "str"  [arg-type]
            multihosts=db.get_conn_hosts(),
            s3_endpoint="http://s3-storage:8000",
            access_key_id=s3_settings.ACCESS_KEY_ID,
            secret_access_key=s3_settings.SECRET_ACCESS_KEY,
            bucket=s3_bucket,
            replace_secret="WE_ONLY_NEED_DB_MODE_HERE_SO_NO_SECRET",
        ),
        filename=filename,
        file_fmt="Native",
        schema_line=tbl_schema,
    )
    insert_stmt = f"INSERT INTO FUNCTION {s3_tbl_func_for_db} SELECT * FROM {tbl.db.quote(tbl.name)}"
    db.execute(insert_stmt)
