from __future__ import annotations

import base64
import json
from typing import Optional, Tuple

import sqlalchemy_bigquery._types as bq_types
from google.cloud.bigquery import Client as BQClient
from google.auth.credentials import Credentials as BQCredentials
import google.oauth2.service_account as g_service_account
import sqlalchemy as sa

from bi_core.connection_models.common_models import DBIdent, SchemaIdent, TableIdent
from bi_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter, BaseConnLineConstructor
from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery

from bi_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from bi_connector_bigquery.core.target_dto import BigQueryConnTargetDTO
from bi_connector_bigquery.core.error_transformer import big_query_db_error_transformer


class BigQueryConnLineConstructor(BaseConnLineConstructor[BigQueryConnTargetDTO]):
    def _get_dsn_params(
        self,
        safe_db_symbols: Tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        return {
            'dialect': self._dialect_name,
            'project_id': self._target_dto.project_id,
        }


class BigQueryDefaultAdapter(BaseClassicAdapter[BigQueryConnTargetDTO]):
    conn_type = CONNECTION_TYPE_BIGQUERY
    dsn_template = '{dialect}://{project_id}'
    conn_line_constructor_type = BigQueryConnLineConstructor
    _error_transformer = big_query_db_error_transformer

    _type_code_to_sa = {
        'STRING': bq_types.STRING,
        'DATE': bq_types.DATE,
        'DATETIME': bq_types.DATETIME,
        'BOOL': bq_types.BOOL,
        'BOOLEAN': bq_types.BOOLEAN,
        'INTEGER': bq_types.INTEGER,
        'INT64': bq_types.INT64,
        'FLOAT': bq_types.FLOAT,
        'FLOAT64': bq_types.FLOAT64,
        'NUMERIC': bq_types.NUMERIC,
        'BIGNUMERIC': bq_types.BIGNUMERIC,
        # TODO: ARRAY
    }

    def get_default_db_name(self) -> Optional[str]:
        return self._target_dto.project_id

    def get_engine_kwargs(self) -> dict:
        return {
            'credentials_base64': self._target_dto.credentials,
        }

    def _get_bq_credentials(self) -> BQCredentials:
        credentials_info = json.loads(base64.b64decode(self._target_dto.credentials))
        credentials = g_service_account.Credentials.from_service_account_info(
            credentials_info
        )
        return credentials

    def get_bq_client(self) -> BQClient:
        client = BQClient(credentials=self._get_bq_credentials())
        return client

    def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        client = self.get_bq_client()
        bq_datasets = list(client.list_datasets())
        project_id = self._target_dto.project_id
        db_engine = self.get_db_engine(db_name=project_id)
        quoter = db_engine.dialect.identifier_preparer.quote

        subqueries = [
            sa.select([
                sa.literal_column('project_id'),
                sa.literal_column('dataset_id'),
                sa.literal_column('table_id'),
            ]).select_from(
                sa.text(quoter('{project_id}.{dataset_id}.__TABLES__'.format(
                    project_id=project_id, dataset_id=dataset_info.dataset_id
                )))
            )
            for dataset_info in bq_datasets
        ]
        query = DBAdapterQuery(query=sa.union_all(*subqueries))
        result = self.execute(query)
        return [
            TableIdent(
                db_name=_project_id,
                schema_name=dataset_id,
                table_name=table_name,
            )
            for _project_id, dataset_id, table_name in result.get_all()
        ]

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return None
