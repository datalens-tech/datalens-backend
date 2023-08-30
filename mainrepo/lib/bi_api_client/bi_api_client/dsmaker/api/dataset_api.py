from __future__ import annotations

from http import HTTPStatus
from itertools import chain
from typing import Any, Optional, Union, Iterable

import attr

from bi_api_client.dsmaker.primitives import (
    UpdateAction, Dataset, ResultField,
)
from bi_api_client.dsmaker.api.base import HttpApiResponse
from bi_api_client.dsmaker.api.http_sync_base import SyncHttpApiV1Base
from bi_api_client.dsmaker.api.serialization_base import BaseApiV1SerializationAdapter
from bi_api_client.dsmaker.api.schemas.dataset import (
    ResultFieldSchema, ResultSchemaAuxSchema,
    DataSourceSchema, SourceAvatarSchema, AvatarRelationSchema,
    ObligatoryFilterSchema, ComponentErrorListSchema,
)


@attr.s(frozen=True)
class HttpDatasetApiResponse(HttpApiResponse):
    _dataset: Optional[Dataset] = attr.ib(kw_only=True, default=None)

    @property
    def dataset(self) -> Dataset:
        assert self._dataset is not None
        return self._dataset

    @classmethod
    def extract_response_errors(cls, response_json: dict[str, Any]) -> list[str]:
        errors = super().extract_response_errors(response_json)
        for key, value in chain.from_iterable((
            response_json.items(),
            response_json.get('dataset', {}).items()
        )):
            if key.endswith('errors') and value:
                errors.append(f'{key}: {value}')

        return errors


@attr.s
class DatasetApiV1SerializationAdapter(BaseApiV1SerializationAdapter):
    @staticmethod
    def load_dataset_from_response_body(dataset: Dataset, body: dict) -> Dataset:
        """
        Load dataset contents from response

        :param dataset: old dataset object, used for loading aliases of nested items and other properties
        :param body: API response body
        """
        new_id = body.get('id') or dataset.id
        new_name = body.get('name', dataset.name)
        body = body['dataset']
        new_dataset = Dataset(
            created_=dataset.created_,
            name=new_name,
            id=new_id,
            revision_id=body.get('revision_id'),
            rls=body.get('rls') or {},
            component_errors=ComponentErrorListSchema().load(body.get('component_errors') or {}),
            result_schema_aux=ResultSchemaAuxSchema().load(body.get('result_schema_aux') or {}),
            obligatory_filters=[
                ObligatoryFilterSchema().load(filter_info) for filter_info in body['obligatory_filters']
            ],
        )

        for dsrc_data in body['sources']:
            source_id = dsrc_data['id']
            source_alias = dataset.sources.get_alias(source_id) or source_id
            new_dataset.sources[source_alias] = DataSourceSchema().load(dsrc_data)

        for avatar_data in body['source_avatars']:
            avatar_id = avatar_data['id']
            avatar_alias = dataset.source_avatars.get_alias(avatar_id) or avatar_id
            new_dataset.source_avatars[avatar_alias] = SourceAvatarSchema().load(avatar_data)

        for relation_data in body['avatar_relations']:
            relation_id = relation_data['id']
            relation_alias = dataset.avatar_relations.get_alias(relation_id) or relation_id
            new_dataset.avatar_relations[relation_alias] = AvatarRelationSchema().load(relation_data)

        for field_data in body['result_schema']:
            field_id = field_data['guid']
            field_alias = dataset.result_schema.get_alias(field_id) or field_id
            new_dataset.result_schema[field_alias] = ResultFieldSchema().load(field_data)

        return new_dataset


@attr.s
class SyncHttpDatasetApiV1(SyncHttpApiV1Base):
    _created_dataset_id_list: list[str] = attr.ib(init=False, factory=list)
    # Override serial_adapter with subclass
    serial_adapter: DatasetApiV1SerializationAdapter = attr.ib(init=False, factory=DatasetApiV1SerializationAdapter)

    def save_dataset(
            self, dataset: Dataset,
            preview: bool = True,
            fail_ok: bool = False,
            dir_path: str = None,
            workbook_id: str = None,
            created_via: str = None,
            lock_timeout: int = 3,
    ) -> HttpDatasetApiResponse:
        dataset.prepare()

        if self.serial_adapter.generate_implicit_updates(dataset=dataset):
            raise RuntimeError('Dataset has pending updates that have not been applied yet!')

        data = dict(self.dump_dataset_to_request_body(dataset), preview=preview)
        if not dataset.created_:
            if dir_path is not None and workbook_id is not None:
                raise RuntimeError("workbook_id and dir_path can not be used simultaneously")
            if workbook_id is not None:
                data['workbook_id'] = workbook_id
            if dir_path is not None:
                data['dir_path'] = dir_path
            if created_via is not None:
                data['created_via'] = created_via
            response = self._request(
                '/api/v1/datasets', method='post',
                data=dict(data, name=dataset.name),
                lock_timeout=lock_timeout,
            )

        else:
            response = self._request(
                f'/api/v1/datasets/{dataset.id}/versions/draft', method='put',
                data=data,
                lock_timeout=lock_timeout,
            )

        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        else:
            # `fail_ok` should not mean `allow 5xx`
            assert response.status_code < 500, response.json

        new_dataset: Optional[Dataset] = None
        if response.status_code == HTTPStatus.OK:
            new_dataset = self.serial_adapter.load_dataset_from_response_body(dataset=dataset, body=response.json)
            if not dataset.created_:
                new_dataset_id = response.json['id']
                new_dataset.created_ = True
                new_dataset.id = new_dataset_id
                self._created_dataset_id_list.append(new_dataset_id)

        return HttpDatasetApiResponse(
            json=response.json,
            status_code=response.status_code,
            dataset=new_dataset,
        )

    def delete_dataset(self, dataset_id: str, fail_ok: bool = False) -> None:
        response = self._request(
            f'/api/v1/datasets/{dataset_id}/versions/draft', method='delete',
        )
        if not fail_ok:
            assert response.status_code == HTTPStatus.OK, response.json
        else:
            # `fail_ok` should not mean `allow 5xx`
            assert response.status_code < 500, response.json

    def cleanup_created_resources(self):  # type: ignore  # TODO: fix
        for dataset_id in self._created_dataset_id_list:
            try:
                self.delete_dataset(dataset_id, fail_ok=True)
            except Exception:  # noqa
                pass

    def copy_dataset(
            self,
            dataset: Dataset,
            new_key: str,
    ) -> HttpDatasetApiResponse:
        data = {
            'new_key': new_key
        }

        response = self._request(f'/api/v1/datasets/{dataset.id}/copy', method='post', data=data)
        assert response.status_code == 200, f"Dataset copy fail: {response.data}"
        dataset = self.serial_adapter.load_dataset_from_response_body(dataset=dataset, body=response.json)
        return HttpDatasetApiResponse(
            json=response.json,
            status_code=response.status_code,
            dataset=dataset,
        )

    def load_dataset(self, dataset: Dataset) -> HttpDatasetApiResponse:
        response = self._request(f'/api/v1/datasets/{dataset.id}/versions/draft', method='get')
        dataset = self.serial_adapter.load_dataset_from_response_body(dataset=dataset, body=response.json)
        dataset.created_ = True
        return HttpDatasetApiResponse(
            json=response.json,
            status_code=response.status_code,
            dataset=dataset,
        )

    def apply_updates(  # type: ignore  # TODO: fix
            self, dataset: Dataset,
            updates: list[Union[UpdateAction, dict]] = None,
            fail_ok: bool = False,
    ) -> HttpDatasetApiResponse:
        if dataset.created_:
            url = f'/api/v1/datasets/{dataset.id}/versions/draft/validators/schema'
        else:
            url = '/api/v1/datasets/validators/dataset'

        data = self.dump_dataset_to_request_body(dataset)
        updates = list(updates or ()) + self.serial_adapter.generate_implicit_updates(dataset)
        data['updates'] = self.serial_adapter.dump_updates(updates)
        response = self._request(url, method='post', data=data)

        if fail_ok:
            # `fail_ok` should not mean `allow 5xx`
            assert response.status_code < 500, response.json
        else:
            try:
                assert response.status_code == HTTPStatus.OK, response.json
            except AssertionError:
                print('\n'.join(HttpApiResponse.extract_response_errors(response)))
                raise

        if 'dataset' in response.json:
            dataset = self.serial_adapter.load_dataset_from_response_body(dataset=dataset, body=response.json)
        return HttpDatasetApiResponse(
            json=response.json,
            status_code=response.status_code,
            dataset=dataset,
        )

    def validate_field(self, dataset: Dataset, field: ResultField) -> HttpDatasetApiResponse:
        if dataset.created_:
            url = f'/api/v1/datasets/{dataset.id}/versions/draft/validators/field'
        else:
            url = '/api/v1/datasets/validators/field'

        if not field.id:
            field.prepare()

        data = self.dump_dataset_to_request_body(dataset)
        data.update({'field': {'guid': field.id, 'title': field.title, 'formula': field.formula}})
        response = self._request(url, method='post', data=data)
        return HttpDatasetApiResponse(
            json=response.json,
            status_code=response.status_code,
            dataset=None,
        )

    def refresh_dataset_sources(
            self,
            dataset: Dataset,
            source_ids: Iterable[str],
            fail_ok: bool = False,
    ) -> HttpDatasetApiResponse:
        refresh_resp = self.apply_updates(dataset=dataset, fail_ok=fail_ok, updates=[
            {
                'action': 'refresh_source',
                'source': {
                    'id': source_id,
                }
            } for source_id in source_ids
        ])
        return refresh_resp
