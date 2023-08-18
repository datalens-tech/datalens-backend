import logging
from datetime import timezone, datetime
from typing import Optional, Dict, Any, ClassVar
from uuid import UUID

import attr
import sqlalchemy as sa
from aiopg.sa.result import RowProxy

from bi_testing_db_provision.dao.base_dao import BaseDAO
from bi_testing_db_provision.db.resource import ResourceRecord
from bi_testing_db_provision.model.enums import ResourceKind, ResourceState, ResourceType
from bi_testing_db_provision.model.request_base import ResourceRequest
from bi_testing_db_provision.model.resource import ResourceBase
from bi_testing_db_provision.schemas.common.request import GenericResourceRequestSchema
from bi_testing_db_provision.schemas.common.resource_external_data import (
    GenericResourceExternalDataSchema
)
from bi_testing_db_provision.schemas.storage.resource_internal_data import (
    GenericResourceInternalDataSchema
)

LOGGER = logging.getLogger(__name__)


@attr.s
class ResourceDAO(BaseDAO[ResourceBase]):
    record_cls = ResourceRecord
    update_notification_channel: ClassVar[str] = 'resource_update'

    @staticmethod
    def _resource_hash_uuid(resource_hash_bytes: bytes) -> UUID:
        return UUID(bytes=b'\x00' * (16 - len(resource_hash_bytes)) + resource_hash_bytes)

    def _model_to_record(self, resource: ResourceBase, for_create: bool) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        request_hash = resource.request.get_hash()
        assert len(request_hash) <= 16
        values = dict(
            resource_kind=resource.kind,
            resource_type=resource.type,
            resource_state=resource.state,
            resource_internal_data=(
                None if resource.internal_data is None
                else GenericResourceInternalDataSchema().dump(resource.internal_data)
            ),
            resource_external_data=(
                None if resource.external_data is None
                else GenericResourceExternalDataSchema().dump(resource.external_data)
            ),
            resource_request=GenericResourceRequestSchema().dump(resource.request),
            resource_request_hash=self._resource_hash_uuid(request_hash),
            session_id=resource.session_id,
            update_ts=now,
        )
        if for_create:
            values.update(
                id=resource.id,
                create_ts=now,
            )
        return values

    def _record_to_model(self, record: Optional[RowProxy]) -> Optional[ResourceBase]:  # type: ignore  # TODO: fix
        if record is None:
            return None

        r_kind = record['resource_kind']
        r_type = record['resource_type']
        r_cls = ResourceBase.get_resource_cls(r_kind, r_type)

        internal_data = record['resource_internal_data']
        external_data = record['resource_external_data']

        return r_cls.create(
            resource_id=record['id'],
            state=record['resource_state'],
            request=GenericResourceRequestSchema().load(record['resource_request']),
            internal_data=(
                None if internal_data is None
                else GenericResourceInternalDataSchema().load(internal_data)
            ),
            external_data=(
                None if external_data is None
                else GenericResourceExternalDataSchema().load(external_data)
            ),
            session_id=record['session_id'],
        )

    def _model_to_notification_event(self, model: ResourceBase) -> Dict[str, Any]:
        return dict(
            id=model.id.hex,  # type: ignore  # TODO: fix
            state=model.state.name,
        )

    async def wait_for_resource_update(
            self,
            resource_id: Optional[UUID] = None,
            target_state: Optional[ResourceState] = None,
            timeout: Optional[float] = None,  # TODO FIX: Threat as total timeout
    ) -> dict:
        assert not self._conn.in_transaction, "Notifications will not be received inside transaction"

        await self._conn.subscribe(self.update_notification_channel)

        while True:
            event = await self._conn.wait_for_single_event(self.update_notification_channel, timeout=timeout)
            if resource_id is not None and event.get('id') != resource_id:
                continue
            if target_state is not None and event.get('state') != target_state.name:
                continue

            return event

    async def find_and_lock_free_resource(
            self,
            resource_type: ResourceType,
            request: ResourceRequest,
            resource_kind: Optional[ResourceKind] = None,
            timeout: Optional[float] = None,
    ) -> Optional[ResourceBase]:
        assert timeout is None, "Waiting for locked resources is not supported yet"

        request_hash = self._resource_hash_uuid(request.get_hash())
        search_predicate = [
            ResourceRecord.resource_type == resource_type,
            ResourceRecord.resource_request_hash == request_hash,
            ResourceRecord.resource_state == ResourceState.free,
        ]
        if resource_kind is not None:
            search_predicate.append(ResourceRecord.resource_kind == resource_kind)

        return await self.find_and_lock_resource(sa.and_(*search_predicate))

    async def find_and_lock_resource(self, where_clause: Any) -> Optional[ResourceBase]:
        result = await self._conn.execute(
            ResourceRecord.select_()
                .where(where_clause)
                .limit(1)
                .with_for_update(skip_locked=True)
        )

        row = await result.fetchone()
        result.close()

        return self._record_to_model(row)

    async def update_resource_state_for_session(self, session_id: UUID, new_state: ResourceState) -> None:
        assert session_id is not None

        result = await self._conn.execute(
            ResourceRecord.select_()
                .where(ResourceRecord.session_id == session_id)
                .with_for_update()
        )
        rows = await result.fetchall()
        resource_list = [self._record_to_model(row) for row in rows]

        if not resource_list:
            return

        # Check that transition is OK
        for resource in resource_list:
            resource.clone(state=new_state)  # type: ignore  # TODO: fix

        await self._conn.execute(
            ResourceRecord.update_()
                .values(resource_state=new_state)
                .where(ResourceRecord.session_id == session_id)
        )
        # TODO FIX: Send event
