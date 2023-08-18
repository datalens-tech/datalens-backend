from __future__ import annotations

import asyncio
from typing import Tuple, ClassVar, Sequence, Dict, Optional
from uuid import UUID

import attr

from bi_testing_db_provision.dao.resource_dao import ResourceDAO
from bi_testing_db_provision.dao.session_dao import SessionDAO
from bi_testing_db_provision.db_connection import DBConn
from bi_testing_db_provision.model.enums import SessionState, ResourceState, ResourceKind, ResourceType
from bi_testing_db_provision.model.request_base import ResourceRequest
from bi_testing_db_provision.model.resource import ResourceBase
from bi_testing_db_provision.model.session import Session


@attr.s
class SessionRequest:
    description: str = attr.ib()


@attr.s
class ResourcePublicRequest:
    resource_type: ResourceType = attr.ib()
    resource_request: ResourceRequest = attr.ib()
    resource_kind: Optional[ResourceKind] = attr.ib()


class ResourceReceiptFactory:
    map_r_type_r_pref_list: ClassVar[Dict[ResourceType, Sequence[ResourceKind]]] = {
        ResourceType.standalone_postgres: (ResourceKind.single_docker,),
        ResourceType.standalone_clickhouse: (ResourceKind.single_docker,),
        ResourceType.standalone_mssql: (ResourceKind.single_docker,),
        ResourceType.standalone_oracle: (ResourceKind.single_docker,),
    }

    def preprocess_resource_request(
            self,
            public_request: ResourcePublicRequest,
            preference_order: int = 0,
    ) -> Tuple[ResourceKind, ResourceRequest]:
        r_kind_pref_seq = self.map_r_type_r_pref_list[public_request.resource_type]

        try:
            r_kind = r_kind_pref_seq[preference_order]
        except IndexError as exc:
            raise ValueError(f"Preference {preference_order} not available for {public_request.resource_type}") from exc
        else:
            # TODO FIX: Validate & set defaults for request
            return r_kind, public_request.resource_request

    def make_resource(self, r_kind: ResourceKind, r_type: ResourceType, r_req: ResourceRequest) -> ResourceBase:
        return ResourceBase.get_resource_cls(r_kind, r_type).create(
            resource_id=None,
            request=r_req,
            state=ResourceState.create_required,
        )


@attr.s()
class ZavHoz:
    _session_dao: SessionDAO = attr.ib(kw_only=True, default=None)
    _resource_dao: ResourceDAO = attr.ib(kw_only=True, default=None)
    _conn: DBConn = attr.ib(kw_only=True, default=None)
    _resource_reciept_factory: ResourceReceiptFactory = attr.ib(kw_only=True, factory=ResourceReceiptFactory)

    def __attrs_post_init__(self) -> None:
        if self._conn is None:
            assert self._session_dao is not None and self._resource_dao is not None
            assert self._session_dao.conn is self._resource_dao.conn
            self._conn = self._session_dao.conn
        else:
            assert self._session_dao is None and self._resource_dao is None
            self._session_dao = SessionDAO(self._conn)
            self._resource_dao = ResourceDAO(self._conn)

    async def get_locked_session(self, session_id: UUID, should_be_active: bool) -> Session:
        assert self._conn.in_transaction, "Attempt to get locked session outside of transaction"
        session = await self._session_dao.get_by_id(session_id, for_update=True)
        if session is None:
            raise ValueError("Session not found")
        if should_be_active and session.state != SessionState.active:
            raise ValueError("Session is not active")
        return session

    async def start_session(self, session_request: SessionRequest) -> Session:
        async with self._conn.begin():
            session = Session(
                id=None,
                state=SessionState.active,
                description=session_request.description,
            )
            session = await self._session_dao.create(session)
            return session

    async def stop_session(self, session_id: UUID) -> None:
        async with self._conn.begin():
            session = await self.get_locked_session(session_id, should_be_active=True)
            session = session.clone(state=SessionState.deleted)
            await self._session_dao.update(session)
            await self._resource_dao.update_resource_state_for_session(session_id, ResourceState.recycle_required)

    async def acquire_resource(self, public_request: ResourcePublicRequest, session_id: UUID) -> ResourceBase:
        async with self._conn.begin():
            session = await self.get_locked_session(session_id, should_be_active=True)
            assert session.state == SessionState.active

            r_type = public_request.resource_type
            r_kind, normalized_request = self._resource_reciept_factory.preprocess_resource_request(public_request)

            # Trying to find free resource
            resource = await self._resource_dao.find_and_lock_free_resource(
                resource_type=r_type,
                resource_kind=r_kind,
                request=normalized_request,
                timeout=None,
            )

            # Free resource was found case
            if resource is not None:
                assert resource.state == ResourceState.free
                resource = resource.clone(
                    state=ResourceState.acquired,
                    session_id=session_id,
                )
                resource = await self._resource_dao.update(resource)
                return resource

            # No free resource. Provisioning new one
            # TODO FIX: Check quotas/limits
            resource = self._resource_reciept_factory.make_resource(r_kind, r_type, normalized_request)
            assert resource.state == ResourceState.create_required

            resource = resource.clone(session_id=session_id)
            resource = await self._resource_dao.create(resource)

            return resource

    async def wait_for_resource_ready(self, resource_id: UUID) -> ResourceBase:
        try:
            return await asyncio.wait_for(self._wait_for_resource_ready(resource_id), timeout=60.)
        except asyncio.TimeoutError:
            raise ValueError("Resource wait timeout. Try again later...")

    async def _wait_for_resource_ready(self, resource_id: UUID) -> ResourceBase:
        while True:
            resource = await self._resource_dao.get_by_id(resource_id)
            assert resource is not None
            if resource.state in (ResourceState.acquired, ResourceState.deleted):
                return resource
            try:
                await self._resource_dao.wait_for_resource_update(resource_id, timeout=10.)
            except asyncio.TimeoutError:
                continue
