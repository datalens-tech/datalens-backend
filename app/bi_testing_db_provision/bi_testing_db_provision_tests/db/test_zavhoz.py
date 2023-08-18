import pytest

from bi_testing_db_provision.model.enums import ResourceType, ResourceState, SessionState, ResourceKind
from bi_testing_db_provision.model.request_db import DBRequest
from bi_testing_db_provision.model.resource_docker import StandalonePostgresResource
from bi_testing_db_provision.model.session import Session
from bi_testing_db_provision.dao.resource_dao import ResourceDAO
from bi_testing_db_provision.zavhoz import ZavHoz, ResourcePublicRequest, SessionRequest

pytestmark = pytest.mark.usefixtures("tdbp_clean_resources")


@pytest.mark.asyncio
async def test_zavhoz_session_create(tdbp_pg_conn_factory):
    conn = await tdbp_pg_conn_factory.get()
    zavhoz = ZavHoz(conn=conn)

    session_request = SessionRequest(description="test session")
    session = await zavhoz.start_session(session_request)

    assert session == Session(
        id=session.id,
        create_ts=session.create_ts,
        update_ts=session.update_ts,
        state=SessionState.active,
        description=session_request.description,
    )

    await zavhoz.stop_session(session.id)


@pytest.mark.asyncio
async def test_zavhoz_no_free_resource(tdbp_pg_conn_factory):
    conn = await tdbp_pg_conn_factory.get()
    zavhoz = ZavHoz(conn=conn)

    session = await zavhoz.start_session(SessionRequest("test session"))

    public_rq = ResourcePublicRequest(
        resource_type=ResourceType.standalone_postgres,
        resource_kind=None,
        resource_request=DBRequest(
            version='9.3',
        )
    )

    initial_resource = await zavhoz.acquire_resource(public_rq, session.id)
    assert initial_resource.session_id == session.id
    assert initial_resource.state == ResourceState.create_required

    await zavhoz.stop_session(session.id)


@pytest.mark.asyncio
async def test_zavhoz_has_free_resource(tdbp_pg_conn_factory):
    conn = await tdbp_pg_conn_factory.get()
    zavhoz = ZavHoz(conn=conn)

    session = await zavhoz.start_session(SessionRequest("test session"))
    assert session.id

    public_rq = ResourcePublicRequest(
        resource_type=ResourceType.standalone_postgres,
        resource_kind=ResourceKind.single_docker,
        resource_request=DBRequest(
            version='9.3',
        )
    )

    # Creating matching free resource manually
    resource_dao = ResourceDAO(await tdbp_pg_conn_factory.get())
    expected_resource = await resource_dao.create(StandalonePostgresResource.create(
        resource_id=None,
        request=public_rq.resource_request,
        state=ResourceState.free,
    ))

    actual_resource = await zavhoz.acquire_resource(public_rq, session.id)

    # Checking that we got resource that we create previously
    assert actual_resource.id == expected_resource.id
    assert actual_resource.session_id == session.id
    assert actual_resource.state == ResourceState.acquired

    await zavhoz.stop_session(session.id)
