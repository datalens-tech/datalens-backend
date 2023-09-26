from __future__ import annotations

import asyncio
import datetime
import logging
import random
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)
import uuid

import attr
import pytest
import pytz
import sqlalchemy as sa

from bi_legacy_test_bundle_tests.core.utils import (
    assert_no_warnings,
    get_dump_request_profile_records,
)
from dl_api_commons.reporting.profiler import (
    PROFILING_LOG_NAME,
    DefaultReportingProfiler,
)
from dl_constants.enums import (
    DataSourceRole,
    SelectorType,
    UserDataType,
)
from dl_core.data_processing.cache.engine import (
    EntityCacheEngineAsync,
    ResultCacheEntry,
)
from dl_core.data_processing.cache.primitives import (
    CacheTTLConfig,
    LocalKeyRepresentation,
)
from dl_core.data_processing.dashsql import DashSQLCachedSelector
from dl_core.data_processing.selectors.db import DatasetDbDataSelectorAsync
from dl_core.data_processing.streaming import AsyncChunked
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import ExpressionCtx
from dl_core.services_registry import ServicesRegistry
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.data import DataFetcher
from dl_core_testing.dataset_wrappers import DatasetTestWrapper
from dl_core_testing.utils import SROptions
from dl_testing.utils import get_log_record
from dl_utils.aio import await_sync


if TYPE_CHECKING:
    from redis.asyncio import Redis

    from dl_core.us_connection_base import ClassicConnectionSQL
    from dl_core.us_dataset import Dataset


# To use fixture "loop" in each test in module
#  Cause: if any other test before will use it default loop for thread will be set to None
pytestmark = pytest.mark.usefixtures("loop", "app_context")


def pytz_local_now(tz_name):
    utc_now = datetime.datetime.now(pytz.utc)
    return utc_now.astimezone(pytz.timezone(tz_name))


@attr.s
class CachePseudoKeyParts:
    ds_id: str = attr.ib(factory=lambda: str(uuid.uuid4()))
    conn_id: str = attr.ib(factory=lambda: str(uuid.uuid4()))
    conn_revision = attr.ib(factory=lambda: "".join(random.choice("0123456789ABCDEFabcdef") for _ in range(16)))
    database_name = attr.ib(factory=lambda: "".join(random.choice("0123456789ABCDEFabcdef") for _ in range(16)))


def make_cache_key(
    connection_id: str,
    connection_revision_id: str,
    db_name: str,
    compiled_query: str,
) -> LocalKeyRepresentation:
    cache_key = LocalKeyRepresentation()
    cache_key = cache_key.extend(part_type="connection_id", part_content=connection_id)
    cache_key = cache_key.extend(part_type="connection_revision_id", part_content=connection_revision_id)
    cache_key = cache_key.extend(part_type="db_name", part_content=db_name)
    cache_key = cache_key.extend(part_type="query", part_content=compiled_query)
    return cache_key


# @pytest.mark.parametrize("compress_alg", [m for m in CompressAlg])
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "compiled_query, compress, data",
    [
        (
            "SELECT some_int, some_float, some_str, some_bool",
            False,
            [[1, 1.0, "aza", False], [None, None, None, True], [2, 2.356, "olo", True]],
        ),
        (
            "SELECT some_datetime, some_date",
            False,
            [
                [datetime.datetime.utcnow(), datetime.date(2016, 12, 1)],
            ],
        ),
        (
            "SELECT some_int, some_float, some_str, some_bool",
            True,
            [[1, 1.0, "aza", False], [None, None, None, True], [2, 2.356, "olo", True]],
        ),
        (
            "SELECT some_datetime, some_date",
            True,
            [
                [datetime.datetime.utcnow(), datetime.date(2016, 12, 1)],
            ],
        ),
        # Datetime serialization
        ("SELECT dt_pytz_geo", True, [[pytz_local_now("Europe/Moscow")]]),
        (
            "SELECT dt_std_tz_fixed_offset",
            True,
            [[datetime.datetime(2016, 9, 1, 16, 20, 1, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30)))]],
        ),
        ("SELECT dt_naive", True, [[datetime.datetime(2016, 9, 1, 16, 20, 1, tzinfo=None)]]),
    ],
)
async def test_dataset_proxy_engine(
    qc_async_redis: Redis,
    compiled_query,
    data,
    compress,
    # compress_alg,
):
    # EntityCacheEngineAsync.DEFAULT_COMPRESS_ALG = compress_alg

    ck = CachePseudoKeyParts()

    cache_key = make_cache_key(
        connection_id=ck.conn_id,
        connection_revision_id=ck.conn_revision,
        db_name=ck.database_name,
        compiled_query=compiled_query,
    )

    dse_a = EntityCacheEngineAsync(entity_id=ck.ds_id, rc=qc_async_redis)
    await dse_a.invalidate_all()

    await dse_a._update_cache(local_key_rep=cache_key, result=data)

    async_cached_data = list(await dse_a._get_from_cache(local_key_rep=cache_key))

    assert async_cached_data == data


# noinspection PyProtectedMember
@pytest.mark.parametrize(
    "single_dt_data",
    [
        pytz_local_now("Europe/Moscow"),
        datetime.datetime(2016, 9, 1, 16, 20, 1, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30))),
    ],
)
def test_offset_preserving(single_dt_data: datetime.datetime):
    result_data = [[single_dt_data]]
    cache_entry = ResultCacheEntry(
        key_parts_str="Q1",
        result_data=result_data,
    )
    cache_data = cache_entry.to_redis_data()
    cache_entry_roundtrip = ResultCacheEntry.from_redis_data(cache_data)
    result_data_roundtrip = cache_entry_roundtrip.make_result_data()
    dt_roundtrip = result_data_roundtrip[0][0]

    assert isinstance(single_dt_data, datetime.datetime)
    assert single_dt_data == dt_roundtrip
    assert single_dt_data.tzinfo.utcoffset(single_dt_data) == dt_roundtrip.tzinfo.utcoffset(dt_roundtrip)


@pytest.mark.asyncio
async def test_us_dataset_cache_invalidate_all(qc_async_redis: Redis):
    ck = CachePseudoKeyParts()
    dce = EntityCacheEngineAsync(entity_id=ck.ds_id, rc=qc_async_redis)

    result_map = {
        "SELECT {}".format(i): {
            "result": [
                [
                    i,
                ],
            ],
        }
        for i in range(0, 100)
    }

    for cq, rc in result_map.items():
        result = rc["result"]
        cache_key = make_cache_key(
            connection_id=ck.conn_id,
            connection_revision_id=ck.conn_revision,
            db_name=ck.database_name,
            compiled_query=cq,
        )
        await dce._update_cache(local_key_rep=cache_key, result=result)

    for cq, rc in result_map.items():
        result = rc["result"]
        cache_key = make_cache_key(
            connection_id=ck.conn_id,
            connection_revision_id=ck.conn_revision,
            db_name=ck.database_name,
            compiled_query=cq,
        )
        cached_data = await dce._get_from_cache(local_key_rep=cache_key)
        assert result == cached_data

    await dce.invalidate_all()

    for cq in result_map:
        cache_key = make_cache_key(
            connection_id=ck.conn_id,
            connection_revision_id=ck.conn_revision,
            db_name=ck.database_name,
            compiled_query=cq,
        )
        cached_data = await dce._get_from_cache(local_key_rep=cache_key)
        assert cached_data is None


def _change_connection(
    connection: ClassicConnectionSQL,
    *,
    us_manager: SyncUSManager,
    dataset: Optional[Dataset] = None,
    name_tpl: str = "{} (changed)",
) -> None:
    """Update the connection data to make it end up with different cache keys"""
    connection.data.name = name_tpl.format(connection.data.name)
    us_manager.save(connection)

    if dataset is not None:
        # TODO FIX: Remove after full implementation of https://st.yandex-team.ru/BI-1230
        #  Connection that was modified on previous step was already loaded in USM cache, so we need to flush it
        assert us_manager is not None
        us_manager._loaded_entries.clear()
        us_manager.load_dependencies(dataset)


# TODO FIX: Rewrite this test to use full async env (async USM)
# @pytest.mark.parametrize("compress_alg", [m for m in CompressAlg])
@pytest.mark.parametrize("selector_type", [SelectorType.CACHED, SelectorType.CACHED_LAZY])
@pytest.mark.parametrize("override_ttl_in_conn", [True, False])
def test_us_dataset_cache_engine_integration(
    cached_dataset_postgres: Dataset,
    us_conn_cache_tests_pg,
    async_service_registry_factory,
    bi_context,
    caplog,
    selector_type,
    override_ttl_in_conn,
    default_sync_usm,
    # compress_alg,
):
    us_manager = default_sync_usm
    connection = us_conn_cache_tests_pg
    dataset = cached_dataset_postgres
    role = DataSourceRole.origin
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    default_caches_ttl_config = CacheTTLConfig()
    sr: ServicesRegistry = async_service_registry_factory(
        SROptions(
            rci=bi_context,
            with_caches=True,
            default_caches_ttl_config=default_caches_ttl_config,
        )
    )

    # assert reporting_registry

    expected_ttl = default_caches_ttl_config.ttl_sec_direct
    if override_ttl_in_conn:
        expected_ttl = 34
        connection.data.cache_ttl_sec = expected_ttl
        _change_connection(connection, dataset=dataset, us_manager=us_manager)

    dataset_id = dataset.uuid
    assert dataset_id
    dce = EntityCacheEngineAsync(
        entity_id=dataset_id,
        rc=sr.get_caches_redis_client(),
    )
    caplog.set_level(logging.INFO, logger=PROFILING_LOG_NAME)
    caplog.set_level(logging.INFO, logger="dl_core.data_processing.cache")
    caplog.clear()

    data_source = ds_wrapper.get_sql_data_source_strict(
        source_id=dataset.get_single_data_source_id(),
        role=DataSourceRole.origin,
    )
    connection: ClassicConnectionSQL = data_source.connection

    # Invalidating caches before test
    await_sync(dce.invalidate_all())

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=sa.literal_column("val"),
                avatar_ids=[avatar_id],
                alias="val",
                user_type=UserDataType.string,
            ),
        ],
    )

    # Expect NO CACHE HIT
    data_fetcher = DataFetcher(
        service_registry=sr,
        dataset=dataset,
        selector_type=selector_type,
        us_manager=us_manager,
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(role=role, bi_query=bi_query))

    assert_no_warnings(caplog)

    result = await_sync(data_stream.data.all())
    assert result
    # TODO: compare cached result with the original data.

    # Ensure logging flag in request log
    rep_profiler = DefaultReportingProfiler(reporting_registry=sr.get_reporting_registry())
    rep_profiler.flush_all_query_reports()
    req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    assert req_profiling_log_rec.cache_used is True
    assert req_profiling_log_rec.cache_full_hit is False

    # Ensure TTL
    save_record = get_log_record(
        caplog,
        predicate=lambda rec: rec.msg.startswith("Cache finalized at "),
        single=True,
    )
    assert save_record.cache_ttl_sec == expected_ttl  # noqa

    caplog.clear()
    sr.get_reporting_registry().clear_records()

    # Expect CACHE HIT
    subsequent_result = await_sync(
        await_sync(
            data_fetcher.get_data_stream_async(
                role=role,
                bi_query=bi_query,
            ),
        ).data.all()
    )
    # TODO FIX: Remove me after fix in cache engine
    subsequent_result = [tuple(row) for row in subsequent_result]
    assert result == subsequent_result

    # Ensure logging flag in request log
    rep_profiler.flush_all_query_reports()
    req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    assert req_profiling_log_rec.cache_used is True
    assert req_profiling_log_rec.cache_full_hit is True

    caplog.clear()
    sr.get_reporting_registry().clear_records()

    _change_connection(connection, dataset=dataset, us_manager=us_manager)

    # Expect NO CACHE HIT
    data_stream = await_sync(data_fetcher.get_data_stream_async(role=role, bi_query=bi_query))
    await_sync(data_stream.data.all())

    # Ensure logging flag in request log
    rep_profiler.flush_all_query_reports()
    req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    assert req_profiling_log_rec.cache_used is True
    assert req_profiling_log_rec.cache_full_hit is False


# TODO: fixme https://st.yandex-team.ru/BI-4372
def test_locked_cache(
    monkeypatch,
    cached_dataset_postgres: Dataset,
    async_service_registry_factory,
    bi_context,
    default_sync_usm,
):
    """
    Monkeypatch the postgres connector to
    1. Use the locked cache
    2. Simply sleep instead of returning data (and count the calls).
    """

    # ### Locals and constants ###

    us_manager = default_sync_usm
    dataset = cached_dataset_postgres
    role = DataSourceRole.origin
    selector_type = SelectorType.CACHED_LAZY
    random_key_int = random.getrandbits(24)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    # ### Monkeypatches ###

    sync_result_iter = [[idx * random_key_int] for idx in range(10)]
    state = dict(calls=0)

    async def execute_query_context_monkey(*args: Any, **kwargs: Any) -> AsyncChunked:
        state["calls"] += 1
        await asyncio.sleep(0.5)
        return AsyncChunked.from_chunked_iterable([list(sync_result_iter)])

    monkeypatch.setattr(DatasetDbDataSelectorAsync, "execute_query_context", execute_query_context_monkey)

    # ### Utils and config ###

    sr: ServicesRegistry = async_service_registry_factory(
        SROptions(
            rci=bi_context,
            with_caches=True,
            default_caches_ttl_config=CacheTTLConfig(),
        )
    )
    reporting = sr.get_reporting_registry()
    assert reporting

    # caplog.set_level(logging.INFO, logger=PROFILING_LOG_NAME)
    # caplog.set_level(logging.DEBUG, logger='dl_core.data_processing.cache')
    # caplog.clear()

    data_source = ds_wrapper.get_sql_data_source_strict(
        source_id=dataset.get_single_data_source_id(),
        role=role,
    )
    connection: ClassicConnectionSQL = data_source.connection

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=sa.literal_column(f"id * {random_key_int}"),
                avatar_ids=[avatar_id],
                alias="id",
                user_type=UserDataType.integer,
            ),
        ],
    )

    async def get_one():
        data_fetcher = DataFetcher(
            service_registry=sr,
            dataset=dataset,
            selector_type=selector_type,
            us_manager=us_manager,
        )
        data_stream = await data_fetcher.get_data_stream_async(
            role=role,
            bi_query=bi_query,
        )
        result = await data_stream.data.all()
        return data_stream.data_key, result

    def _cleanup(invalidate: bool = False) -> None:
        # caplog.clear()
        reporting.clear_records()
        state["calls"] = 0
        if invalidate:
            _change_connection(connection, dataset=dataset, us_manager=us_manager)
            sync_result_iter[:] = sync_result_iter + sync_result_iter[:3]

    # ### Actual testing ###

    # Simple single cache miss
    assert state["calls"] == 0
    _, res_data = await_sync(get_one())
    assert state["calls"] == 1
    assert res_data == sync_result_iter

    _cleanup(invalidate=True)

    # Simultaneous requests
    assert state["calls"] == 0
    results = await_sync(asyncio.gather(*(get_one() for _ in range(10))))
    assert state["calls"] == 1  # 10 results, 1 db request

    # assert_no_warnings(caplog)

    keys = [key for key, _ in results]
    assert all(key == keys[0] for key in keys), keys

    res_datas = [res_data for _, res_data in results]
    assert all(res_data == sync_result_iter for res_data in res_datas)

    # Ensure logging flag in request log
    rep_profiler = DefaultReportingProfiler(reporting_registry=reporting)
    rep_profiler.flush_all_query_reports()
    # req_profiling_log_recs = get_dump_request_profile_records(caplog)
    # assert req_profiling_log_recs
    #
    # rec_used = [rec for rec in req_profiling_log_recs if rec.cache_used]
    # assert len(rec_used) == 10, req_profiling_log_recs
    # rec_hit = [rec for rec in req_profiling_log_recs if rec.cache_full_hit]
    # assert len(rec_hit) == 9, req_profiling_log_recs

    _cleanup(invalidate=False)

    # One request afterwards should be a cache hit
    assert state["calls"] == 0
    key, res_data = await_sync(get_one())
    assert key == keys[0]
    assert res_data == sync_result_iter
    assert state["calls"] == 0  # no new db requests

    # Ensure logging flag in request log
    rep_profiler.flush_all_query_reports()
    # req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    # assert req_profiling_log_rec.cache_used is True
    # assert req_profiling_log_rec.cache_full_hit is True

    _cleanup(invalidate=True)

    # After invalidation there should not be a cache hit
    assert state["calls"] == 0
    key, res_data = await_sync(get_one())
    assert key != keys[0]
    assert res_data == sync_result_iter
    assert state["calls"] == 1

    # Ensure logging flag in request log
    rep_profiler.flush_all_query_reports()
    # req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    # assert req_profiling_log_rec.cache_used is True
    # assert req_profiling_log_rec.cache_full_hit is False


def _detuple(value: Any) -> Any:
    """Somewhat equivalent to `json.loads(json.dumps(...))`"""
    if isinstance(value, (list, tuple)):
        return [_detuple(item) for item in value]
    if isinstance(value, dict):
        return {key: _detuple(val) for key, val in value.items()}
    return value


@pytest.mark.parametrize(
    ("is_bleeding_edge_user", "expected"),
    [
        (True, True),
        (False, False),
    ],
)
async def test_dashsql_cache_options(
    us_conn_cache_tests_pg,
    async_service_registry_factory,
    bi_context,
    is_bleeding_edge_user,
    expected,
):
    conn = us_conn_cache_tests_pg
    sr = async_service_registry_factory(
        SROptions(
            rci=bi_context,
            with_caches=True,
            cache_save_background=False,
            default_caches_ttl_config=CacheTTLConfig(),
        )
    )

    dashsql_selector = DashSQLCachedSelector(
        conn=conn,
        sql_query="select 1;",
        params=None,
        db_params={},
        service_registry=sr,
        is_bleeding_edge_user=is_bleeding_edge_user,
    )
    cache_opts = dashsql_selector.make_cache_options()
    opt_is_here = False
    for opt in cache_opts.key.key_parts:
        if opt.part_type == "is_bleeding_edge_user":
            assert opt.part_content == expected
            opt_is_here = True
    assert opt_is_here


# TODO: fixme https://st.yandex-team.ru/BI-4372
@pytest.mark.parametrize("override_ttl_in_conn", ["ttloverride", ""])
async def test_dashsql_cache(
    us_conn_cache_tests_pg,
    async_service_registry_factory,
    bi_context,
    override_ttl_in_conn,
    default_sync_usm,
):
    us_manager = default_sync_usm
    conn = us_conn_cache_tests_pg
    dashsql_selector_cls = DashSQLCachedSelector
    rndval = random.random()
    sql_query = f"select unnest(ARRAY[1/2, 3/{rndval}])"

    sr = async_service_registry_factory(
        SROptions(
            rci=bi_context,
            with_caches=True,
            cache_save_background=False,
            default_caches_ttl_config=CacheTTLConfig(),
        )
    )
    reporting = sr.get_reporting_registry()
    assert reporting
    rep_profiler = DefaultReportingProfiler(reporting_registry=sr.get_reporting_registry())
    assert rep_profiler

    async def req():
        dashsql_selector = dashsql_selector_cls(
            conn=conn,
            sql_query=sql_query,
            params=None,
            db_params={},
            service_registry=sr,
        )
        result_events = await dashsql_selector.execute()
        result = []
        async for event in result_events:
            result.append(event)
        result = _detuple(result)
        return result

    if override_ttl_in_conn:
        expected_ttl = 34
        conn.data.cache_ttl_sec = expected_ttl
        _change_connection(conn, us_manager=us_manager)
    # else:
    #     expected_ttl = CacheTTLConfig().ttl_sec_direct

    # caplog.set_level(logging.INFO, logger=PROFILING_LOG_NAME)
    # caplog.set_level(logging.INFO, logger='dl_core.data_processing.cache')
    # caplog.clear()

    data1 = await req()

    # assert_no_warnings(caplog)

    rep_profiler.flush_all_query_reports()
    # req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    # assert req_profiling_log_rec.cache_used is True
    # assert req_profiling_log_rec.cache_full_hit is False

    # Ensure TTL
    # save_record = get_log_record(
    #     caplog,
    #     predicate=lambda rec: rec.msg.startswith("Cache saved at "),
    #     single=True,
    # )
    # assert save_record.cache_ttl_sec == expected_ttl  # noqa

    # caplog.clear()
    reporting.clear_records()

    data2 = await req()
    assert data2 == data1

    rep_profiler.flush_all_query_reports()
    # req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    # assert req_profiling_log_rec.cache_used is True
    # assert req_profiling_log_rec.cache_full_hit is True

    # caplog.clear()
    reporting.clear_records()

    _change_connection(conn, us_manager=us_manager)

    data3 = await req()
    assert data3 == data1

    # Ensure logging flag in request log
    # rep_profiler.flush_all_query_reports()
    # req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
    # assert req_profiling_log_rec.cache_used is True
    # assert req_profiling_log_rec.cache_full_hit is False
