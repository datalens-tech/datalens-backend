from __future__ import annotations

import asyncio
import os
import threading

import flask

from dl_core.flask_utils.aio_event_loop_middleware import AIOEventLoopMiddleware


def test_simulation(loop):
    pre_test_count = 1000
    pre_test_ids = set()
    marker_attr_n = "_tainted_n"  # marker 'known new loop'
    marker_attr_c = "_tainted_c"  # marker 'known new loop used as current'

    strict_id = True  # whether to consider id() as unique. Was supposed to be mostly so for small numbers.
    loop_keeper = []  # just to prevent GCing

    for idx in range(pre_test_count):
        new_loop = asyncio.new_event_loop()
        loop_keeper.append(new_loop)
        if strict_id:
            assert id(new_loop) not in pre_test_ids, dict(case="new_loop_is_not_new", step=idx)
        new_loop.set_debug(False)
        assert not getattr(new_loop, marker_attr_n, None), "new loop should be new"
        setattr(new_loop, marker_attr_n, True)
        asyncio.set_event_loop(new_loop)
        try:
            current_loop = asyncio.get_event_loop()
            assert current_loop is new_loop, dict(
                case="wrong_current_loop", step=idx, new_loop=new_loop, current_loop=current_loop
            )
            assert getattr(current_loop, marker_attr_n, None), "current loop should have been marked as new"
            assert not getattr(current_loop, marker_attr_c, None), "current loop should not have been used"
            setattr(current_loop, marker_attr_c, True)
            current_loop.run_until_complete(asyncio.sleep(0.0001))
            loop_id = id(current_loop)
        finally:
            new_loop.close()
            asyncio.set_event_loop(None)
        if strict_id:
            assert loop_id not in pre_test_ids, dict(step=idx, case="current_loop_is_not_new")
        pre_test_ids.add(loop_id)

    if strict_id:
        assert len(pre_test_ids) == pre_test_count, "did not get enough unique loops"


def test_app(caplog, loop):
    caplog.set_level("DEBUG")
    app = flask.Flask(__name__)
    AIOEventLoopMiddleware(debug=False).wrap_flask_app(app)

    marker_attr = "_test_tainted"
    strict_id = False  # whether to consider id() as unique. Was supposed to be mostly so for small numbers.

    @app.route("/")
    def home():
        async def make_some_sleep():
            await asyncio.sleep(0.0001)

        loop = asyncio.get_event_loop()
        assert not getattr(loop, marker_attr, None), "loop should be new"
        setattr(loop, marker_attr, True)

        loop.run_until_complete(make_some_sleep())
        return flask.jsonify(
            dict(
                event_loop_id=id(loop),
                pid=os.getpid(),
                thread=repr(threading.current_thread()),
                thread_id=threading.get_ident(),
            )
        )

    client = app.test_client()

    event_loop_id_set = set()
    rq_count = 300
    debug_data = []

    for _ in range(rq_count):
        resp = client.get("/")
        assert resp.status_code == 200
        resp_data = resp.json
        debug_data.append(resp_data)
        event_loop_id_set.add(resp_data["event_loop_id"])

    if strict_id:
        assert len(event_loop_id_set) == rq_count, debug_data
