import flaky
import flask
import flask.testing
import pytest

import dl_rate_limiter


@pytest.fixture(name="app")
def fixture_app(sync_request_limiter: dl_rate_limiter.SyncRequestRateLimiter) -> flask.Flask:
    # create and configure the app
    app = flask.Flask(__name__)

    # return 200 for all requests
    @app.route("/<path:path>")
    def wildcard(path: str):
        return "OK"

    app.config.update({"TESTING": True})
    dl_rate_limiter.FlaskMiddleware(rate_limiter=sync_request_limiter).set_up(app)

    yield app


@pytest.fixture(name="client")
def fixture_client(app: flask.Flask) -> flask.testing.FlaskClient:
    return app.test_client()


@pytest.fixture(name="runner")
def fixture_runner(app: flask.Flask) -> flask.testing.FlaskCliRunner:
    return app.test_cli_runner()


def test_hello(client: flask.testing.FlaskClient):
    response = client.get("/hello")
    assert response.data == b"OK"


def test_unlimited(client: flask.testing.FlaskClient):
    responses = [client.get("/unlimited/1", headers={"X-Test-Header": "test"}) for _ in range(10)]
    assert all(response.status_code == 200 for response in responses)


@flaky.flaky(max_runs=3)
def test_limited(client: flask.testing.FlaskClient):
    responses = [client.get("/limited/1", headers={"X-Test-Header": "test"}) for _ in range(20)]
    assert sum(response.status_code == 429 for response in responses) == 15
    assert sum(response.status_code == 200 for response in responses) == 5


@flaky.flaky(max_runs=3)
def test_limited_invalid_regex(client: flask.testing.FlaskClient):
    responses = [client.get("/limited/1", headers={"X-Test-Header": "t"}) for _ in range(20)]
    assert sum(response.status_code == 200 for response in responses) == 20


@flaky.flaky(max_runs=3)
def test_limited_multiple_limits(client: flask.testing.FlaskClient):
    responses = [client.get("/limited/more_specifically/1", headers={"X-Test-Header": "test"}) for _ in range(10)]
    assert sum(response.status_code == 429 for response in responses) == 9
    assert sum(response.status_code == 200 for response in responses) == 1

    responses = [client.get("/limited/1", headers={"X-Test-Header": "test"}) for _ in range(20)]
    assert sum(response.status_code == 429 for response in responses) == 16
    assert sum(response.status_code == 200 for response in responses) == 4
