from flask import Flask, g
import pytest

import api_v1 as api_v1_module


class _FakeCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows


class _FakeDB:
    def execute(self, *_args, **_kwargs):
        return _FakeCursor()


def _unwrap_all_decorators(func):
    while hasattr(func, "__wrapped__"):
        func = func.__wrapped__
    return func


@pytest.fixture()
def client(monkeypatch):
    app = Flask(__name__)

    @app.before_request
    def _set_context():
        g.api_tier = "government"
        g.api_usage_today = 0
        g.api_rate_limit = 9999999
        g.tier_config = {"history_days": 730}

    app.register_blueprint(api_v1_module.api_v1)

    for endpoint, view_func in list(app.view_functions.items()):
        if endpoint.startswith("api_v1.") and endpoint != "api_v1.api_docs":
            app.view_functions[endpoint] = _unwrap_all_decorators(view_func)

    monkeypatch.setattr(api_v1_module, "get_db", lambda: _FakeDB())

    return app.test_client()


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/latency/example.com/history",
        "/api/v1/routes/example.com/changes",
        "/api/v1/rankings",
        "/api/v1/export/example.com",
    ],
)
@pytest.mark.parametrize("bad_limit", ["abc", "1.5", "", "0", "-5"])
def test_malformed_limit_returns_400(client, path, bad_limit):
    response = client.get(f"{path}?limit={bad_limit}")

    assert response.status_code == 400
    body = response.get_json()
    assert body["error"] in (
        "Invalid query parameter 'limit': expected integer",
        "Invalid query parameter 'limit': must be >= 1",
    )


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/latency/example.com/history",
        "/api/v1/routes/example.com/changes",
        "/api/v1/rankings",
        "/api/v1/export/example.com",
    ],
)
@pytest.mark.parametrize("good_limit", ["10", "999999"])
def test_valid_integer_limit_keeps_current_flow(client, path, good_limit):
    response = client.get(f"{path}?limit={good_limit}")

    assert response.status_code in (200, 404)
