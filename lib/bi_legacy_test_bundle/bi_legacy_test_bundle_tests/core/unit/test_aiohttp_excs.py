from __future__ import annotations

import asyncio
import contextlib
import functools
import socket

import aiohttp.client
import aiohttp.client_exceptions
import aiohttp.connector
import aiohttp.web
import pytest


FAKE_URL = "http://127.0.0.2:62345/"


async def req(url, connect_timeout=0.2, total_timeout=0.4):
    sess_cm = aiohttp.client.ClientSession(
        timeout=aiohttp.client.ClientTimeout(
            connect=connect_timeout,
            total=total_timeout,
        ),
        auth=aiohttp.client.BasicAuth(
            login="1",
            password="2",
            encoding="utf-8",
        ),
        headers={
            "X-Test": "3",
        },
    )
    async with sess_cm as sess:
        async with sess.post(url) as resp:
            status = resp.status
            body = await resp.text()
    return dict(status=status, body=body, resp=resp)


def awrap_sleeper(duration):
    def awrap_sleeper_configured(func):
        @functools.wraps(func)
        async def awrapped_sleeper(*args, **kwargs):
            await asyncio.sleep(duration)
            return await func(*args, **kwargs)

        return awrapped_sleeper

    return awrap_sleeper_configured


def make_araiser(exc):
    async def araiser(*args, **kwargs):
        raise exc

    return araiser


def make_areturner(value):
    async def areturner(*args, **kwargs):
        return value

    return areturner


def monkeywrap(monkeypatch, obj, attname, wrapper):
    base = getattr(obj, attname)
    wrapped = wrapper(base)
    monkeypatch.setattr(obj, attname, wrapped)


@pytest.mark.asyncio
async def test_connect_timeout(monkeypatch):
    with pytest.raises(aiohttp.client_exceptions.ClientConnectorError):
        await req(FAKE_URL)
    monkeywrap(monkeypatch=monkeypatch, obj=aiohttp.connector.TCPConnector, attname="connect", wrapper=awrap_sleeper(3))
    with pytest.raises(aiohttp.client_exceptions.ServerTimeoutError):
        await req(FAKE_URL)


@contextlib.asynccontextmanager
async def run_server(handler, methods=("POST",), bind="127.0.0.1"):
    app = aiohttp.web.Application()
    for method in methods:
        app.router.add_route(method, "/", handler)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((bind, 0))
        port = sock.getsockname()[1]
        url = f"http://{bind}:{port}/"
        server_task = asyncio.create_task(
            aiohttp.web._run_app(
                app=app,
                sock=sock,
                print=None,
            )
        )
        try:
            yield url
        finally:
            server_task.cancel()
    finally:
        sock.close()


@pytest.mark.asyncio
async def test_response_timeout(monkeypatch):
    async def handler_ok(requiest):
        return aiohttp.web.Response(body=b"OK")

    async with run_server(handler_ok) as url:
        resp_info = await req(url, total_timeout=0.03)
        assert resp_info["status"] == 200

    async def handler_fail(request):
        resp = aiohttp.web.StreamResponse(headers={"content-length": "100"})
        await resp.prepare(request)
        await asyncio.sleep(0.04)
        return resp

    async with run_server(handler_fail) as url:
        with pytest.raises(asyncio.TimeoutError):
            await req(url, total_timeout=0.03)
