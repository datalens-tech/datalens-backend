import pytest


@pytest.mark.asyncio
async def test_simple(pg_connection, pseudo_wb_path, wb_ctx_loader):
    ctx = await wb_ctx_loader.load(pseudo_wb_path)

    assert ctx.connections == (pg_connection,)
