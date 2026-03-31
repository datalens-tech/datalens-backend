import dl_logging


def test_log_context_accepts_dict() -> None:
    with dl_logging.LogContext(context={"dotted.key": "value"}):
        ctx = dl_logging.get_log_context()
        assert ctx["dotted.key"] == "value"
    ctx = dl_logging.get_log_context()
    assert "dotted.key" not in ctx
