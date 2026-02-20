import dl_constants
from dl_httpx import (
    BaseRequest,
    ParentContext,
)


REQUEST_ID_HEADER = dl_constants.DLHeadersCommon.REQUEST_ID.value


def test_request_id_without_parent() -> None:
    request = BaseRequest()

    assert request.request_id is not None
    assert isinstance(request.request_id, str)
    assert "--" not in request.request_id


def test_request_id_is_set() -> None:
    request = BaseRequest(request_id="test-123")
    assert request.request_id == "test-123"


def test_request_id_with_parent() -> None:
    parent_id = "parent-123"
    request = BaseRequest(parent_context=ParentContext(request_id=parent_id))

    assert request.request_id.startswith(f"{parent_id}--")
    assert len(request.request_id) > len(parent_id)

    child_part = request.request_id.split("--", 1)[1]
    assert len(child_part) > 0


def test_request_id_in_headers() -> None:
    request = BaseRequest(parent_context=ParentContext(request_id="parent-456"))
    headers = request.headers

    assert REQUEST_ID_HEADER in headers
    assert headers[REQUEST_ID_HEADER] == request.request_id
    assert headers[REQUEST_ID_HEADER].startswith("parent-456--")
