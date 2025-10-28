from io import BytesIO

import botocore.response


class StreamingBodyIO(BytesIO):
    """Wrap a boto StreamingBody in the BytesIO API."""

    def __init__(self, body: botocore.response.StreamingBody) -> None:
        self.body = body

    def readable(self) -> bool:
        return True

    def read(self, n: int | None = -1) -> bytes:
        if n is not None and n < 0:
            n = None
        return self.body.read(n)
